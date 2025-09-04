package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	nodeNameEnvvar                 = "vip_nodename" // matches kube-vip's node name envvar usage
	portEnvvar                     = "HEALTH_PORT"
	systemNamespaceEnvvar          = "SYSTEM_NAMESPACE"
	watchHostIPEnvvar              = "WATCH_IP"
	watchRegistryEnvvar            = "WATCH_REGISTRY"
	watchNginxEnvvar               = "WATCH_NGINX"
	watchControlPlane              = "WATCH_CP"
	ipSuccessThresholdEnvvar       = "IP_SUCCESS_THRESHOLD"
	ipFailureThresholdEnvvar       = "IP_FAILURE_THRESHOLD"
	registrySuccessThresholdEnvvar = "REGISTRY_SUCCESS_THRESHOLD"
	registryFailureThresholdEnvvar = "REGISTRY_FAILURE_THRESHOLD"
	nginxSuccessThresholdEnvvar    = "NGINX_SUCCESS_THRESHOLD"
	nginxFailureThresholdEnvvar    = "NGINX_FAILURE_THRESHOLD"
	cpSuccessThresholdEnvvar       = "CP_SUCCESS_THRESHOLD"
	cpFailureThresholdEnvvar       = "CP_FAILURE_THRESHOLD"

	clientTimeout     = time.Second * 3
	defaultHealthPort = 30901
	clusterCmName     = "cluster"
	clusterCmData     = "clusterConfiguration.yaml"
)

var (
	systemNamespace = "job-operator"
	k8              kubernetes.Interface
	nodeIp          string
)

type NetInfDoc struct {
	Address string `json:"address"`
}

type NodeDoc struct {
	Name              string      `json:"name"`
	NetworkInterfaces []NetInfDoc `json:"networkInterfaces"`
}

type ClusterDoc struct {
	Nodes []NodeDoc `json:"nodes"`
}

func findIpForNode(k8 kubernetes.Interface, nodeName string) (string, error) {
	cm, err := k8.CoreV1().ConfigMaps(systemNamespace).Get(context.Background(), clusterCmName, v1.GetOptions{})
	if err != nil {
		return "", err
	}
	doc := ClusterDoc{}
	if e := yaml.Unmarshal([]byte(cm.Data[clusterCmData]), &doc); e != nil {
		return "", e
	}
	for _, node := range doc.Nodes {
		if node.Name != nodeName {
			continue
		}
		if len(node.NetworkInterfaces) < 1 || node.NetworkInterfaces[0].Address == "" {
			return "", fmt.Errorf("node %s did not have networkInterfaces defined in config", nodeName)
		}
		return node.NetworkInterfaces[0].Address, nil
	}
	return "", fmt.Errorf("node %s was not found in config", nodeName)
}

func InitHttpClient() *http.Client {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	return &http.Client{Transport: tr, Timeout: clientTimeout}
}

// InitK8Client uses in-cluster config
// Note: don't init for 1g VIP mode to avoid cyclic deps
func InitK8Client() *kubernetes.Clientset {
	conf, err := clientcmd.BuildConfigFromFlags("", "")
	if err != nil {
		log.Printf("Failed to load k8 incluster config, %s ", err.Error())
		return nil
	}
	conf.Timeout = clientTimeout
	clientset, err := kubernetes.NewForConfig(conf)
	if err != nil {
		log.Printf("Error creating k8 clientset: %s", err.Error())
		return nil
	}

	if systemNamespace = os.Getenv(systemNamespaceEnvvar); systemNamespace == "" {
		systemNamespace = "job-operator"
	}
	return clientset
}

func InitNginxProbe(client *http.Client) (*Probe, error) {
	if os.Getenv(watchNginxEnvvar) == "" {
		return nil, nil
	}
	if k8 == nil {
		k8 = InitK8Client()
		ip, err := findIpForNode(k8, os.Getenv(nodeNameEnvvar))
		if err != nil {
			log.Fatal("Failed to find IP for node: ", err.Error())
		}
		log.Printf("node ip found: %s", ip)
		nodeIp = ip
	}
	return &Probe{
		ProbeHandler: &ServiceProbe{
			client:       client,
			namespace:    nginxNamespace,
			serviceName:  nginxServiceDeploy,
			installCheck: checkNginxInstall,
			parse:        parseNginxHealthCheck,
		},
		Name:             "nginx",
		FailureThreshold: getThreshold(nginxFailureThresholdEnvvar, DefaultFailureThreshold),
		SuccessThreshold: getThreshold(nginxSuccessThresholdEnvvar, DefaultSuccessThreshold),
		Period:           DefaultProbePeriod,
	}, nil
}

// todo: add registry LB to haproxy for push only and disable watching on registry by default to keep network more stable
func InitRegistryProbe(client *http.Client) (*Probe, error) {
	if os.Getenv(watchRegistryEnvvar) == "" {
		return nil, nil
	}
	if k8 == nil {
		k8 = InitK8Client()
		ip, err := findIpForNode(k8, os.Getenv(nodeNameEnvvar))
		if err != nil {
			log.Fatal("Failed to find IP for node: ", err.Error())
		}
		log.Printf("node ip found: %s", ip)
		nodeIp = ip
	}
	return &Probe{
		ProbeHandler: &ServiceProbe{
			client:       client,
			namespace:    kubeNamespace,
			serviceName:  privateRegistryDs,
			installCheck: checkRegistryInstall,
			parse:        probeHttp2xx,
		},
		Name: "registry",
		// default 100s threshold, relax registry health check to workaround
		// on hourly restart case to keep VIP before moving to haproxy
		FailureThreshold: getThreshold(registryFailureThresholdEnvvar, DefaultFailureThreshold*2),
		SuccessThreshold: getThreshold(registrySuccessThresholdEnvvar, 1),
		Period:           DefaultProbePeriod * 2,
	}, nil
}

// Deprecated, use haproxy
func InitControlPlaneProbe(httpClient *http.Client) *Probe {
	if os.Getenv(watchControlPlane) == "" {
		return nil
	}
	return &Probe{
		ProbeHandler: K8ControlPlaneProbe{
			client: httpClient,
		},
		Name:             "k8sControlPlane",
		FailureThreshold: getThreshold(cpFailureThresholdEnvvar, DefaultFailureThreshold),
		SuccessThreshold: getThreshold(cpSuccessThresholdEnvvar, DefaultSuccessThreshold),
		Period:           DefaultProbePeriod,
	}
}

func InitHostIPProbe() *Probe {
	if os.Getenv(watchHostIPEnvvar) == "" {
		return nil
	}
	ip := os.Getenv(watchHostIPEnvvar)
	log.Printf("Configuring host IP watcher for ip='%s'", ip)
	hostIP := net.ParseIP(ip)
	if hostIP == nil {
		log.Fatalf("Failed to parse given IP '%s'", ip)
	}
	return &Probe{
		ProbeHandler: &HostIPProbe{hostIP: hostIP},
		Name:         "hostIP",
		// kube-vip doesn't recreate its IP
		FailureThreshold: getThreshold(ipFailureThresholdEnvvar, 1),
		SuccessThreshold: getThreshold(ipSuccessThresholdEnvvar, 1),
		Period:           DefaultProbePeriod,
	}
}

func InitProcTaskCmdLine() ProcTask {
	if len(os.Args) < 2 {
		fmt.Printf("usage: %s CMD CMDARGS...\n", os.Args[0])
		os.Exit(1)
	}

	var args []string
	if len(os.Args) > 2 {
		args = os.Args[2:]
	}
	return ProcTask{
		Cmd:  os.Args[1],
		Args: args,
	}
}

func main() {
	mainTask := InitProcTaskCmdLine()
	httpClient := InitHttpClient()

	var probes []*Probe

	cpProbe := InitControlPlaneProbe(httpClient)
	if cpProbe != nil {
		probes = append(probes, cpProbe)
		log.Printf("Watching controlplane services: %s", cpProbe.String())
	}

	nginxProbe, err := InitNginxProbe(httpClient)
	if err != nil {
		log.Fatal("Failed to init nginx probe")
	} else if nginxProbe != nil {
		log.Printf("Watching nginx: %s", nginxProbe.String())
		probes = append(probes, nginxProbe)
	}

	registryProbe, err := InitRegistryProbe(httpClient)
	if err != nil {
		log.Fatal("Failed to init registry probe")
	} else if registryProbe != nil {
		log.Printf("Watching registry: %s", registryProbe.String())
		probes = append(probes, registryProbe)
	}

	hostIPProbe := InitHostIPProbe()
	if hostIPProbe != nil {
		log.Printf("Watching hostIP: %s", hostIPProbe.String())
		probes = append(probes, hostIPProbe)
	}

	port, _ := strconv.Atoi(os.Getenv(portEnvvar))
	if port == 0 {
		port = defaultHealthPort
	}
	log.Printf("Serving health on %d", port)

	mgr := NewHealthManager(mainTask, NewHealthServer(port), probes)
	mgr.RunForever()
}

func getThreshold(envvar string, defaultThreshold int) int {
	value := defaultThreshold
	if val := os.Getenv(envvar); val != "" {
		override, _ := strconv.Atoi(val)
		if override <= 0 {
			log.Fatalf("Environment variable %s expects a non-zero integer", envvar)
		}
		value = override
	}
	return value
}

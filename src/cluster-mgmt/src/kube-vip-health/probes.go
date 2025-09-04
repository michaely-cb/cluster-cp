package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	DefaultProbePeriod      = time.Second * 5
	DefaultFailureThreshold = 5
	DefaultSuccessThreshold = 3

	controlPlaneHealthUrl = "https://localhost:6443/readyz"
	kubeNamespace         = "kube-system"
	privateRegistryDs     = "private-registry"

	nginxNamespace     = "ingress-nginx"
	nginxServiceDeploy = "ingress-nginx-controller"
)

type Probe struct {
	ProbeHandler
	Name             string
	FailureThreshold int
	SuccessThreshold int
	Period           time.Duration

	// ok is the current state of the probe
	ok           bool
	failureCount int
	successCount int
}

func (p *Probe) String() string {
	return fmt.Sprintf("%s{period=%d,failureThreshold=%d,successThreshold=%d}",
		p.Name, int(p.Period.Seconds()), p.FailureThreshold, p.SuccessThreshold)
}

// probeAndTransition checks the ProbeHandler but will only transition the Probe if the previous
// success/failure threshold probes succeeded/failed. Returns true if state transition occurred
func (p *Probe) ProbeAndTransition() bool {
	if probeOk, err := p.Probe(); (!probeOk || err != nil) && p.ok {
		p.successCount = 0
		p.failureCount += 1
	} else if probeOk && !p.ok {
		p.failureCount = 0
		p.successCount += 1
	} else {
		if p.failureCount != 0 {
			log.Printf("%s probe back to ok after temporary failure", p.Name)
		}
		p.failureCount = 0
		p.successCount = 0
	}
	originalState := p.ok
	if p.ok && p.failureCount > 0 {
		if p.failureCount >= p.FailureThreshold {
			p.ok = false
			log.Printf("%s transitioned to not ok", p.Name)
		} else {
			log.Printf("%s probe failed. %d of %d consecutive failed probes before transition to not ok",
				p.Name, p.failureCount, p.FailureThreshold)
		}
	} else if !p.ok && p.successCount > 0 {
		if p.successCount >= p.SuccessThreshold {
			p.ok = true
			log.Printf("%s transitioned to ok", p.Name)
		} else {
			log.Printf("%s probe succeeded. %d of %d consecutive successful probes before transition to ok",
				p.Name, p.successCount, p.SuccessThreshold)
		}
	}
	return originalState != p.ok
}

type ProbeHandler interface {
	// Probe returns a bool indicating if the probe succeeded or error otherwise.
	Probe() (bool, error)
}

type K8ControlPlaneProbe struct {
	client *http.Client
}

func (p K8ControlPlaneProbe) Probe() (bool, error) {
	return probeHttp2xx(p.client, controlPlaneHealthUrl)
}

type HostIPProbe struct {
	hostIP net.IP

	seenHostIP bool
}

// Probe checks if there's an interface bound to the target IP and returns
// false only if the probe previously detected a bound interface but no longer
// finds one.
func (p *HostIPProbe) Probe() (bool, error) {
	inf, err := getInf(p.hostIP)
	if err != nil {
		log.Printf("Failed to check if host has IP, %v", err)
		return false, err
	}

	if inf != "" && !p.seenHostIP {
		p.seenHostIP = true
		log.Printf("HostIPProbe found %s on %s", p.hostIP.String(), inf)
	}

	return !p.seenHostIP || inf != "", nil
}

// getInf returns the name of the interface with the target
func getInf(target net.IP) (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, inf := range interfaces {
		addrs, err := inf.Addrs()
		if err != nil {
			return "", err
		}

		for _, addr := range addrs {
			var ip *net.IPNet
			var ok bool
			if ip, ok = addr.(*net.IPNet); !ok {
				continue
			}

			if target.Equal(ip.IP) {
				return inf.Name, nil
			}
		}
	}
	return "", nil
}

type ServiceProbe struct {
	client      *http.Client
	namespace   string
	serviceName string
	healthUrl   string
	// check whether component installed
	installCheck func(namespace, serviceName string) (string, error)
	// parse heath check result
	parse func(c *http.Client, url string) (bool, error)
	// for different stages of deployment
	seen bool
}

func (p *ServiceProbe) Probe() (bool, error) {
	if !p.seen {
		url, err := p.installCheck(p.namespace, p.serviceName)
		if err != nil {
			if !errors.IsNotFound(err) {
				log.Printf("Unexpected error checking component install: %s, %s", p.serviceName, err.Error())
				return false, err
			}
			log.Printf("%s is not installed, skipping health check", p.serviceName)
			return true, nil
		}
		p.seen = true
		p.healthUrl = url
		log.Printf("%s is installed, start health check on %s", p.serviceName, p.healthUrl)
	}
	ok, err := p.parse(p.client, p.healthUrl)
	if !ok {
		p.seen = false // reset on error in case service deleted
	}
	return ok, err
}

func probeHttp2xx(c *http.Client, url string) (bool, error) {
	resp, err := c.Get(url)
	if err != nil {
		log.Printf("Probe url '%s' failed: %v", url, err)
		return false, err
	}

	defer resp.Body.Close()
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return true, nil
	}

	body := ""
	if b, e := io.ReadAll(resp.Body); e == nil {
		body = string(b)
	}
	log.Printf("Probe url '%s' returned bad status: %d, %s", url, resp.StatusCode, body)
	return false, nil
}

func checkRegistryInstall(namespace, serviceName string) (string, error) {
	dp, err := k8.AppsV1().
		DaemonSets(namespace).
		Get(context.Background(), serviceName, v1.GetOptions{})
	if err != nil {
		return "", err
	}
	var port string
	for _, env := range dp.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "REGISTRY_HTTP_DEBUG_ADDR" {
			port = env.Value
			break
		}
	}
	return fmt.Sprintf("http://%s%s/debug/health", nodeIp, port), nil
}

type Service struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type NginxHealthCheckResponse struct {
	Service             Service `json:"service"`
	LocalEndpoints      int     `json:"localEndpoints"`
	ServiceProxyHealthy *bool   `json:"serviceProxyHealthy"`
}

func checkNginxInstall(namespace, serviceName string) (string, error) {
	svc, err := k8.CoreV1().
		Services(namespace).
		Get(context.Background(), serviceName, v1.GetOptions{})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("http://%s:%d", nodeIp, svc.Spec.HealthCheckNodePort), nil
}

func parseNginxHealthCheck(c *http.Client, url string) (bool, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return false, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		log.Printf("Probe url '%s' failed: %v", url, err)
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("Error: received status code %d", resp.StatusCode)
		return false, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading response body: %v", err)
	}
	var healthResponse NginxHealthCheckResponse
	if err := json.Unmarshal(body, &healthResponse); err != nil {
		log.Printf("Error parsing JSON: %v", err)
	}
	if healthResponse.LocalEndpoints > 0 {
		// nil check for backwards compatible on older k8s versions
		if healthResponse.ServiceProxyHealthy == nil || *healthResponse.ServiceProxyHealthy {
			return true, nil
		}
	}
	log.Printf("nginx health check returned false locally, %v", healthResponse)
	return false, nil
}

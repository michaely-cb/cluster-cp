package pkg

import (
	"encoding/base64"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"

	"sigs.k8s.io/yaml"
)

const (
	// FIXME: should these be moved to job-operator/apis/namespace/v1/namespace_types.go?
	SystemNamespace      = "job-operator"
	UnassignedNamespace  = "*unassigned"
	DefaultSystemCfgPath = "/opt/cerebras/config_v2"
)

type NamedCluster struct {
	Name string `json:"name"`
	Cluster
}

type NamedContext struct {
	Name string `json:"name"`
	Context
}

// NamedConfig copies the structure of https://github.com/kubernetes/client-go/blob/master/tools/clientcmd/api/v1/types.go#L28
// the idea being that future expansion of the client API might add Users as another entity attached to a context.
type NamedConfig struct {
	CurrentContext string         `json:"currentContext"`
	Contexts       []NamedContext `json:"contexts,omitempty"`
	Clusters       []NamedCluster `json:"clusters,omitempty"`
}

// localhost:9000 is chosen because it's the default kind port mapping. It is not guaranteed in any way.
var DefaultCluster = &Cluster{Server: "localhost:9000"}

// Cluster copies the structure from https://github.com/kubernetes/client-go/blob/master/tools/clientcmd/api/types.go#L67
// where it makes sense.
type Cluster struct {
	// Server is the address or IP of the cluster server, e.g. localhost:443 or 127.0.0.1:443
	Server string `json:"server"`

	// Authority is the authority of grpc connection option. e.g. cluster-server.cluster.example.local
	Authority string `json:"authority,omitempty"`

	// Namespaces contains the certificate authorities for different namespaces
	Namespaces []NamespaceCertAuthority `json:"namespaces,omitempty"`
}

type NamespaceCertAuthority struct {
	// Name is name of the namespace
	Name string `json:"name"`

	// CertificateAuthority is the path to a PEM-encoded certificate authority certificates.
	CertificateAuthority string `json:"certificateAuthority,omitempty"`

	// CertificateAuthorityData contains PEM-encoded certificate authority certificates. Overrides CertificateAuthority
	CertificateAuthorityData []byte `json:"certificateAuthorityData,omitempty"`
}

// Context joins multiple configuration components into a unified context. For now it is redundant, only containing
// a Cluster but in the future this may also hold a User.
type Context struct {
	Cluster string `json:"cluster"`
}

// Config is the internal representation of configuration which doesn't rely on lists of objects with names but converts
// those lists into a map.
type Config struct {
	// CurrentContext references the currently active cluster.
	CurrentContext string
	Contexts       map[string]*Context
	Clusters       map[string]*Cluster

	Path string

	OnMgmtNode bool
}

func NewConfig() *Config {
	return &Config{
		Contexts: make(map[string]*Context),
		Clusters: make(map[string]*Cluster),
	}
}

// GetCurrentCluster returns the cluster set in the current context.
func (c *Config) GetCurrentCluster() *Cluster {
	currentCtx := c.Contexts[c.CurrentContext]
	if currentCtx == nil {
		return DefaultCluster
	}

	if cluster, ok := c.Clusters[currentCtx.Cluster]; ok {
		return cluster
	}
	return DefaultCluster
}

// LoadNamespaceCertAuthority decides which authority csctl should talk to.
// If the client has access to the default namespace authority, we always set
// the authority to default namespace. Otherwise, we set the authority same
// as the namespace argument.
func (c *Config) LoadNamespaceCertAuthority(namespace string) (*NamespaceCertAuthority, error) {
	_, err := os.Stat(c.Path)
	if err != nil && os.IsNotExist(err) {
		return nil, fmt.Errorf("Specified csconfig file '%s' does not exist.", c.Path)
	}

	cluster := c.GetCurrentCluster()
	var namespaces []string
	for _, nsCertAuth := range cluster.Namespaces {
		// If multiple namespaces are available from the config and the system namespace is also available,
		// we always default to use the system namespace for csctl commands.
		if nsCertAuth.Name == SystemNamespace {
			return &nsCertAuth, nil
		}
		namespaces = append(namespaces, nsCertAuth.Name)
	}
	sort.Strings(namespaces)

	actionMessage := fmt.Sprintf("Please contact sysadmins for support.")
	if namespace == "" {
		if len(cluster.Namespaces) == 1 {
			return &cluster.Namespaces[0], nil
		} else if len(cluster.Namespaces) == 0 {
			return nil, fmt.Errorf("This node does not have access to any namespace. %s", actionMessage)
		} else {
			actionMessage = fmt.Sprintf("Please select a namespace with the '--namespace' option with one of %v.", namespaces)
			return nil, fmt.Errorf("This node has access to multiple namespaces. %s", actionMessage)
		}
	} else {
		for _, nsCertAuth := range (*cluster).Namespaces {
			if nsCertAuth.Name == namespace {
				return &nsCertAuth, nil
			}
		}
		return nil, fmt.Errorf("This node does not have access to namespace %s. %s",
			namespace, actionMessage)
	}
}

func (nc *NamedConfig) ConvertToConfig() (*Config, error) {
	c := NewConfig()
	for i, ctx := range nc.Contexts {
		if _, ok := c.Contexts[ctx.Name]; ok {
			return nil, fmt.Errorf("invalid config: context '%s' existed more than once", ctx.Name)
		}
		c.Contexts[ctx.Name] = &nc.Contexts[i].Context
	}
	for i, cluster := range nc.Clusters {
		if _, ok := c.Clusters[cluster.Name]; ok {
			return nil, fmt.Errorf("invalid config: cluster '%s' existed more than once", cluster.Name)
		}
		c.Clusters[cluster.Name] = &nc.Clusters[i].Cluster
	}
	c.CurrentContext = nc.CurrentContext
	return c, nil
}

func (c *Config) ConvertToNamedConfig() *NamedConfig {
	nc := &NamedConfig{}
	ctxNames := Keys(c.Contexts)
	sort.Strings(ctxNames)
	for _, name := range ctxNames {
		nc.Contexts = append(nc.Contexts, NamedContext{Name: name, Context: *c.Contexts[name]})
	}
	clusterNames := Keys(c.Clusters)
	sort.Strings(clusterNames)
	for _, name := range clusterNames {
		nc.Clusters = append(nc.Clusters, NamedCluster{Name: name, Cluster: *c.Clusters[name]})
	}
	nc.CurrentContext = c.CurrentContext
	return nc
}

func LoadConfig(cfgPath string) (*Config, error) {
	resolvedCfgPath, err := resolveCfgPath(cfgPath)
	if err != nil {
		return nil, err
	}

	namedCfg := &NamedConfig{}
	cfgBytes, err := os.ReadFile(resolvedCfgPath)
	// TODO: ideally we would like to refactor this code path,
	// such that it checks for file existence and returns exception upon missing config file.
	// Right now set-cluster needs the empty config created here for subsequent SaveConfig() calls,
	// it should use a separate loading function that's not sensitive to file existence.
	// Once the refacotr is complete we can remove the block in LoadNamespaceCertAuthority() that validates file existence.
	if err != nil && os.IsNotExist(err) {
		cfg := NewConfig()
		cfg.Path = resolvedCfgPath
		return cfg, nil
	} else if err != nil {
		return nil, err
	}

	if err = yaml.Unmarshal(cfgBytes, namedCfg); err != nil {
		return nil, err
	}
	cfg, err := namedCfg.ConvertToConfig()
	if err != nil {
		return nil, err
	}
	cfg.Path = resolvedCfgPath
	return cfg, nil
}

func (c *Config) SaveConfig() error {
	cfgDir := filepath.Dir(c.Path)
	err := os.MkdirAll(cfgDir, fs.ModePerm)
	if err != nil && !os.IsExist(err) {
		return fmt.Errorf("config error: failed to create config dir %s, %v", cfgDir, err)
	}
	contents, err := yaml.Marshal(c.ConvertToNamedConfig())
	if err != nil {
		return err
	}
	err = os.WriteFile(c.Path, contents, fs.ModePerm)
	if err != nil {
		return fmt.Errorf("config error: failed to write config file, %s %v", c.Path, err)
	}
	return err
}

// OmitData returns a reduced config for intended for human-readable terminal output.
func (c *Config) OmitData() *Config {
	rv := NewConfig()
	sDec, _ := base64.StdEncoding.DecodeString("DATA+OMITTED")
	for name, cluster := range c.Clusters {
		clusterCopy := *cluster
		for i, ns := range clusterCopy.Namespaces {
			if len(ns.CertificateAuthorityData) > 0 {
				clusterCopy.Namespaces[i].CertificateAuthorityData = sDec
			}
		}
		rv.Clusters[name] = &clusterCopy
	}
	rv.CurrentContext = c.CurrentContext
	return rv
}

func resolveCfgPath(cfgPath string) (string, error) {
	return ReplaceHome(cfgPath), nil
}

package pkg

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvert(t *testing.T) {
	t.Run("convert is reversible", func(t *testing.T) {
		nc := newConfig()
		c, err := nc.ConvertToConfig()
		require.NoError(t, err)

		ncc := c.ConvertToNamedConfig()
		assert.ElementsMatch(t, nc.Clusters, ncc.Clusters)
		assert.ElementsMatch(t, nc.Contexts, ncc.Contexts)
		assert.Equal(t, nc.CurrentContext, ncc.CurrentContext)
	})

	t.Run("repeated name errs", func(t *testing.T) {
		nc := newConfig()
		nc.Clusters[0].Name = nc.Clusters[1].Name
		_, err := nc.ConvertToConfig()
		assert.Error(t, err)
	})
}

func TestLoadConfig(t *testing.T) {
	t.Run("default config exists", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("/opt/cerebras", tempDir)
		cfgBytes, err := json.Marshal(newConfig())
		require.NoError(t, err)
		cfgPath := tempDir + "/config_v2"
		require.NoError(t, os.WriteFile(cfgPath, cfgBytes, 0644))

		cfg, err := LoadConfig(cfgPath)
		assert.NoError(t, err)
		assert.Equal(t, cfgPath, cfg.Path)
	})

	t.Run("load config user override not exists ok", func(t *testing.T) {
		_, err := LoadConfig(t.TempDir() + "/config_v2")
		assert.NoError(t, err)
	})
}

func TestLoadNamespaceCertificateAuthority(t *testing.T) {
	tempDir := t.TempDir()
	t.Setenv("/opt/cerebras", tempDir)
	cfgBytes, err := json.Marshal(newConfig())
	require.NoError(t, err)
	cfgPath := tempDir + "/config_v2"
	require.NoError(t, os.WriteFile(cfgPath, cfgBytes, 0644))

	cfg, err := LoadConfig(cfgPath)
	assert.NoError(t, err)

	t.Run("use the only namespace available", func(t *testing.T) {
		cfg.CurrentContext = "fooctx"

		// does not specify the namespace argument
		nsCertAuth, err := cfg.LoadNamespaceCertAuthority("")
		assert.NoError(t, err)
		assert.Equal(t, "foo-ns", nsCertAuth.Name)

		// explicitly specifying the namespace argument
		nsCertAuth, err = cfg.LoadNamespaceCertAuthority("foo-ns")
		assert.NoError(t, err)
		assert.Equal(t, "foo-ns", nsCertAuth.Name)
	})

	t.Run("no namespace and certificate authority available", func(t *testing.T) {
		cfg.CurrentContext = "noauthctx"

		_, err := cfg.LoadNamespaceCertAuthority("")
		assert.ErrorContains(t, err, "does not have access to any namespace")
	})

	t.Run("multiple namespaces and certificate authorities available", func(t *testing.T) {
		cfg.CurrentContext = "multiauthctx-1"

		_, err := cfg.LoadNamespaceCertAuthority("")
		assert.ErrorContains(t, err, "with one of")

		// explicitly specifying the namespace argument
		nsCertAuth, err := cfg.LoadNamespaceCertAuthority("ns-0")
		assert.NoError(t, err)
		assert.Equal(t, "ns-0", nsCertAuth.Name)

		nsCertAuth, err = cfg.LoadNamespaceCertAuthority("ns-1")
		assert.NoError(t, err)
		assert.Equal(t, "ns-1", nsCertAuth.Name)

		cfg.CurrentContext = "multiauthctx-2"

		nsCertAuth, err = cfg.LoadNamespaceCertAuthority("")
		assert.NoError(t, err)
		assert.Equal(t, SystemNamespace, nsCertAuth.Name)

		nsCertAuth, err = cfg.LoadNamespaceCertAuthority("ns-0")
		assert.NoError(t, err)
		assert.Equal(t, SystemNamespace, nsCertAuth.Name)

		// should use the ingress of the default namespace
		// whether the namespace is valid will be checked on the server side
		nsCertAuth, err = cfg.LoadNamespaceCertAuthority("does-not-exist")
		assert.NoError(t, err)
		assert.Equal(t, SystemNamespace, nsCertAuth.Name)
	})

	t.Run("no matching namespace", func(t *testing.T) {
		cfg.CurrentContext = "fooctx"

		_, err := cfg.LoadNamespaceCertAuthority("does-not-exist")
		assert.ErrorContains(t, err, "does not have access to namespace")
	})

	t.Run("cfg file does not exist", func(t *testing.T) {
		nonExistFilePath := "non-exist"
		cfg.Path = nonExistFilePath

		errorMessage := fmt.Sprintf("Specified csconfig file '%s' does not exist.", nonExistFilePath)

		// does not specify the namespace argument
		_, err := cfg.LoadNamespaceCertAuthority("")
		assert.ErrorContains(t, err, errorMessage)

		// explicitly specifying the namespace argument
		_, err = cfg.LoadNamespaceCertAuthority("foo-ns")
		assert.ErrorContains(t, err, errorMessage)
	})
}

func newConfig() *NamedConfig {
	fooCluster := Cluster{Server: "localhost:8080", Namespaces: []NamespaceCertAuthority{
		{
			Name:                 "foo-ns",
			CertificateAuthority: "cert",
		},
	}}
	barCluster := Cluster{Server: "localhost:8081", Namespaces: []NamespaceCertAuthority{
		{
			Name:                     "bar-ns",
			CertificateAuthorityData: []byte{0, 1, 2, 3},
		},
	}}
	noAuthCluster := Cluster{Server: "localhost:8082", Namespaces: []NamespaceCertAuthority{}}
	multiAuthCluster1 := Cluster{Server: "localhost:8083", Namespaces: []NamespaceCertAuthority{
		{
			Name:                 "ns-0",
			CertificateAuthority: "cert",
		},
		{
			Name:                     "ns-1",
			CertificateAuthorityData: []byte{0, 1, 2, 3},
		},
	}}
	multiAuthCluster2 := multiAuthCluster1
	multiAuthCluster2.Namespaces = append(multiAuthCluster2.Namespaces, NamespaceCertAuthority{
		Name:                 SystemNamespace,
		CertificateAuthority: "system-cert",
	})
	return &NamedConfig{
		Clusters: []NamedCluster{
			{Name: "bar", Cluster: barCluster},
			{Name: "foo", Cluster: fooCluster},
			{Name: "no-auth", Cluster: noAuthCluster},
			{Name: "multi-auth-1", Cluster: multiAuthCluster1},
			{Name: "multi-auth-2", Cluster: multiAuthCluster2},
		},
		Contexts: []NamedContext{
			{Name: "barctx", Context: Context{Cluster: "bar"}},
			{Name: "fooctx", Context: Context{Cluster: "foo"}},
			{Name: "noauthctx", Context: Context{Cluster: "no-auth"}},
			{Name: "multiauthctx-1", Context: Context{Cluster: "multi-auth-1"}},
			{Name: "multiauthctx-2", Context: Context{Cluster: "multi-auth-2"}},
		},
		CurrentContext: "fooctx",
	}
}

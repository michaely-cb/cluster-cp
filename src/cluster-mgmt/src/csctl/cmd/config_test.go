package cmd

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	"cerebras.com/cluster/csctl/pkg"
)

func writeTmpYaml(t *testing.T, name string, contents interface{}) string {
	tempDir := t.TempDir()
	fn := fmt.Sprintf("%s/%s", tempDir, name)

	v, err := yaml.Marshal(contents)
	require.NoError(t, err)

	err = os.WriteFile(fn, v, fs.ModePerm)
	require.NoError(t, err)

	return fn
}

func TestViewConfig(t *testing.T) {
	ctx := pkg.CmdCtx{
		UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
	}
	cfgIn := func() pkg.NamedConfig {
		return pkg.NamedConfig{
			CurrentContext: "prod",
			Clusters: []pkg.NamedCluster{
				{
					Name: "batcave",
					Cluster: pkg.Cluster{
						Server: "localhost:8888",
						Namespaces: []pkg.NamespaceCertAuthority{
							{
								Name:                 "job-operator",
								CertificateAuthority: "/usr/var/cert",
							},
						},
					},
				},
				{
					Name: "mb0",
					Cluster: pkg.Cluster{
						Server: "localhost:9999",
						Namespaces: []pkg.NamespaceCertAuthority{
							{
								Name:                     "job-operator",
								CertificateAuthorityData: []byte("somedata"),
							},
						},
					},
				},
			},
			Contexts: []pkg.NamedContext{
				{Name: "dev", Context: pkg.Context{Cluster: "batcave"}},
				{Name: "prod", Context: pkg.Context{Cluster: "mb0"}},
			},
		}
	}
	t.Run("raw output", func(t *testing.T) {
		cmd := NewCsCtlCmd(&ctx)
		namedCfgIn := cfgIn()
		stdout := new(bytes.Buffer)
		cmd.SetOut(stdout)
		cfgFp := writeTmpYaml(t, "config", namedCfgIn)
		cmd.SetArgs([]string{"--csconfig", cfgFp, "config", "view", "--raw"})

		require.NoError(t, cmd.Execute())

		namedCfgOut := pkg.NamedConfig{}
		err := yaml.Unmarshal(stdout.Bytes(), &namedCfgOut)
		require.NoError(t, err)
		assert.Equal(t, namedCfgIn, namedCfgOut)

		cfgIn, err := namedCfgIn.ConvertToConfig()
		require.NoError(t, err)
		cfgOut, err := namedCfgOut.ConvertToConfig()
		require.NoError(t, err)

		assert.Equal(t, cfgIn, cfgOut)
	})

	t.Run("omitted embedded cert output", func(t *testing.T) {
		cmd := NewCsCtlCmd(&ctx)
		stdout := new(bytes.Buffer)
		cmd.SetOut(stdout)
		cfgFp := writeTmpYaml(t, "config", cfgIn())
		cmd.SetArgs([]string{"--csconfig", cfgFp, "config", "view"})

		require.NoError(t, cmd.Execute())

		cfgOut := map[string]interface{}{}
		require.NoError(t, yaml.Unmarshal(stdout.Bytes(), &cfgOut))
		clusters := cfgOut["clusters"].([]interface{})
		foundMb0 := false
		for _, c := range clusters {
			cluster := c.(map[string]interface{})
			if cluster["name"].(string) == "mb0" {
				foundMb0 = true
				for _, nsCertAuth := range cluster["namespaces"].([]interface{}) {
					data := nsCertAuth.(map[string]interface{})["certificateAuthorityData"].(string)
					assert.Equal(t, data, "DATA+OMITTED")
				}
				break
			}
		}
		assert.True(t, foundMb0)
	})
}

func TestClusterFlow(t *testing.T) {
	ctx := pkg.CmdCtx{
		UserIdentityProvider: pkg.DefaultUserIdentityProvider{},
	}

	// quick glue test to ensure everything is working properly together
	testdir := map[string]string{}
	cfgFp := func(t *testing.T) string {
		dir, ok := "", false
		if dir, ok = testdir[t.Name()]; !ok {
			dir = t.TempDir()
			testdir[t.Name()] = dir
		}
		return fmt.Sprintf("%s/config", dir)
	}
	cfgArgs := func(t *testing.T, args ...string) []string {
		return append([]string{"--csconfig", cfgFp(t), "config"}, args...)
	}
	cfg := func(t *testing.T) *pkg.Config {
		contents, err := os.ReadFile(cfgFp(t))
		require.NoError(t, err)
		c := &pkg.NamedConfig{}
		require.NoError(t, yaml.Unmarshal(contents, c))
		cfg, err := c.ConvertToConfig()
		require.NoError(t, err)
		return cfg
	}

	t.Run("view empty config", func(t *testing.T) {
		rootCmd := NewCsCtlCmd(&ctx)
		rootCmd.SetArgs(cfgArgs(t, "view"))
		err := rootCmd.Execute()
		require.NoError(t, err)
	})

	t.Run("set update delete cluster flow", func(t *testing.T) {
		rootCmd := NewCsCtlCmd(&ctx)
		rootCmd.SetArgs(cfgArgs(t, "set-cluster", "kind", "--server", "localhost:8888"))
		require.NoError(t, rootCmd.Execute())
		c := cfg(t)
		assert.Equal(t, "localhost:8888", c.Clusters["kind"].Server)

		ns := "foo-ns"
		certFp := t.TempDir() + "/cert"
		require.NoError(t, os.WriteFile(certFp, []byte("x"), os.ModePerm))
		rootCmd.SetArgs(cfgArgs(t,
			"set-cluster", "kind",
			"--server", "",
			"--namespace", ns,
			"--embed-certs",
			"--certificate-authority", certFp))
		require.NoError(t, rootCmd.Execute())
		c = cfg(t)
		assert.Equal(t, "", c.Clusters["kind"].Server)
		for _, nsCertAuth := range c.Clusters["kind"].Namespaces {
			if nsCertAuth.Name == ns {
				assert.Empty(t, nsCertAuth.CertificateAuthority)
				assert.Equal(t, []byte("x"), nsCertAuth.CertificateAuthorityData)
			}
		}

		// replace embedded certs with path
		rootCmd = NewCsCtlCmd(&ctx)
		rootCmd.SetArgs(cfgArgs(t,
			"set-cluster", "kind",
			"--server", "",
			"--namespace", ns,
			"--certificate-authority", certFp))
		require.NoError(t, rootCmd.Execute())
		c = cfg(t)
		assert.Equal(t, "", c.Clusters["kind"].Server)
		for _, nsCertAuth := range c.Clusters["kind"].Namespaces {
			if nsCertAuth.Name == ns {
				assert.Equal(t, certFp, nsCertAuth.CertificateAuthority)
				assert.Nil(t, nsCertAuth.CertificateAuthorityData)
			}
		}

		// replace a cert without specifying namespace
		rootCmd = NewCsCtlCmd(&ctx)
		rootCmd.SetArgs(cfgArgs(t,
			"set-cluster", "kind",
			"--server", "",
			"--certificate-authority", certFp))
		require.ErrorContains(t, rootCmd.Execute(), "required to be set together")

		rootCmd.SetArgs(cfgArgs(t, "delete-cluster", "kind"))
		require.NoError(t, rootCmd.Execute())
		c = cfg(t)
		assert.Empty(t, c.Clusters)

		require.NoError(t, rootCmd.Execute()) // delete non-exist => no-op
	})

	t.Run("use cluster flow", func(t *testing.T) {
		rootCmd := NewCsCtlCmd(&ctx)
		rootCmd.SetArgs(cfgArgs(t, "use-cluster", "kind"))
		assert.Error(t, rootCmd.Execute())

		rootCmd.SetArgs(cfgArgs(t, "set-cluster", "kind"))
		require.NoError(t, rootCmd.Execute())

		rootCmd.SetArgs(cfgArgs(t, "use-cluster", "kind"))
		assert.NoError(t, rootCmd.Execute())
		c := cfg(t)
		assert.Equal(t, "kind", c.CurrentContext)
		assert.Equal(t, "kind", c.Contexts["kind"].Cluster)
	})

	t.Run("use context flow", func(t *testing.T) {
		rootCmd := NewCsCtlCmd(&ctx)
		rootCmd.SetArgs(cfgArgs(t, "set-cluster", "kind", "--server", "localhost:8888"))
		require.NoError(t, rootCmd.Execute())

		rootCmd.SetArgs(cfgArgs(t, "set-context", "test", "--cluster", "kind"))
		require.NoError(t, rootCmd.Execute())

		rootCmd.SetArgs(cfgArgs(t, "use-context", "test"))
		require.NoError(t, rootCmd.Execute())
		c := cfg(t)
		assert.Equal(t, "test", c.CurrentContext)

		stderr := new(bytes.Buffer)
		rootCmd.SetErr(stderr)
		defer rootCmd.SetErr(os.Stderr)
		rootCmd.SetArgs(cfgArgs(t, "delete-context", "test"))
		require.NoError(t, rootCmd.Execute())
		assert.Contains(t, stderr.String(), "use-context") // warning about missing context
	})

	t.Run("use context error flow", func(t *testing.T) {
		rootCmd := NewCsCtlCmd(&ctx)
		rootCmd.SetArgs(cfgArgs(t, "use-context", "kind"))
		err := rootCmd.Execute()
		assert.Error(t, err)
	})

	t.Run("set update delete context flow", func(t *testing.T) {
		rootCmd := NewCsCtlCmd(&ctx)
		rootCmd.SetArgs(cfgArgs(t, "set-context", "kind", "--cluster", "x"))
		assert.NoError(t, rootCmd.Execute())

		c := cfg(t)
		assert.Equal(t, "x", c.Contexts["kind"].Cluster)

		rootCmd.SetArgs(cfgArgs(t, "set-context", "kind", "--cluster", "y"))
		assert.NoError(t, rootCmd.Execute())
		c = cfg(t)
		assert.Equal(t, "y", c.Contexts["kind"].Cluster)

		rootCmd.SetArgs(cfgArgs(t, "delete-context", "kind"))
		assert.NoError(t, rootCmd.Execute())
		c = cfg(t)
		assert.NotContains(t, c.Contexts, "kind")
	})
}

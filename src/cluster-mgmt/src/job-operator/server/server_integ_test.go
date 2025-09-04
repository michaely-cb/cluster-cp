//go:build integration && server

package server

import (
	"testing"

	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/stretchr/testify/require"

	wscommon "cerebras.com/job-operator/common"
)

const PORT = 9000

// Because we use a static endpointslice we setup and teardown the environment
// every test to ensure envs are isolated
func TestServerNoIp(t *testing.T) {
	t.Setenv("POD_IP", "")

	t.Run("test upsertEndpointSlice without POD_IP", func(t *testing.T) {
		mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)

		defer func() {
			cancel()
			require.NoError(t, testEnv.Stop())
		}()

		server := NewGRPCServer(PORT, mgr.GetClient(), mgr.GetLogger())
		require.NotNil(t, server)

		err := server.upsertEndpointSlice(ctx)
		require.Error(t, err)
		require.ErrorContains(t, err, "pod IP is not set")
	})
}

func TestServer(t *testing.T) {
	POD_IP := "10.0.0.0"
	t.Setenv("POD_IP", POD_IP)

	t.Run("test upsertEndpointSlice endpoint slice does not exist", func(t *testing.T) {
		mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)

		defer func() {
			cancel()
			require.NoError(t, testEnv.Stop())
		}()

		server := NewGRPCServer(PORT, mgr.GetClient(), mgr.GetLogger())
		require.NotNil(t, server)

		endpointSlice := &discoveryv1.EndpointSlice{}

		require.True(t, apierrors.IsNotFound(mgr.GetAPIReader().Get(ctx, client.ObjectKey{
			Name:      wscommon.GRPCServerEndpointSliceName,
			Namespace: wscommon.Namespace,
		}, endpointSlice)))

		err := server.upsertEndpointSlice(ctx)
		require.NoError(t, err)

		require.NoError(t, mgr.GetAPIReader().Get(ctx, client.ObjectKey{
			Name:      wscommon.GRPCServerEndpointSliceName,
			Namespace: wscommon.Namespace,
		}, endpointSlice))

		require.Equal(t, 1, len(endpointSlice.Endpoints))
		require.Equal(t, 1, len(endpointSlice.Endpoints[0].Addresses))
		require.Equal(t, POD_IP, endpointSlice.Endpoints[0].Addresses[0])
	})

	t.Run("test upsertEndpointSlice endpoint does not exist success", func(t *testing.T) {
		mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
		defer func() {
			cancel()
			require.NoError(t, testEnv.Stop())
		}()

		server := NewGRPCServer(PORT, mgr.GetClient(), mgr.GetLogger())
		require.NotNil(t, server)

		require.NoError(t, mgr.GetClient().Create(ctx, &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      wscommon.GRPCServerEndpointSliceName,
				Namespace: wscommon.Namespace,
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints:   []discoveryv1.Endpoint{},
		}))

		require.NoError(t, server.upsertEndpointSlice(ctx))

		endpointSlice := &discoveryv1.EndpointSlice{}

		require.NoError(t, mgr.GetAPIReader().Get(ctx, client.ObjectKey{
			Name:      wscommon.GRPCServerEndpointSliceName,
			Namespace: wscommon.Namespace,
		}, endpointSlice))

		require.Equal(t, 1, len(endpointSlice.Endpoints))
		require.Equal(t, 1, len(endpointSlice.Endpoints[0].Addresses))
		require.Equal(t, POD_IP, endpointSlice.Endpoints[0].Addresses[0])
	})

	t.Run("test upsertEndpointSlice endpoint already exists success", func(t *testing.T) {
		mgr, testEnv, ctx, cancel := wscommon.EnvSetup(false, t)
		defer func() {
			cancel()
			require.NoError(t, testEnv.Stop())
		}()

		server := NewGRPCServer(PORT, mgr.GetClient(), mgr.GetLogger())
		require.NotNil(t, server)

		oldPodIp1 := "10.0.0.1"
		oldPodIp2 := "10.0.0.2"

		require.NoError(t, mgr.GetClient().Create(ctx, &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      wscommon.GRPCServerEndpointSliceName,
				Namespace: wscommon.Namespace,
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses: []string{oldPodIp1, oldPodIp2},
				},
			},
		}))

		require.NoError(t, server.upsertEndpointSlice(ctx))

		endpointSlice := &discoveryv1.EndpointSlice{}

		require.NoError(t, mgr.GetAPIReader().Get(ctx, client.ObjectKey{
			Name:      wscommon.GRPCServerEndpointSliceName,
			Namespace: wscommon.Namespace,
		}, endpointSlice))

		require.Equal(t, 1, len(endpointSlice.Endpoints))
		require.Equal(t, 1, len(endpointSlice.Endpoints[0].Addresses))
		require.Equal(t, POD_IP, endpointSlice.Endpoints[0].Addresses[0])
	})
}

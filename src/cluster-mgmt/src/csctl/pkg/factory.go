package pkg

import (
	"context"
	"crypto/x509"
	"fmt"
	"go/build"
	"regexp"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	pbadm "cerebras/pb/workflow/appliance/cluster_mgmt/csadm"
	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
)

const (
	grpcServiceConfig = `{
		"methodConfig": [{
			"name": [
				{"service": "cluster.cluster_mgmt_pb.csctl.CsCtlV1"},
				{"service": "cluster.cluster_mgmt_pb.csadm.CsAdmV1"}
			],
			"retryPolicy": {
				"maxAttempts": 4,
				"initialBackoff": "3s",
				"maxBackoff": "10s",
				"backoffMultiplier": 2,
				"retryableStatusCodes": ["UNAVAILABLE", "UNKNOWN", "RESOURCE_EXHAUSTED"]
			}
		}]
	}`
	maxCallRecvMsgSize = 10 * 1024 * 1024
)

// ClientFactory allows for overriding connection generation for tests
type ClientFactory interface {
	// NewCsAdmV1 returns a csadm server client
	NewCsAdmV1(ctx context.Context) (pbadm.CsAdmV1Client, error)

	// NewCsCtlV1 returns a csctl server client
	NewCsCtlV1(ctx context.Context) (pb.CsCtlV1Client, error)

	// Close all connections created by this Factory.
	Close() error
}

type ClientFactoryImpl struct {
	cluster    Cluster
	nsCertAuth NamespaceCertAuthority
	cmdCtx     *CmdCtx

	conn *grpc.ClientConn
	mu   sync.Mutex

	isUserAuthEnabled bool
}

func NewClientFactory(cmdCtx *CmdCtx, cluster *Cluster, nsCertAuth *NamespaceCertAuthority) ClientFactory {
	return &ClientFactoryImpl{
		cmdCtx:     cmdCtx,
		cluster:    *cluster,
		nsCertAuth: *nsCertAuth,
	}
}

func (c *ClientFactoryImpl) getConn() (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		c.cmdCtx.Logger.Debugf("Reusing the existing connection.")
		return c.conn, nil
	}
	c.cmdCtx.Logger.Debugf("Establishing a new connection...")

	var conn *grpc.ClientConn
	var err error

	server := c.cluster.Server
	if hasPort, _ := regexp.MatchString(":\\d+$", server); !hasPort {
		server = fmt.Sprintf("%s:443", server)
	}

	c.cmdCtx.Logger.Infof("Server: %s", server)

	var opts []grpc.DialOption
	var credOpt grpc.DialOption
	if c.nsCertAuth.CertificateAuthorityData != nil {
		certPool := x509.NewCertPool()
		if !certPool.AppendCertsFromPEM(c.nsCertAuth.CertificateAuthorityData) {
			return nil, fmt.Errorf(
				"failed to parse cert from certificateAuthorityData for namespace %s", c.nsCertAuth.Name,
			)
		}
		credOpt = grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(certPool, ""))
	} else {
		credOpt = grpc.WithTransportCredentials(insecure.NewCredentials())
	}

	authority := c.cluster.Authority
	if c.cluster.Authority != "" {
		// TODO: Deprecate this if we adopted global cluster server in the future
		authority, err = buildNsAwareAuthority(authority, c.nsCertAuth.Name)
		if err != nil {
			panic(err)
		}
		opts = append(opts, grpc.WithAuthority(authority))
	}

	c.cmdCtx.Logger.Infof("Ingress namespace: %s, resource namespace: %s", c.nsCertAuth.Name, c.cmdCtx.Namespace)
	c.cmdCtx.Logger.Infof("Authority: %s, certificate:\n%s", authority, string(c.nsCertAuth.CertificateAuthorityData))

	userAgent := fmt.Sprintf("%s (csctl:%s/%s)", CerebrasVersion, build.Default.GOOS, build.Default.GOARCH)
	opts = append(opts, credOpt,
		grpc.WithUserAgent(userAgent),
		grpc.WithChainUnaryInterceptor(
			UserAuthInterceptor(c),
			NamespaceInterceptor(c),
			CertificateErrorInterceptor(c.cmdCtx.Cfg.Path),
			ClientMetadataInterceptor(),
		),
		grpc.WithChainStreamInterceptor(
			UserAuthStreamInterceptor(c),
			NamespaceStreamInterceptor(c),
		),
		grpc.WithDefaultServiceConfig(grpcServiceConfig),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxCallRecvMsgSize)),
	)
	conn, err = grpc.Dial(server, opts...)
	if err != nil {
		return nil, err
	}
	c.cmdCtx.Logger.Debugf("Established a new connection: %v", conn)
	c.conn = conn
	return c.conn, nil
}

func (c *ClientFactoryImpl) NewCsAdmV1(ctx context.Context) (pbadm.CsAdmV1Client, error) {
	err := c.loadServerConfig(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}
	return pbadm.NewCsAdmV1Client(conn), nil
}

func (c *ClientFactoryImpl) NewCsCtlV1(ctx context.Context) (pb.CsCtlV1Client, error) {
	err := c.loadServerConfig(ctx)
	if err != nil {
		return nil, err
	}
	conn, err := c.getConn()
	if err != nil {
		return nil, err
	}
	return pb.NewCsCtlV1Client(conn), nil
}

func (c *ClientFactoryImpl) loadServerConfig(ctx context.Context) error {
	c.isUserAuthEnabled = false
	conn, err := c.getConn()
	if err != nil {
		return err
	}

	client := pb.NewCsCtlV1Client(conn)
	response, err := client.GetServerConfig(ctx, &pb.GetServerConfigRequest{})
	if err != nil {
		if status.Code(err) == codes.Unimplemented {
			c.cmdCtx.Logger.Warnf("GetServerConfig not implemented at the server side. Default to disable user auth checks.")
			// If an unimplemented error is returned, we assume that user auth is not enabled. This is to support
			// backward compatability.
			c.isUserAuthEnabled = false
		} else {
			return err
		}
	} else {
		c.isUserAuthEnabled = response.IsUserAuthEnabled
	}

	return nil
}

func (c *ClientFactoryImpl) Close() error {
	defer func() { c.conn = nil }()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	if err != nil {
		return fmt.Errorf("error closing client connection: %s", err.Error())
	}
	c.cmdCtx.Logger.Debugf("Closing connection.")
	return nil
}

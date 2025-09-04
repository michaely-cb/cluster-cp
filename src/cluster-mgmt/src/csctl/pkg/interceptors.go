package pkg

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	AuthUidHeader           = "auth-uid"
	AuthGidHeader           = "auth-gid"
	AuthTokenHeader         = "auth-token"
	ResourceNamespaceHeader = "resource-namespace"
	ClientBuildVersion      = "client-build-version"
	ClientSemanticVersion   = "client-semantic-version"
)

func getUserAuthMetadata(c *ClientFactoryImpl) (uid, gid, token string, err error) {
	token = os.Getenv("CSAUTH_TOKEN")
	if c.isUserAuthEnabled && len(token) == 0 {
		getCerebrasToken := "/usr/local/bin/get-cerebras-token"
		_, err := os.Stat(getCerebrasToken)
		if err != nil {
			return "", "", "", errors.New(
				fmt.Sprintf(
					"UserAuth is enabled in the cluster, but %s does not exist. Please upgrade the user/management node",
					getCerebrasToken,
				),
			)
		}

		cmd := exec.Command(getCerebrasToken)
		tokenBytes, err := cmd.Output()
		if err != nil {
			return "", "", "", err
		}
		token = string(tokenBytes)
		if strings.HasPrefix(token, "Token=") {
			token = token[len("Token="):]
		} else {
			return "", "", "", errors.New("User auth token is invalid")
		}
	}

	user, err := user.Current()
	if err != nil {
		return "", "", "", err
	}

	return user.Uid, user.Gid, token, nil
}

func UserAuthInterceptor(c *ClientFactoryImpl) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {

		uid, gid, token, err := getUserAuthMetadata(c)
		if err != nil {
			return err
		}
		ctx = metadata.AppendToOutgoingContext(ctx, AuthUidHeader, uid, AuthGidHeader, gid, AuthTokenHeader, token)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func UserAuthStreamInterceptor(c *ClientFactoryImpl) grpc.StreamClientInterceptor {
	return func(
		ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption) (grpc.ClientStream, error) {

		uid, gid, token, err := getUserAuthMetadata(c)
		if err != nil {
			return nil, err
		}
		ctx = metadata.AppendToOutgoingContext(ctx, AuthUidHeader, uid, AuthGidHeader, gid, AuthTokenHeader, token)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func NamespaceInterceptor(c *ClientFactoryImpl) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx,
			ResourceNamespaceHeader, c.cmdCtx.Namespace,
		)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func NamespaceStreamInterceptor(c *ClientFactoryImpl) grpc.StreamClientInterceptor {
	return func(ctx context.Context,
		desc *grpc.StreamDesc,
		cc *grpc.ClientConn,
		method string,
		streamer grpc.Streamer,
		opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx = metadata.AppendToOutgoingContext(ctx,
			ResourceNamespaceHeader, c.cmdCtx.Namespace,
		)
		return streamer(ctx, desc, cc, method, opts...)
	}
}

func CertificateErrorInterceptor(cfgPath string) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if err != nil && strings.Contains(err.Error(), "authentication handshake failed") {
			// Return a user-friendly error message
			errorMsg := fmt.Sprintf("Authentication handshake failed. "+
				"Please check csctl config %s and contact sysadmins for support.\n"+
				"Detailed error: %s.",
				cfgPath, err)
			return errors.New(errorMsg)
		}
		return err
	}
}

func ClientMetadataInterceptor() grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption) error {
		ctx = metadata.AppendToOutgoingContext(ctx,
			ClientBuildVersion, CerebrasVersion,
			ClientSemanticVersion, SemanticVersion,
		)
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

package pkg

import (
	"context"
	"fmt"
	"os"
	"strconv"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"

	"cerebras.net/kube-user-auth/pkg"
)

const (
	// ClientMetaKey is the key used to store client meta in the context
	ClientMetaKey = "clientMeta"

	AuthUid               = "auth-uid"
	AuthGid               = "auth-gid"
	AuthUsername          = "auth-username"
	AuthToken             = "auth-token"
	WorkflowId            = "workflow-id"
	ClientBuildVersion    = "client-build-version"
	ClientSemanticVersion = "client-semantic-version"
)

type ClientMeta struct {
	UID                   int64
	GID                   int64
	Username              string
	Groups                []int64
	Token                 string
	WorkflowID            string
	ClientBuildVersion    string
	ClientSemanticVersion string
}

var UnknownUser = ClientMeta{
	UID: -1,
	GID: -1,
}

var skipAuth = map[string]bool{
	"/cluster.cluster_mgmt_pb.ClusterManagement/GetServerConfig":  true,
	"/cluster.cluster_mgmt_pb.ClusterManagement/GetJobInitStatus": true,
	"/cluster.cluster_mgmt_pb.csctl.CsCtlV1/GetServerConfig":      true,
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/UpdateSystemStatus":   true,
	"/grpc.health.v1.Health/Check":                                true,
}

var denyRoot = map[string]bool{
	"/cluster.cluster_mgmt_pb.ClusterManagement/InitCompileJob":        true,
	"/cluster.cluster_mgmt_pb.ClusterManagement/InitExecuteJob":        true,
	"/cluster.cluster_mgmt_pb.ClusterManagement/InitImageBuildJob":     true,
	"/cluster.cluster_mgmt_pb.ClusterManagement/InitSdkCompileJob":     true,
	"/cluster.cluster_mgmt_pb.ClusterManagement/InitSdkExecuteJob":     true,
	"/cluster.cluster_mgmt_pb.ClusterManagement/CreateWorkloadManager": true,
}

func GetClientMetaFromContext(ctx context.Context) (ClientMeta, error) {
	return getClientMetaFromContext(ctx, false)
}

func getClientMetaFromContext(ctx context.Context, disableUserAuth bool) (ClientMeta, error) {
	// return if already parsed
	var meta ClientMeta
	meta, ok := ctx.Value(ClientMetaKey).(ClientMeta)
	if ok {
		return meta, nil
	}

	md, mdOk := metadata.FromIncomingContext(ctx)
	if !mdOk {
		return UnknownUser, nil
	}
	// new request
	var err error
	if meta.UID, err = parseMetaIntVal(md, AuthUid, "uid"); !disableUserAuth && err != nil {
		return UnknownUser, err
	}
	if meta.GID, err = parseMetaIntVal(md, AuthGid, "gid"); !disableUserAuth && err != nil {
		return UnknownUser, err
	}

	if meta.GID != -1 {
		meta.Groups = append(meta.Groups, meta.GID)
	}
	if username := md.Get(AuthUsername); len(username) > 0 {
		meta.Username = username[0] // Username is optional
	}
	if wflowId := md.Get(WorkflowId); len(wflowId) > 0 {
		meta.WorkflowID = wflowId[0]
	}
	if userTokens := md.Get(AuthToken); len(userTokens) > 0 {
		meta.Token = userTokens[0]
	}
	if clientBuildVersion := md.Get(ClientBuildVersion); len(clientBuildVersion) > 0 {
		meta.ClientBuildVersion = clientBuildVersion[0]
	}
	if clientSemanticVersion := md.Get(ClientSemanticVersion); len(clientSemanticVersion) > 0 {
		meta.ClientSemanticVersion = clientSemanticVersion[0]
	}
	return meta, nil
}

// ClientMetaInterceptor adds client meta to the context.
func NewClientMetaInterceptor(disableUserAuth bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip authentication for excludedMethods.
		if skipAuth[info.FullMethod] {
			return handler(ctx, req)
		}

		clientMeta, err := getClientMetaFromContext(ctx, disableUserAuth)
		if err != nil {
			return nil, err
		}
		return handler(context.WithValue(ctx, ClientMetaKey, clientMeta), req)
	}
}

// Authenticate the user.
func NewUserAuthInterceptor(disableUserAuth bool) grpc.UnaryServerInterceptor {
	// no op if disabled
	if disableUserAuth {
		return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			return handler(ctx, req)
		}
	}

	tokenExpireTimeSec := pkg.DefaultTokenExpireTimeSec
	authSecret, err := os.ReadFile("/kube-user-auth/secret")
	if err != nil {
		panic(fmt.Sprintf("Can't read user auth secret: %v", err))
	}
	userAuth := pkg.NewSharedSecretUserAuth(authSecret, tokenExpireTimeSec)

	// override meta with authenticated user info
	validateToken := func(userAuth pkg.UserAuth, meta *ClientMeta) error {
		userInfo, err := userAuth.ValidateToken(meta.Token)
		if err != nil {
			log.Errorf("Can't validate token: %s, %v", meta.Token, err)
			return err
		}
		meta.Username = userInfo.Username
		meta.UID = userInfo.UID
		meta.GID = userInfo.GID
		meta.Groups = userInfo.Groups
		return nil
	}

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip authentication for excludedMethods.
		if skipAuth[info.FullMethod] {
			return handler(ctx, req)
		}

		meta, ok := ctx.Value(ClientMetaKey).(ClientMeta)
		if !ok {
			return nil, grpcStatus.Errorf(codes.Internal, "missing user details")
		}

		if meta.Token == "" {
			return nil, grpcStatus.Errorf(codes.Unauthenticated, "missing user auth-token")
		}

		err = validateToken(userAuth, &meta)
		if err != nil {
			return nil, grpcStatus.Errorf(codes.Unauthenticated, err.Error())
		}

		return handler(context.WithValue(ctx, ClientMetaKey, meta), req)
	}
}

func NewDenyRootInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !denyRoot[info.FullMethod] {
			return handler(ctx, req)
		}

		user, ok := ctx.Value(ClientMetaKey).(ClientMeta)
		if !ok {
			return nil, grpcStatus.Errorf(codes.Internal, "missing user details")
		}

		if user.UID == 0 {
			return nil, grpcStatus.Errorf(
				codes.PermissionDenied,
				"User{username='%s',UID=0} is not allowed to execute this method %s. Please use a non-root user.",
				user.Username,
				info.FullMethod,
			)
		}
		return handler(ctx, req)
	}
}

func parseMetaIntVal(md metadata.MD, key, idType string) (int64, error) {
	val := md.Get(key)
	if len(val) == 0 {
		return -1, grpcStatus.Errorf(codes.Unauthenticated, "missing %s", idType)
	}
	parsedId, err := strconv.ParseInt(val[0], 10, 64)
	if err != nil {
		return -1, grpcStatus.Errorf(codes.Unauthenticated, "invalid %s", idType)
	}
	return parsedId, nil
}

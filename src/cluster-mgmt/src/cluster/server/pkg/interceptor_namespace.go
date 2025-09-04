package pkg

import (
	"context"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcStatus "google.golang.org/grpc/status"

	wscommon "cerebras.com/job-operator/common"
)

const (
	// XForwardedHostHeader is the key used to store the ingress endpoint.
	XForwardedHostHeader = "x-forwarded-host"

	// ResourceNamespaceHeader is the key used to store which namespace
	// the resource lives in.
	// Starting from release 2.1, this header allows clients to specify the target
	// namespace where the resource they are querying or updating should reside.
	// Prior to release 2.1, this namespace info used to be set at the client side
	// as part of the request body. We continue to respect the namespace info set
	// in the request body until release 2.3 to provide backward compatibility.
	// TODO: By release 2.3, we will deprecate the namespace attribute in all request
	// bodies.
	ResourceNamespaceHeader = "resource-namespace"
)

var invalidHeaderErr = grpcStatus.Errorf(
	codes.PermissionDenied,
	"Header %s is required but did not have a valid value",
	XForwardedHostHeader,
)

var skipInterceptor = map[string]bool{
	// Skip checks otherwise we would see a lot of warning messages
	// related to header not found in metadata.
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/UpdateSystemStatus":   true,
	"/cluster.cluster_mgmt_pb.ClusterManagement/GetJobInitStatus": true,
	"/grpc.health.v1.Health/Check":                                true,
}

var denyUserNamespace = map[string]bool{
	// GetSession is allowed on user namespaces
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/GetSession":              false,
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/CreateOrUpdateSession":   true,
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/DeleteSession":           true,
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/ListSession":             true,
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/CreateSystemMaintenance": true,
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/GetSystemMaintenance":    true,
	"/cluster.cluster_mgmt_pb.csadm.CsAdmV1/ListSystemMaintenance":   true,
}

// NewNamespaceInterceptor does permission check for system-namespace only apis and adds namespace key to context.
func NewNamespaceInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// Skip namespace interceptor checks.
		if skipInterceptor[info.FullMethod] {
			return handler(ctx, req)
		}
		ingressNs, resourceNs := "", ""
		md, mdOk := metadata.FromIncomingContext(ctx)
		if !mdOk {
			return nil, invalidHeaderErr
		}
		// Retrieves user namespace by parsing namespace from authority in x-forwarded-host header
		// e.g. <namespace>.cluster-server.mbx.cerebrassc.local
		domains := md.Get(XForwardedHostHeader)
		if len(domains) == 0 {
			return nil, invalidHeaderErr
		}
		hosts := strings.Split(domains[0], ".")
		if len(hosts) < 2 {
			return nil, invalidHeaderErr
		}

		ingressNs = hosts[0]
		if ns := md.Get(ResourceNamespaceHeader); len(ns) > 0 {
			resourceNs = ns[0]
		} else {
			logrus.Debugf("%s header not found in metadata, default to use the current namespace %s.",
				ResourceNamespaceHeader, wscommon.Namespace)
			resourceNs = wscommon.Namespace
		}

		if denyUserNamespace[info.FullMethod] && ingressNs != wscommon.SystemNamespace {
			return nil, grpcStatus.Errorf(
				codes.PermissionDenied,
				"User is not allowed to call %s on user namespace '%s'.",
				info.FullMethod,
				ingressNs,
			)
		}

		if userNSRequestsSystemNSClusterWriteAPI(ingressNs, info.FullMethod) {
			return nil, grpcStatus.Errorf(
				codes.PermissionDenied,
				"User attempted to launch or modify a job in the system namespace. This typically means the user's "+
					" session had no resources and was scaled down. An admin must add resources to your session for job launches to proceed. "+
					"Check `csctl get cluster` to verify your session has no resources")
		}

		// If "ingressNs" is non-empty, it means the client expects the following namespace interceptor check.
		// AND if "ingressNs" is not the default namespace, it means the interceptor check should be performed.
		// AND if "ingressNs" does not match "resourceNs", it means the client was targeting an ingress that
		// was not the default namespace, but attempted to query/update resources that belong to a different namespace.
		// We should deny such a request.
		// The csctl client code should not allow this case to happen. This is just a server-side guard to handle
		// bugs and malicious requests.
		if ingressNs != "" && ingressNs != wscommon.DefaultNamespace && ingressNs != resourceNs {
			return nil, grpcStatus.Errorf(
				codes.PermissionDenied,
				"Request is not allowed to access resources in the '%s' namespace.",
				resourceNs,
			)
		}
		return handler(context.WithValue(ctx, ResourceNamespaceHeader, ingressNs), req)
	}
}

// Returns true if a user's NS/session was scaled down and their ingress redirected to the system namespace and they
// attempted a write operation
func userNSRequestsSystemNSClusterWriteAPI(ingressNS string, grpcMethod string) bool {
	if wscommon.Namespace != wscommon.SystemNamespace {
		return false // not system namespace cluster-server
	}
	if ingressNS == "" || ingressNS == wscommon.SystemNamespace {
		return false // ingress not set or not user namespace
	}
	if !strings.HasPrefix(grpcMethod, "/cluster.cluster_mgmt_pb.ClusterManagement/") {
		return false // not cluster API
	}
	return strings.Contains(grpcMethod, "Init") || strings.Contains(grpcMethod, "Delete") || strings.Contains(grpcMethod, "Cancel") // not a mutate method
}

func GetNamespaceFromContext(ctx context.Context) string {
	namespace := wscommon.Namespace
	md, mdOk := metadata.FromIncomingContext(ctx)
	if mdOk {
		if ns := md.Get(ResourceNamespaceHeader); len(ns) > 0 {
			namespace = ns[0]
		}
	}
	return namespace
}

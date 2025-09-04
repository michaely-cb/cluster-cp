package pkg

import (
	"context"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"cerebras.com/job-operator/common"
)

const (
	DefaultTaskSpecKey = "default"
)

var excludeLogMethods = map[string]bool{
	"/grpc.health.v1.Health/Check": true,
}

var excludeResponsePayloadMethods = map[string]bool{
	"/cluster.cluster_mgmt_pb.csctl.CsCtlV1/DownloadLogExport": true,
}

func Ternary[T any](condition bool, valueOnConditionTrue, valueOtherwise T) T {
	if condition {
		return valueOnConditionTrue
	}
	return valueOtherwise
}

// TODO: Add a semantic version interceptor to ensure no compatibility issue between cluster client and cluster server.
func NewUnaryInterceptors(disableUserAuth bool) grpc.ServerOption {
	return grpc.ChainUnaryInterceptor(
		UnaryPanicRecoveryInterceptor(),
		NewClientMetaInterceptor(disableUserAuth),
		NewUserAuthInterceptor(disableUserAuth),
		NewDenyRootInterceptor(),
		NewNamespaceInterceptor(),
		UnaryLoggingInterceptor,
	)
}

func NewStreamInterceptors() grpc.ServerOption {
	return grpc.ChainStreamInterceptor(
		StreamPanicRecoveryInterceptor(),
		StreamLoggingInterceptor,
	)
}

func UnaryLoggingInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {

	// Log request
	logRequest(ctx, req, info.FullMethod)

	start := time.Now()
	resp, err := handler(ctx, req)

	// Log response
	logResponse(resp, err, info.FullMethod, time.Since(start))
	return resp, err
}

func StreamLoggingInterceptor(
	srv interface{},
	stream grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler) error {

	// Log request
	logRequest(stream.Context(), nil, info.FullMethod)

	start := time.Now()
	err := handler(srv, stream)

	// Log response
	logResponse(nil, err, info.FullMethod, time.Since(start))
	return err
}

func logRequest(ctx context.Context, req interface{}, method string) {
	if excludeLogMethods[method] {
		return
	}

	userAgent := "unknown"
	resourceNs := common.Namespace
	workflowId := ""
	clientBuildVersion, clientSemanticVersion := GetClientVersionFromContext(ctx)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		ua := md.Get("user-agent")
		if len(ua) > 0 {
			userAgent = strings.Join(ua, ",")
		}
		ns := md.Get(ResourceNamespaceHeader)
		if len(ns) > 0 {
			resourceNs = ns[0]
		}
		wid := md.Get(WorkflowId)
		if len(wid) > 0 {
			workflowId = wid[0]
		}
	}

	fields := log.Fields{
		"type":                "request",
		"user-agent":          userAgent,
		ClientBuildVersion:    clientBuildVersion,
		ClientSemanticVersion: clientSemanticVersion,
		"namespace":           resourceNs,
		"workflow-id":         workflowId,
		"method":              method,
		"payload":             common.LogProtoAsJson(req, true),
	}
	log.WithFields(fields).Info("gRPC request")
}

func logResponse(resp interface{}, err error, method string, duration time.Duration) {
	if excludeLogMethods[method] {
		return
	}

	fields := log.Fields{
		"type":     "response",
		"method":   method,
		"duration": duration,
	}

	if err != nil {
		fields["err"] = err
		log.WithFields(fields).Warn("gRPC error")
	} else {
		if !excludeResponsePayloadMethods[method] && resp != nil {
			fields["payload"] = resp
		}
		log.WithFields(fields).Info("gRPC response")
	}
}

func GetClientVersionFromContext(ctx context.Context) (string, string) {
	clientBuildVersion := "unknown"
	clientSemanticVersion := "unknown"
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		version := md.Get(ClientBuildVersion)
		if len(version) > 0 {
			clientBuildVersion = version[0]
		} else {
			// We used to send client build version through user agent prior to cluster rel-3.0.0
			// TODO: Remove this branch in cluster rel-3.2.0
			ua := md.Get("user-agent")
			if len(ua) > 0 {
				clientBuildVersion = strings.Split(ua[0], " ")[0]
			}
		}
		semver := md.Get(ClientSemanticVersion)
		if len(semver) > 0 {
			clientSemanticVersion = semver[0]
		}
	}
	return clientBuildVersion, clientSemanticVersion
}

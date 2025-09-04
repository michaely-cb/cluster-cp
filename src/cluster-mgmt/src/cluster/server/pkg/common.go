package pkg

import (
	"strings"

	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/errors"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

const (
	SysAdminSupportMsg = "Please contact systems for support."
)

var CerebrasVersion string // should be set by "go build"

var httpCode2GRPC = map[int]codes.Code{
	400: codes.InvalidArgument,
	401: codes.Unauthenticated,
	403: codes.PermissionDenied,
	404: codes.NotFound,
	409: codes.Unknown,
	412: codes.FailedPrecondition,
	429: codes.ResourceExhausted,
	499: codes.Canceled,
	500: codes.Internal,
	501: codes.Unimplemented,
	503: codes.Unavailable,
	504: codes.DeadlineExceeded,
}

func HttpCode2GRPC(code int) codes.Code {
	if c, ok := httpCode2GRPC[code]; ok {
		return c
	}
	return codes.Internal
}

func MapStatusError(err error) error {
	if err == nil {
		return nil
	}
	if k8sErr, ok := err.(*errors.StatusError); ok {
		return grpcStatus.Errorf(HttpCode2GRPC(int(k8sErr.Status().Code)), k8sErr.ErrStatus.Message)
	}
	// idempotence check
	if strings.Contains(err.Error(), "Cluster server internal error") {
		return err
	}
	return grpcStatus.Errorf(codes.Internal, "Cluster server internal error: %v", err)
}

func hasCommonGroup(group1 []int64, group2 []int64) bool {
	group1Map := map[int64]bool{}
	for _, group := range group1 {
		group1Map[group] = true
	}

	for _, group := range group2 {
		if group1Map[group] {
			return true
		}
	}
	return false
}

func CheckUserPermission(userDetails ClientMeta, wsjob *wsapisv1.WSJob) error {
	if userDetails.UID != 0 {
		if wsjob != nil &&
			(wsjob.Spec.User == nil ||
				(userDetails.GID != wsjob.Spec.User.Gid &&
					!hasCommonGroup(userDetails.Groups, []int64{wsjob.Spec.User.Gid}) &&
					!hasCommonGroup(userDetails.Groups, wsjob.Spec.User.Groups))) {
			return grpcStatus.Errorf(
				codes.PermissionDenied, "Wsjob %s is not owned by any group: %v. %s",
				wsjob.Name, userDetails.Groups, SysAdminSupportMsg)
		}
	}
	return nil
}

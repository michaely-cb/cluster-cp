package pkg

import (
	"context"
	"fmt"

	"github.com/coreos/go-semver/semver"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

const (
	JobOperatorSemanticVersion = "SEMANTIC_VERSION"
)

func GetJobOperatorSemanticVersion(kubeClientSet kubernetes.Interface) (string, error) {
	cm, err := kubeClientSet.CoreV1().
		ConfigMaps(wscommon.SystemNamespace).
		Get(context.Background(), ClusterEnvConfigMapName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to read job operator version ConfigMap: %w", err)
	}

	version, exists := cm.Data[JobOperatorSemanticVersion]
	if !exists {
		return "", fmt.Errorf("%s key not found in ConfigMap", JobOperatorSemanticVersion)
	}
	return version, nil
}

func operatorSupports(operatorSemverStr string, feat wsapisv1.SoftwareFeature) error {
	operatorSemver, err := semver.NewVersion(operatorSemverStr)
	if err != nil {
		return err
	}
	minimalVersion := wsapisv1.SoftwareFeatureSemverMap[feat]
	if operatorSemver.LessThan(minimalVersion) {
		msg := fmt.Sprintf("Job operator of version %s does not support %s, "+
			"please request cluster admin to upgrade job operator to latest(>=%s)",
			operatorSemverStr, feat, minimalVersion)
		return grpcStatus.Errorf(codes.Unimplemented, msg)
	}

	return nil
}

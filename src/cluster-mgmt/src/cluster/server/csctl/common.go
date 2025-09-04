package csctl

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"cerebras.com/job-operator/common"
)

const (
	K8sLabelPrefix     = "labels.k8s.cerebras.com/"
	K8sRoleLabelPrefix = "k8s.cerebras.com/node-role-"

	Memx   = "memoryx"
	Swarmx = "swarmx"
)

var nodeMarketingNames = map[string]string{
	"broadcastreduce": Swarmx,
	"memory":          Memx,
}

var labelKvRegex = regexp.MustCompile(`^([A-Za-z0-9][-A-Za-z0-9_.]{0,61})?[A-Za-z0-9]$`)

func ValidateLabelKey(key string) error {
	if !labelKvRegex.Match([]byte(key)) {
		return fmt.Errorf("invalid label key: %s. Must match regex %s", key, labelKvRegex.String())
	}
	return nil
}

func ValidateLabelValue(val *string) error {
	if val != nil && (*val != "" && !labelKvRegex.Match([]byte(*val))) {
		return fmt.Errorf("invalid label value: %s. Must be empty or match regex %s", *val, labelKvRegex.String())
	}
	return nil
}

func platformVersionMatch(label, version string) bool {
	shouldMatch := true
	if strings.Contains(version, "!") {
		version = strings.ReplaceAll(version, "!", "")
		shouldMatch = false
	}

	for _, v := range common.ParseLabelVersions(label) {
		if v == version {
			return shouldMatch
		}
	}
	return !shouldMatch
}

// getNodeMarketingRole infer the marking role ("broadcastreduce"->"swarmx") from the labels on a node. Unlabeled nodes
// are "none" role while multi-role nodes are "any"
func getNodeMarketingRole(node *corev1.Node) string {
	roles := map[string]bool{}
	for l := range node.Labels {
		if strings.HasPrefix(l, K8sRoleLabelPrefix) {
			n := strings.TrimPrefix(l, K8sRoleLabelPrefix)
			if mn, ok := nodeMarketingNames[n]; ok {
				roles[mn] = true
			} else {
				roles[n] = true
			}
		}
	}
	roleNames := common.SortedKeys(roles)
	if len(roleNames) == 0 {
		return "none"
	} else if len(roleNames) == 1 {
		return roleNames[0]
	} else if len(roleNames) == 2 && slices.Equal(roleNames, []string{"coordinator", "management"}) {
		// If a node only contains "management" and "coordinator" role, it means mgmt/coord separation
		// did not happen in this cluster. In that case, the existing behaviour is to use "management"
		// as the node role.
		return "management"
	}
	return "any"
}

func MapProtoTime(t metav1.Time) *tspb.Timestamp {
	pt := t.ProtoTime()
	return &tspb.Timestamp{Seconds: pt.Seconds, Nanos: pt.Nanos}
}

func min[V float64 | int64](a, b V) V {
	if a < b {
		return a
	}
	return b
}

// fmtMemRound is similar to the default String() method of Quantity, but it rounds down to the nearest Ki, Mi, Gi, etc.
// except Mi where values below 8Gi are rounded to the nearest Mi unless it's wouldn't lose precision, the idea being
// to preserve some precision where smaller values are more likely to make a difference.
func fmtMemRound(q int64) string {
	if q == 0 {
		return "unknown"
	}
	if q < (1 << 10) {
		return "0"
	} else if q < 1<<20 {
		return fmt.Sprintf("%dKi", q>>10)
	} else if q < 1<<33 && !(q&((1<<30)-1) == 0) {
		return fmt.Sprintf("%dMi", q>>20)
	} else {
		return fmt.Sprintf("%dGi", q>>30)
	}
}

// fmtCpuRound formats cpu in cores with optional tenth place decimals
func fmtCpuRound(q float64) string {
	if q < 0 {
		return "unknown"
	}
	if int(q*10) == int(q)*10 {
		return strconv.Itoa(int(q))
	}
	return fmt.Sprintf("%.1f", q)
}

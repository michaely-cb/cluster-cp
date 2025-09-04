package pkg

import (
	"bytes"
	"os/exec"
	"strconv"
	"strings"
)

// InClusterNodeChecker interface for checking if running on a management node
type InClusterNodeChecker interface {
	CheckInClusterNode() bool
	CheckSingleNodeCluster() bool
}

// DefaultInClusterNodeChecker implements InClusterNodeChecker using systemctl
type DefaultInClusterNodeChecker struct{}

func (d DefaultInClusterNodeChecker) CheckInClusterNode() bool {
	cmd := exec.Command("systemctl", "is-active", "kubelet")
	out := new(bytes.Buffer)
	cmd.Stdout = out
	if err := cmd.Run(); err == nil {
		output := strings.TrimSpace(out.String())
		return output == "active"
	}
	return false
}

// CheckSingleNodeCluster returns true if the system namespace CM says there’s exactly one node.
func (d DefaultInClusterNodeChecker) CheckSingleNodeCluster() bool {
	// Read cluster config directly from file system
	cmd := exec.Command("bash", "-c",
		"yq '.nodes | length' /opt/cerebras/cluster/cluster.yaml",
	)

	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		// yq failed or file doesn't exist → assume multi-node
		return false
	}

	countStr := strings.TrimSpace(out.String())
	count, err := strconv.Atoi(countStr)
	if err != nil {
		// parse error → assume multi-node
		return false
	}

	return count == 1
}

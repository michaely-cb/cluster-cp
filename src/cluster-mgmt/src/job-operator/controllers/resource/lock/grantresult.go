package lock

import (
	"strings"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	"cerebras.com/job-operator/common/resource"
)

type groupErrorType int

const (
	NoProgress groupErrorType = iota
	InsufficientPopGroups
	InsufficientTotalGroups
	PartiallyScheduled
	SearchFailed
	NoGroupGrantError
)

type grantResult struct {
	name                string
	resourceGrants      map[string]resource.Grant
	brGroupGrant        resource.GroupGrant
	chfGroupGrants      []resource.GroupGrant
	actGroupGrants      []resource.GroupGrant
	wgtCmdGroupGrants   []resource.GroupGrant
	kvssGroupGrants     []resource.GroupGrant
	swDriverGroupGrants []resource.GroupGrant
	groupGrants         map[string][]resource.GroupGrant
	upperBounds         map[string]resource.Subresources

	groupGrantShortage int // keep track of NG grant shortage in case of partial schedule
	axGrantShortage    int // keep track of AX grant shortage in case of partial schedule

	resourceErrors      []error
	brGroupError        error
	chfGroupErrors      []error
	actGroupErrors      []error
	wgtCmdGroupErrors   []error
	kvssGroupErrors     []error
	swDriverGroupErrors []error
	groupErrors         []string // for each failed group request
	groupErrorType      groupErrorType
}

// Customer-facing lock grant error message
func (g grantResult) String() string {
	sb := strings.Builder{}
	if len(g.resourceErrors) > 0 {
		for i, err := range g.resourceErrors {
			sb.WriteString(err.Error())
			if i != len(g.resourceErrors)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(". ")
	}
	if g.brGroupError != nil {
		sb.WriteString(g.brGroupError.Error())
		sb.WriteString(". ")
	}
	if len(g.chfGroupErrors) > 0 {
		sb.WriteString("chief group request ")
		for i, err := range g.chfGroupErrors {
			sb.WriteString(err.Error())
			if i != len(g.chfGroupErrors)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(". ")
	}
	if len(g.actGroupErrors) > 0 {
		sb.WriteString("activation group request ")
		for i, err := range g.actGroupErrors {
			sb.WriteString(err.Error())
			if i != len(g.actGroupErrors)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(". ")
	}
	if len(g.kvssGroupErrors) > 0 {
		sb.WriteString("kvss group request ")
		for i, err := range g.kvssGroupErrors {
			sb.WriteString(err.Error())
			if i != len(g.kvssGroupErrors)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(". ")
	}
	if len(g.wgtCmdGroupErrors) > 0 {
		sb.WriteString("weight/command group request ")
		for i, err := range g.wgtCmdGroupErrors {
			sb.WriteString(err.Error())
			if i != len(g.wgtCmdGroupErrors)-1 {
				sb.WriteString(", ")
			}
		}
		sb.WriteString(". ")
	}
	if len(g.groupErrors) > 0 {
		sb.WriteString("group request ")
		for i, groupDetails := range g.groupErrors {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(groupDetails)
		}
		sb.WriteString(". ")
	}
	return strings.TrimSpace(sb.String())
}

func (g grantResult) systemRequestCount() int {
	return g.resourceGrants[rlv1.SystemsRequestName].Request.Count
}

func (g grantResult) systemGrantCount() int {
	return g.resourceGrants[rlv1.SystemsRequestName].Size()
}

func (g grantResult) systemGrantShortage() int {
	return g.resourceGrants[rlv1.SystemsRequestName].Request.Count - g.systemGrantCount()
}

func (g grantResult) ok() bool {
	return len(g.resourceErrors) == 0 && g.brGroupError == nil &&
		len(g.chfGroupErrors) == 0 && len(g.actGroupErrors) == 0 && len(g.wgtCmdGroupErrors) == 0 && len(g.groupErrors) == 0 && len(g.kvssGroupErrors) == 0 && len(g.swDriverGroupErrors) == 0
}

func (g grantResult) mergeGroupGrants() map[string][]resource.GroupGrant {
	mergedGGs := map[string][]resource.GroupGrant{}

	if len(g.brGroupGrant.Grants) > 0 {
		mergedGGs[g.brGroupGrant.Request.Id] = []resource.GroupGrant{g.brGroupGrant}
	}
	// Collect all group grant slices into a single slice of slices
	allGroupGrantSlices := [][]resource.GroupGrant{
		g.actGroupGrants,
		g.kvssGroupGrants,
		g.chfGroupGrants,
		g.wgtCmdGroupGrants,
		g.swDriverGroupGrants,
	}

	// Loop through each slice and merge grants
	for _, groupGrants := range allGroupGrantSlices {
		for _, gg := range groupGrants {
			mergedGGs[gg.Request.Id] = append(mergedGGs[gg.Request.Id], gg)
		}
	}

	maps.Copy(mergedGGs, g.groupGrants)
	return mergedGGs
}

// Given a grantResult instance, we check if nvmf granting should be enabled for a job.
// We enable nvmf granting only if the coordinator node and all memoryx nodes have gone through the nvmf deployment.
func (g grantResult) nvmfEnabled(c *resource.Collection) bool {
	shouldEnable := true
	nvmfReadyNodes := map[string]bool{}
	for reqId, rg := range g.resourceGrants {
		if reqId != rlv1.CoordinatorRequestName {
			continue
		}
		resId := rg.ResourceIds[0]
		res, ok := c.Get(resId)
		if ok {
			nvmfReadyNodes[resId] = res.IsNvmfReady()
			shouldEnable = shouldEnable && nvmfReadyNodes[resId]
		}
	}
	for reqId, ggs := range g.groupGrants {
		if !strings.HasPrefix(reqId, rlv1.PrimaryNgPrefix) {
			continue
		}
		for _, gg := range ggs {
			for _, rg := range gg.Grants {
				for _, resId := range rg.ResourceIds {
					res, ok := c.Get(resId)
					if ok && res.IsMemxNode() {
						nvmfReadyNodes[resId] = res.IsNvmfReady()
						shouldEnable = shouldEnable && nvmfReadyNodes[resId]
					}
				}
			}
		}
	}
	logrus.Infof("lock: %s, nvmf enabled: %t, nvmf ready nodes: %v", g.name, shouldEnable, nvmfReadyNodes)
	return shouldEnable
}

package namespace

import (
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"github.com/google/uuid"

	nsv1 "cerebras.com/job-operator/apis/namespace/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

const (
	reqErrPreamble    = "session request not possible because"
	invalidRequestErr = "requested was invalid because"
	resourcesInUseMsg = "Please update session request or check job status with \"csctl get jobs\" and cancel running jobs"
)

// operator- prefix for debugging purpose
func operatorRequestId() string { return "operator-" + uuid.NewString() }

func estimateResourceSpec(collection *resource.Collection, status nsv1.NamespaceReservationStatus, workloadType nsv1.SessionWorkloadType) nsv1.ResourceSpec {
	freeMgmtMemoryMi, popNgCount, lrgNgCount, xlrgNgCount := 0, 0, 0, 0

	for _, ngName := range status.Nodegroups {
		if rs, ok := collection.Get(resource.NodegroupType + "/" + ngName); ok {
			ng := nsrNGFromResource(rs, "")
			if ng.isPop {
				popNgCount += 1
			}
			if ng.isLargeMem {
				lrgNgCount += 1
			} else if ng.isXLargeMem {
				xlrgNgCount += 1
			}
		}
	}
	for _, nodeName := range status.Nodes {
		if rs, ok := collection.Get(resource.NodeType + "/" + nodeName); ok {
			if _, ok = rs.GetProp(resource.RoleCrdPropKey); ok {
				freeMgmtMemoryMi += rs.Subresources().Mem()
			}
		}
	}

	parallelExecuteSysNg := common.Min(len(status.Systems), popNgCount)
	if wsapisv1.IsInferenceCluster || workloadType == nsv1.InferenceWorkloadType {
		// v2-network inference-only cluster || inference session, should have no node group
		parallelExecuteSysNg = len(status.Systems)
	}
	parallelExecuteCrd := freeMgmtMemoryMi / perExecuteCrdMemEstimateMi
	parallelExecute := common.Min(parallelExecuteCrd, parallelExecuteSysNg)
	compileMgmtMem := freeMgmtMemoryMi - (perExecuteCrdMemEstimateMi * parallelExecute)
	parallelCompile := compileMgmtMem / perCompileCrdMemEstimateMi
	return nsv1.ResourceSpec{
		ParallelExecuteCount:  parallelExecute,
		ParallelCompileCount:  parallelCompile,
		LargeMemoryRackCount:  lrgNgCount,
		XLargeMemoryRackCount: xlrgNgCount,
	}
}

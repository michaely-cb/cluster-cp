package namespace

import (
	"github.com/sirupsen/logrus"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

var perCompileCrdMemEstimateMi int
var perExecuteCrdMemEstimateMi int

type nsState struct {
	collection *resource.Collection // resources with allocations, for resource property selection

	// NSR.name -> NSR
	reservations map[string]*namespacev1.NamespaceReservation
}

// Copy returns a shallow copy
func (s nsState) Copy() nsState {
	return nsState{
		collection:   s.collection.Copy(),
		reservations: common.CopyMap(s.reservations),
	}
}

func (s nsState) getNS(rid string) string {
	r, ok := s.collection.Get(rid)
	if ok {
		if v, hasProp := r.GetProp(common.NamespaceKey); hasProp {
			return v
		}
	}

	return ""
}

func (s nsState) setNS(rid, ns string) {
	s.collection.UpdateProp(rid, common.NamespaceKey, ns)
}

// addInitialNSR sets the granted resources of the NSR as allocated in the nsState. Returns deleted resourceIds
func (s nsState) addInitialNSR(nsr *namespacev1.NamespaceReservation) *namespacev1.RequestParams {
	isSetRequired := false
	setResourceParams := &namespacev1.RequestParams{
		UpdateMode: namespacev1.DebugSetMode,
	}

	for _, rid := range nsr.Status.GetAllocatedRIDs() {
		if res, ok := s.collection.Get(rid); !ok {
			isSetRequired = true
		} else {
			s.setNS(rid, nsr.Name) // should be a no-op
			switch res.Type() {
			case resource.SystemType:
				setResourceParams.SystemNames = append(setResourceParams.SystemNames, res.Name())
			case resource.NodegroupType:
				setResourceParams.NodegroupNames = append(setResourceParams.NodegroupNames, res.Name())
			case resource.NodeType:
				if res.IsCrdNode() {
					setResourceParams.CoordinatorNodeNames = append(setResourceParams.CoordinatorNodeNames, res.Name())
				} else if res.IsAxNode() {
					setResourceParams.ActivationNodeNames = append(setResourceParams.ActivationNodeNames, res.Name())
				} else if res.IsIxNode() {
					setResourceParams.InferenceDriverNodeNames = append(setResourceParams.InferenceDriverNodeNames, res.Name())
				}
			}
		}
	}

	// Reconciler expects certain values for proper removal
	// nil -> keep the current value
	// []string{""} -> remove all resources
	// see reconcile_debug.go for more details
	if isSetRequired {
		if setResourceParams.SystemNames == nil {
			setResourceParams.SystemNames = []string{""}
		}
		if setResourceParams.NodegroupNames == nil {
			setResourceParams.NodegroupNames = []string{""}
		}
		if setResourceParams.CoordinatorNodeNames == nil {
			setResourceParams.CoordinatorNodeNames = []string{""}
		}
		if setResourceParams.ActivationNodeNames == nil {
			setResourceParams.ActivationNodeNames = []string{""}
		}
		if setResourceParams.InferenceDriverNodeNames == nil {
			setResourceParams.InferenceDriverNodeNames = []string{""}
		}
	}

	// for testing only
	if wsapisv1.AutoSetAxOnNsrInit {
		setResourceParams.AutoSelectActivationNodes = true
		setResourceParams.ActivationNodeNames = nil
	}
	s.reservations[nsr.Name] = nsr
	return common.Ternary(isSetRequired, setResourceParams, nil)
}

// public for testing
func InitMemConstants() {
	// estimates for how much memory an execute / compile coordinator requires
	perCompileCrdMemEstimateMi = wsapisv1.CrdCompileMinMemoryBytes >> 20
	if perCompileCrdMemEstimateMi == 0 {
		perCompileCrdMemEstimateMi = 50 << 10 // 50 Gi in Mi
	}
	logrus.Infof("init perCompileCrdMemEstimateMi %dMi", perCompileCrdMemEstimateMi)

	perExecuteCrdMemEstimateMi = wsapisv1.CrdExecuteMinMemoryBytes >> 20
	if perExecuteCrdMemEstimateMi == 0 {
		perExecuteCrdMemEstimateMi = 32 << 10 // 32 Gi in Mi
	}
	logrus.Infof("init perExecuteCrdMemEstimateMi %dMi", perExecuteCrdMemEstimateMi)
}

func init() {
	InitMemConstants()
}

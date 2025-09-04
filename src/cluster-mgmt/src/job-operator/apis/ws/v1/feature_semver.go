package v1

import (
	"github.com/coreos/go-semver/semver"
)

type SoftwareFeature string

const (
	CompressedConfig             SoftwareFeature = "compressed cluster config"
	ParallelExperts              SoftwareFeature = "parallel experts"
	RestartWorkflow              SoftwareFeature = "restart workflow"
	ScInfraSetup                 SoftwareFeature = "security context infra setup"
	DebugVolume                  SoftwareFeature = "debug volume readiness"
	WseDomainHydration           SoftwareFeature = "wse domain hydration"
	HeartBeatJobStatus           SoftwareFeature = "heartbeat poll job status"
	KvssSupport                  SoftwareFeature = "prompt caching support"
	InferenceJobSharing          SoftwareFeature = "inference job sharing"
	LinearMemoryRangeSupport     SoftwareFeature = "linear memory range support"
	ClientSideTaskCommandSupport SoftwareFeature = "client-side appliance task command enablement support"
	SWDriverIXSupport            SoftwareFeature = "swdriver ix scheduling support"
	ExpertParallelOnBr           SoftwareFeature = "expert parallel on BR for weight and gradient"
	GenericRTMemFairShareAndLMR  SoftwareFeature = "improved memory fair-share during job sharing and upperbound granting, LMR support for generic RT"
)

var SoftwareFeatureSemverMap = map[SoftwareFeature]semver.Version{
	CompressedConfig:             *semver.New("1.0.2"),
	ParallelExperts:              *semver.New("1.0.5"),
	RestartWorkflow:              *semver.New("1.0.6"),
	ScInfraSetup:                 *semver.New("1.0.7"),
	DebugVolume:                  *semver.New("1.0.8"),
	WseDomainHydration:           *semver.New("1.0.9"),
	HeartBeatJobStatus:           *semver.New("1.0.10"),
	KvssSupport:                  *semver.New("1.0.11"),
	InferenceJobSharing:          *semver.New("1.0.12"),
	LinearMemoryRangeSupport:     *semver.New("1.0.13"),
	ClientSideTaskCommandSupport: *semver.New("1.1.0"),
	SWDriverIXSupport:            *semver.New("1.2.0"),
	ExpertParallelOnBr:           *semver.New("1.3.0"),
	GenericRTMemFairShareAndLMR:  *semver.New("1.3.1"),
}

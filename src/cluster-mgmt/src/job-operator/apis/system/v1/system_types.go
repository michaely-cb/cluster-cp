/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	"fmt"
	"sort"
	"strings"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type SystemState string

const (
	HealthOk                  = "ok"
	HealthDegraded            = "degraded"
	HealthErr                 = "err"
	PortAffinityPropKeyPrefix = "port-affinity"
	// DefaultNumPortDomains is the fallback domain count (used by cs2 / cs3).
	DefaultNumPortDomains = 4
)

// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

//+genclient
//+genclient:nonNamespaced
//+resource:path=system
//+kubebuilder:object:generate=true
//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:scope=Cluster
//+kubebuilder:printcolumn:name="type",type=string,JSONPath=`.spec.type`
//+kubebuilder:printcolumn:name="controlAddress",type=string,JSONPath=`.spec.controlAddress`
//+kubebuilder:printcolumn:name="managementAddress",type=string,JSONPath=`.spec.managementAddress`

type System struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec SystemSpec `json:"spec"`
	// +kubebuilder:default={state: "OK", message: ""}
	Status SystemStatus `json:"status,omitempty"`
}

type SystemSpec struct {
	Name string `json:"name"`
	// +kubebuilder:default=cs2
	Type     string `json:"type,omitempty"`
	CmAddr   string `json:"controlAddress"`
	MgmtAddr string `json:"managementAddress,omitempty"`
	Ports    []Port `json:"-"`
	// HasError means systems has either internal error or port error
	HasError bool `json:"-"`
	// +kubebuilder:default=false
	Unschedulable bool              `json:"unschedulable,omitempty"`
	Properties    map[string]string `json:"-"`
	Hostname      string            `json:"hostname,omitempty"`
}

type SystemStatus struct {
	// +kubebuilder:default=""
	// Deprecated, to be removed at rel-2.6
	State SystemState `json:"state,omitempty"`
	// +kubebuilder:default=""
	Message    string                 `json:"message,omitempty"`
	Conditions []corev1.NodeCondition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
type SystemList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []System `json:"items"`
}

type Port struct {
	Name     string `json:"-"`
	HasError bool   `json:"-"`
}

func (ss *SystemSpec) GetName() string {
	return ss.Name
}

func (ss *SystemSpec) GetType() string {
	return ss.Type
}

func (ss *SystemSpec) GetProperties() map[string]string {
	return ss.Properties
}

func (ss *SystemSpec) GetGroupName() string {
	return ss.Properties["group"]
}

func (ss *SystemSpec) GetStamp() string {
	return ss.Properties["stamp"]
}

// PortCount returns number of data ports for this system.
func (ss *SystemSpec) PortCount() int {
	return GetCSVersionSpec(CSVersionType(ss.Type)).NumPorts
}

func (ss *SystemSpec) InitPorts() {
	if len(ss.Ports) == 0 {
		portCount := ss.PortCount()
		for i := 0; i < portCount; i++ {
			ss.Ports = append(ss.Ports, Port{
				Name: fmt.Sprintf("n%.2d", i),
			})
		}
	}
}

func (ss *SystemSpec) GetAvailableCsPorts() int {
	res := 0
	for _, p := range ss.Ports {
		if !p.HasError {
			res++
		}
	}
	return res
}

func (ss *SystemSpec) GetErrorPorts() []string {
	var res []string
	for _, port := range ss.Ports {
		if port.HasError {
			res = append(res, port.Name)
		}
	}
	return res
}

// returns whether system can be considered as schedulable
// e.g. port errors may not block scheduling
func (ss *SystemSpec) IsSchedulable() (string, bool) {
	// return true if no error
	if !ss.HasError {
		return HealthOk, true
	}
	// if err, either system error or port error
	// no port errors indicates system level error which is blocking
	if len(ss.GetErrorPorts()) == 0 {
		return HealthErr, false
	}
	// port level error, only incompatible ports errors blocks scheduling
	// note: this check is a stricter bound for a particular system needed for BR scheduling.
	// For AX scheduling, port level error across groups on a particular system can be tolerated.
	// As long as there is at least one usable port per domain, AX scheduling should accommodate that.
	_, err := SystemPortCompatibleCheck([]*SystemSpec{ss})
	if err == nil {
		return HealthDegraded, true
	}
	return HealthErr, false
}

// check whether assigned systems are valid to BR usage
// i.e. supporting one port failure per fpga domain / one group failure at most for now
// return error group id if found + true if valid
// can be scaled for more failure case in future
// Warning: currently systems are scheduled in ungrouped way, but it can lead to unoptimized option
// e.g. sys0-port0, sys1-port1, sys2-port3 down, for 2cs2 job, it should go with sys0-sys2
// it may recover in next cycles, keep simple for now but watch for potential optimizations
func SystemPortCompatibleCheck(systems []*SystemSpec) (int, error) {
	groupId := -1
	errorGroups := map[int]int{}
	errorInfo := map[string][]string{}
	for _, sys := range systems {
		spec := GetCSVersionSpec(CSVersionType(sys.Type))
		for id, port := range sys.Ports {
			if port.HasError {
				groupId = spec.Group(id)
				errorGroups[groupId]++
				errorInfo[sys.Name] = append(errorInfo[sys.Name], port.Name)
			}
			// error out if multiple groups error out within same/across systems
			if len(errorGroups) > 1 {
				err := fmt.Errorf("multiple port group failures: %s", fmtPortGroupErrors(errorInfo))
				logrus.Errorf(err.Error())
				return -1, err
			}
		}
	}
	return groupId, nil
}

func fmtPortGroupErrors(sysPorts map[string][]string) string {
	sb := strings.Builder{}
	for sys, ports := range sysPorts {
		if sb.Len() != 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(sys)
		sb.WriteString("{ports=")
		for i, port := range ports {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(port)
		}
		sb.WriteString("}")
	}
	return sb.String()
}

func GetParsedSysPortAffinity(sys string, portId int) string {
	return fmt.Sprintf("%s-%d", sys, portId)
}

func GetSysPortAffinityKey(sys string, portId int) string {
	return fmt.Sprintf("%s/%s", PortAffinityPropKeyPrefix, GetParsedSysPortAffinity(sys, portId))
}

// skip Unschedulable which needs decision case by case
func (ss *SystemSpec) Equals(o *SystemSpec) bool {
	return ss.Name == o.Name && ss.Type == o.Type && ss.CmAddr == o.CmAddr && ss.MgmtAddr == o.MgmtAddr && ss.Hostname == o.Hostname
}

// skip Unschedulable which needs decision case by case
func (ss *SystemSpec) Copy(o *SystemSpec) {
	ss.Name = o.Name
	ss.Type = o.Type
	ss.CmAddr = o.CmAddr
	ss.MgmtAddr = o.MgmtAddr
	ss.Hostname = o.Hostname
}

// CSVersionType represents a hardware generation (cs2 / cs3 / cs4).
// A minimal spec abstraction is introduced to centralize per-version constants (port counts, domain layout, etc.).
// while allowing future extension (e.g. max BR pods per node, NIC bandwidth patterns) without scattering switch logic.
type CSVersionType string

const (
	CSVersion2 CSVersionType = "cs2"
	CSVersion3 CSVersionType = "cs3"
	CSVersion4 CSVersionType = "cs4"
)

// CSVersionSpec encapsulates CS-version–dependent topology traits.
type CSVersionSpec struct {
	Version        CSVersionType
	NumPorts       int
	NumPortDomains int
}

// NumGroups returns how many ports per domain (group width).
func (s CSVersionSpec) NumGroups() int {
	if s.NumPortDomains == 0 {
		return 0
	}
	return s.NumPorts / s.NumPortDomains
}

// Group returns the group (column) index for a port.
func (s CSVersionSpec) Group(portId int) int {
	return portId % s.NumGroups()
}

// Domain returns the domain (row) index for a port.
func (s CSVersionSpec) Domain(portId int) int {
	gw := s.NumGroups()
	if gw == 0 {
		return -1
	}
	return portId / gw
}

// PortsFromDomain returns all port ids in a domain.
func (s CSVersionSpec) PortsFromDomain(domainId int) []int {
	gw := s.NumGroups()
	if gw == 0 || domainId < 0 || domainId >= s.NumPortDomains {
		return nil
	}
	start := domainId * gw
	ports := make([]int, 0, gw)
	for i := 0; i < gw; i++ {
		ports = append(ports, start+i)
	}
	return ports
}

var (
	csVersionSpecs = map[CSVersionType]CSVersionSpec{
		// cs2 / cs3: 12 ports, 4 domains => 3 ports per domain
		CSVersion2: {Version: CSVersion2, NumPorts: 12, NumPortDomains: DefaultNumPortDomains},
		CSVersion3: {Version: CSVersion3, NumPorts: 12, NumPortDomains: DefaultNumPortDomains},
		// cs4: 24 ports, 8 domains => still 3 ports per domain (wider domain fanout)
		CSVersion4: {Version: CSVersion4, NumPorts: 24, NumPortDomains: 8},
	}
	defaultCSVersionSpec = csVersionSpecs[CSVersion2] // default to cs2 spec for unknown/mixed versions
)

// GetCSVersionSpec returns the spec for a CS version (falls back to default for unknown/mixed).
func GetCSVersionSpec(t CSVersionType) CSVersionSpec {
	if spec, ok := csVersionSpecs[t]; ok {
		return spec
	}
	return defaultCSVersionSpec
}

func logSystemTypeSummary(systems []*SystemSpec) {
	sort.Slice(systems, func(i, j int) bool {
		return systems[i].Type < systems[j].Type
	})
	var sb strings.Builder
	sb.WriteString("System type summary:\n")
	for _, s := range systems {
		if s == nil {
			continue
		}
		sb.WriteString(fmt.Sprintf("  %s: %s\n", s.Type, s.Name))
	}
	logrus.Info(sb.String())
}

// DetectCSVersionSpec inspects the provided systems:
// - If all are cs2/cs3 (any mix), returns cs2 spec.
// - If all are cs4, returns cs4 spec.
// - If cs4 is mixed with cs2/cs3, panics.
// - Otherwise returns cs4 spec.
func DetectCSVersionSpec(systems []*SystemSpec) CSVersionSpec {
	hasCS2 := false
	hasCS3 := false
	hasCS4 := false

	for _, s := range systems {
		if s == nil {
			continue
		}
		t := CSVersion2
		if s.Type != "" {
			t = CSVersionType(s.Type)
		}
		switch t {
		case CSVersion2:
			hasCS2 = true
		case CSVersion3:
			hasCS3 = true
		case CSVersion4:
			hasCS4 = true
		}
	}

	if hasCS4 && (hasCS2 || hasCS3) {
		logSystemTypeSummary(systems)
		panic("Invalid system version mix: cs4 cannot coexist with cs2/cs3")
	}
	if hasCS4 {
		return GetCSVersionSpec(CSVersion4)
	}
	if hasCS2 && hasCS3 {
		// mixed cs2/cs3: return cs2 spec
		return GetCSVersionSpec(CSVersion2)
	}
	if hasCS2 {
		return GetCSVersionSpec(CSVersion2)
	}
	if hasCS3 {
		return GetCSVersionSpec(CSVersion3)
	}
	return defaultCSVersionSpec
}

func init() {
	SchemeBuilder.Register(&System{}, &SystemList{})
}

package resource_config

import (
	apiresource "k8s.io/apimachinery/pkg/api/resource"

	"cerebras.com/job-operator/common/resource"
)

type cfgSystem struct {
	name string
}

func (c cfgSystem) GetName() string                  { return c.name }
func (c cfgSystem) GetType() string                  { return "cs2" }
func (c cfgSystem) GetAvailableCsPorts() int         { return 12 }
func (c cfgSystem) GetProperties() map[string]string { return nil }
func (c cfgSystem) IsSchedulable() (string, bool)    { return resource.HealthOk, true }

type cfgNode struct {
	name       string
	role       string
	properties map[string]string
}

func (c cfgNode) GetName() string { return c.name }
func (c cfgNode) GetRole() string { return c.role }
func (c cfgNode) GetExpandedRoles() map[string]bool {
	if c.role == "management" {
		return map[string]bool{c.role: true, "coordinator": true}
	}
	return map[string]bool{c.role: true}
}
func (c cfgNode) GetProperties() map[string]string                        { return c.properties }
func (c cfgNode) GetAvailableCsPorts() (map[int][]string, map[string]int) { return nil, nil }
func (c cfgNode) IsSchedulable() (string, bool)                           { return resource.HealthOk, true }

func newCfgNode(name, role string, props ...string) INodeCfg {
	return cfgNode{
		name:       name,
		role:       role,
		properties: resource.MustSplitProps(props),
	}
}

type MockNodeCapacity struct {
	cpu apiresource.Quantity
	mem apiresource.Quantity
}

func MockNodeCapacityNormal() MockNodeCapacity {
	return MockNodeCapacity{
		cpu: apiresource.MustParse("64"),
		mem: apiresource.MustParse("128Gi"),
	}
}

func MockNodeCapacityLarge() MockNodeCapacity {
	return MockNodeCapacity{
		cpu: apiresource.MustParse("64"),
		mem: apiresource.MustParse("1Ti"),
	}
}

func MockNodeCapacityXLarge() MockNodeCapacity {
	return MockNodeCapacity{
		cpu: apiresource.MustParse("64"),
		mem: apiresource.MustParse("2Ti"),
	}
}

func (m MockNodeCapacity) Cpu(node string) apiresource.Quantity {
	return m.cpu
}

func (m MockNodeCapacity) Mem(node string) apiresource.Quantity {
	return m.mem
}

func (m MockNodeCapacity) Equals(n NodeCapacity) bool {
	return true
}

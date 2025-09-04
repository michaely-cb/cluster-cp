//go:build default

package resource_config

import (
	"strconv"
	"testing"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common/resource"
)

type assertCollection func(*testing.T, *resource.Collection)

func groupHasProp(name, key, expect string) assertCollection {
	return func(t *testing.T, c *resource.Collection) {
		g, ok := c.GetGroup(name)
		if !ok {
			t.Errorf("expected group %s to exist but did not", name)
			t.FailNow()
		}
		val, hasKey := g.GetProp(key)
		if hasKey && expect == "" {
			t.Errorf(
				"expected group %s to not have prop %s but got val '%s'",
				g.Name(), key, val,
			)
		} else if expect != "" && val != expect {
			t.Errorf(
				"expected group %s to have prop %s=%s but got val '%s'",
				g.Name(), key, expect, val,
			)
		}
	}
}
func groupIsDepopulated(name string) assertCollection {
	return groupHasProp(name, wsapisv1.MemxPopNodegroupProp, "false")
}
func groupIsPopulated(name string) assertCollection {
	return groupHasProp(name, wsapisv1.MemxPopNodegroupProp, "true")
}
func groupMemxClassRegular(name string) assertCollection {
	return groupHasProp(name, wsapisv1.MemxMemClassProp, wsapisv1.PopNgTypeRegular)
}

func groupMemxClassLarge(name string) assertCollection {
	return groupHasProp(name, wsapisv1.MemxMemClassProp, wsapisv1.PopNgTypeLarge)
}

func groupMemxClassXLarge(name string) assertCollection {
	return groupHasProp(name, wsapisv1.MemxMemClassProp, wsapisv1.PopNgTypeXLarge)
}

func nodeHasProp(name, key, expect string) assertCollection {
	return func(t *testing.T, c *resource.Collection) {
		rs, ok := c.Get("node/" + name)
		if !ok {
			t.Errorf("expected node %s to exist but did not", name)
			t.FailNow()
		}
		val, hasKey := rs.GetProp(key)
		if !hasKey || val != expect {
			t.Errorf(
				"expected node %s to have prop %s=%s but got val '%s'",
				rs.Name(), key, expect, val,
			)
		}
	}
}

func newCfgSystems(numCsx int) []ISystemCfg {
	var systemCfg []ISystemCfg
	for i := 0; i < numCsx; i++ {
		systemCfg = append(systemCfg, cfgSystem{name: "s" + strconv.Itoa(i)})
	}
	return systemCfg
}

func TestInitializeResourceManagement(t *testing.T) {
	memxPerPopNgOrig := wsapisv1.MinMemxPerPopNodegroup
	wsapisv1.MinMemxPerPopNodegroup = 2
	defer func() {
		wsapisv1.MinMemxPerPopNodegroup = memxPerPopNgOrig
	}()

	testCases := []struct {
		name          string
		nodes         []INodeCfg
		systems       []ISystemCfg
		groupProps    map[string]map[string]string
		nodeHealthy   IsResourceHealthy
		systemHealthy IsResourceHealthy
		nodeCapacity  NodeCapacity

		assertions []assertCollection
	}{
		{
			name: "single group defaults to populated",
			nodes: []INodeCfg{
				newCfgNode("n00", "management", "group:ng0"),
				newCfgNode("n01", "memory", "group:ng0"),
				newCfgNode("n02", "worker", "group:ng0"),
			},
			systems:    newCfgSystems(1),
			groupProps: map[string]map[string]string{"ng0": {}},

			assertions: []assertCollection{groupIsPopulated("ng0"), groupMemxClassRegular("ng0")},
		},
		{
			name: "two group adds depop prop",
			nodes: []INodeCfg{
				newCfgNode("n00", "management", "group:ng0", "ns:alpha"),
				newCfgNode("n01", "memory", "group:ng0"),
				newCfgNode("n02", "memory", "group:ng0"),
				newCfgNode("n03", "worker", "group:ng0"),

				newCfgNode("n10", "memory", "group:ng1"),
				newCfgNode("n11", "worker", "group:ng1"),
			},
			systems:    newCfgSystems(2),
			groupProps: map[string]map[string]string{"ng0": {"ns": "beta"}, "ng1": {}},

			assertions: []assertCollection{
				groupIsPopulated("ng0"),
				groupIsDepopulated("ng1"),
				groupMemxClassRegular("ng0"),
				groupMemxClassRegular("ng1"),
				nodeHasProp("n00", "ns", "alpha"),
			},
		},

		{
			name: "two group adds depop prop when pop group unhealthy",
			nodes: []INodeCfg{
				newCfgNode("n00", "management", "group:ng0"),
				newCfgNode("n01", "memory", "group:ng0"),
				newCfgNode("n02", "memory", "group:ng0"),
				newCfgNode("n03", "worker", "group:ng0"),

				newCfgNode("n10", "memory", "group:ng1"),
				newCfgNode("n11", "memory", "group:ng0"),
				newCfgNode("n12", "worker", "group:ng1"),
			},
			nodeHealthy: func(n string) bool { return n != "n10" },
			systems:     newCfgSystems(2),
			groupProps:  map[string]map[string]string{"ng0": {}, "ng1": {}},

			assertions: []assertCollection{groupIsPopulated("ng0"), groupIsDepopulated("ng1")},
		},

		{
			name: "large memx class added",
			nodes: []INodeCfg{
				newCfgNode("n00", "management", "group:ng0"),
				newCfgNode("n01", "memory", "group:ng0"),
				newCfgNode("n02", "memory", "group:ng0"),
				newCfgNode("n03", "worker", "group:ng0"),
			},
			systems:      newCfgSystems(2),
			groupProps:   map[string]map[string]string{"ng0": {}, "ng1": {}},
			nodeCapacity: MockNodeCapacityLarge(),

			assertions: []assertCollection{groupMemxClassLarge("ng0")},
		},

		{
			name: "xlarge memx class added",
			nodes: []INodeCfg{
				newCfgNode("n00", "management", "group:ng0"),
				newCfgNode("n01", "memory", "group:ng0"),
				newCfgNode("n02", "memory", "group:ng0"),
				newCfgNode("n03", "worker", "group:ng0"),
			},
			systems:      newCfgSystems(2),
			groupProps:   map[string]map[string]string{"ng0": {}, "ng1": {}},
			nodeCapacity: MockNodeCapacityXLarge(),

			assertions: []assertCollection{groupMemxClassXLarge("ng0")},
		},

		{
			name: "affinity propagation",
			nodes: []INodeCfg{
				newCfgNode("n00", "activation",
					"stamp:0", resource.PortAffinityPropKeyPrefix+"/systemf0-7:", resource.PortAffinityPropKeyPrefix+"/systemf1-7:"),
			},
			systems: newCfgSystems(2),

			assertions: []assertCollection{
				nodeHasProp("n00", "stamp", "0"),
				nodeHasProp("n00", resource.PortAffinityPropKeyPrefix+"/systemf0-7", ""),
				nodeHasProp("n00", resource.PortAffinityPropKeyPrefix+"/systemf1-7", ""),
			},
		},
	}

	for _, tc := range testCases {
		if tc.nodeCapacity == nil {
			tc.nodeCapacity = MockNodeCapacityNormal()
		}

		t.Run(tc.name, func(t *testing.T) {
			c := InitializeResourceManagement(
				tc.nodes,
				tc.groupProps,
				tc.systems,
				map[string]string{}, // ns assignment
				tc.nodeCapacity,
			)

			for _, assert := range tc.assertions {
				assert(t, c)
			}
		})
	}
}

package resource

import (
	"strconv"

	"golang.org/x/exp/slices"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
)

type Group struct {
	name       string
	properties map[string]string
}

func NewGroup(name string, properties map[string]string) Group {
	return Group{
		name:       name,
		properties: properties,
	}
}

func (g Group) Name() string {
	return g.name
}

func (g Group) Props() map[string]string {
	return CopyMap(g.properties)
}

func (g Group) GetProp(k string) (string, bool) {
	v, ok := g.properties[k]
	return v, ok
}

// IsPopulated returns true if the nodegroup is populated.
func (g Group) IsPopulated() bool {
	isPop := false
	if val, ok := g.GetProp(wsapisv1.MemxPopNodegroupProp); ok {
		if _val, err := strconv.ParseBool(val); err == nil {
			isPop = _val
		}
	}
	return isPop
}

// HasProps returns true if the group has the properties specified.
// If props is nil or empty, this function also returns true.
// E.g. props = {"a": ["1", "2"], "b": ["3"]}
// will return true that if the group has a property "a" with value "1" or "2"
// and a property "b" with value "3".
func (g Group) HasProps(props map[string][]string) bool {
	for propK, propV := range props {
		v, ok := g.GetProp(propK)
		if !ok || !slices.Contains(propV, v) {
			return false
		}
		continue
	}
	return true
}

func (g Group) DeepCopy() Group {
	return Group{
		name:       g.name,
		properties: CopyMap(g.properties),
	}
}

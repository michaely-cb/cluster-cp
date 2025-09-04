package resource

import (
	"fmt"
	"sort"
	"strings"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

// Map returns a list of mapper applied to every element of eles
func Map[A any, B any](mapper func(a A) B, eles []A) []B {
	var rv []B
	for _, ele := range eles {
		rv = append(rv, mapper(ele))
	}
	return rv
}

// MapValues returns a list of mapper applied to every element of eles
func MapValues[K comparable, A any, B any](mapper func(a A) B, eles map[K]A) map[K]B {
	rv := map[K]B{}
	for k, ele := range eles {
		rv[k] = mapper(ele)
	}
	return rv
}

// CopyMap does a shallow copy of a map by returning a new map with the same k/v's
func CopyMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return nil
	}

	clone := make(map[K]V, len(m))
	for k, v := range m {
		clone[k] = v
	}
	return clone
}

// Keys returns the keys of a map
func Keys[K comparable, V any](m map[K]V) []K {
	rv := make([]K, 0, len(m))
	for k, _ := range m {
		rv = append(rv, k)
	}
	return rv
}

// SortedKeys returns the sorted keys of a map by key comparison
func SortedKeys[K constraints.Ordered, V any](m map[K]V) []K {
	rv := Keys(m)
	slices.Sort(rv)
	return rv
}

// SortedKeysByValSize returns the sorted keys of a map by val length comparison
func SortedKeysByValSize[K constraints.Ordered, V any](m map[K][]V, smallerFirst bool) []K {
	rv := Keys(m)
	sort.Slice(rv, func(i, j int) bool {
		if len(m[rv[i]]) != len(m[rv[j]]) {
			return smallerFirst && len(m[rv[i]]) < len(m[rv[j]]) || !smallerFirst && len(m[rv[i]]) > len(m[rv[j]])
		}
		return rv[i] < rv[j]
	})
	return rv
}

func EnsureMap[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return make(map[K]V, 0)
	}
	return m
}

// MustSplitProps splits strings of form "key:value" into map[key] = value
func MustSplitProps(props []string) map[string]string {
	rv, err := SplitProps(props)
	if err != nil {
		panic(err)
	}
	return rv
}

// SplitProps splits strings of form "key:value" into map[key] = value
func SplitProps(props []string) (map[string]string, error) {
	p := map[string]string{}
	for _, prop := range props {
		if prop == "" {
			continue
		}
		kv := strings.Split(prop, ":")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid props list: must pass props in form <key>:<value>")
		}
		p[kv[0]] = kv[1]
	}
	return p, nil
}

func SplitValues(m map[string]string) map[string][]string {
	rv := map[string][]string{}
	for k, v := range m {
		rv[k] = strings.Split(v, ",")
	}
	return rv
}

func Max[K constraints.Ordered](x, y K) K {
	if x < y {
		return y
	}
	return x
}

func PlatformVersionResKey(k string) string {
	return fmt.Sprintf("%s-%s", PlatformVersionPropertyKey, k)
}

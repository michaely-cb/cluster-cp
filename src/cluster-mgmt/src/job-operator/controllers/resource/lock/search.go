package lock

import (
	"sort"

	"cerebras.com/job-operator/common"
)

type searchable interface {
	Id() string
	Cost() int
}

// search s for some unique combination of s[i] -> value in s[i] by exhaustively searching. Returns nil if no such
// combination exists. Since this is an exhaustive depth first search, it is not recommended for large s.
// Today, it is used to find unique combinations of nodegroups where nodegroups is typically < 32.
// examples:
//
//	s = {a: [x, y], b: [x], c: [y, z]} -> {a: y, b: x, c: z}
//	s = {a: [x, y], b: [x], c: [x, y]} -> nil
func search[S searchable](s map[string][]S) map[string]S {
	keys := common.Keys(s)

	// perf optimization: do an initial pass to see if the search is even possible
	used := map[string]bool{}
	for _, k := range keys {
		for _, v := range s[k] {
			used[v.Id()] = false
		}
	}
	if len(used) < len(keys) {
		return nil
	}

	// perf optimization: sort keys by length of s[key] so that we search the smallest set first
	sort.Slice(keys, func(i, j int) bool {
		return len(s[keys[i]]) < len(s[keys[j]])
	})

	// result optimization: sort each searchable in the candidate lists by score so that we approx
	// search for resources with the lowest remaining
	for _, k := range keys {
		sort.Slice(s[k], func(i, j int) bool {
			return s[k][i].Cost() < s[k][j].Cost()
		})
	}

	selection := map[string]S{}

	var searchIndex func(i int) bool
	searchIndex = func(i int) bool {
		if i == len(keys) {
			return true
		}
		for _, v := range s[keys[i]] {
			if !used[v.Id()] {
				used[v.Id()] = true
				if searchIndex(i + 1) {
					selection[keys[i]] = v
					return true
				}
				used[v.Id()] = false
			}
		}
		return false
	}

	if searchIndex(0) {
		return selection
	}
	return nil
}

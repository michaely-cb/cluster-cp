package ws

import (
	"strings"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

// End user configs which modify the lock request.
type schedulerHints struct {
	AllowSystems    []string
	AllowNodegroups []string

	// WeightGroupProps are additional selection on properties for weight nodes in the nodegroups requests.
	// Deprecated: deprecated for external use, use cpu/mem subresource requests to select weight nodes instead.
	WeightGroupProps map[string][]string
}

func (r *WSJobReconciler) schedulerHintsFromJob(j *wsapisv1.WSJob) (schedulerHints, *CriticalError) {
	hints := schedulerHints{
		WeightGroupProps: map[string][]string{
			wsapisv1.MemxPopNodegroupProp: {"true"}, // always select populated node groups
		},
	}

	for k, v := range j.Annotations {
		if !strings.HasPrefix(k, wsapisv1.SchedulerHintPrefix) {
			continue
		}
		hint := k[len(wsapisv1.SchedulerHintPrefix):]
		if k == wsapisv1.SchedulerHintAllowSys {
			hints.AllowSystems = strings.Split(v, ",")
			if len(hints.AllowSystems) < int(j.Spec.NumWafers) {
				return hints, CriticalErrorf(
					"Job requested %d systems but only specified %d hint system(s)",
					int(j.Spec.NumWafers), len(hints.AllowSystems))
			}

			unhealthySystemsAllowed := false
			if j.Annotations[wsapisv1.EnableAllowUnhealthySystems] == "true" {
				unhealthySystemsAllowed = true
			}

			for _, system := range hints.AllowSystems {
				if _, ok := r.Cfg.GetAssignedSystemsMap(j.Namespace)[system]; !ok {
					if _, ok = r.Cfg.UnhealthySystems[system]; ok {
						if unhealthySystemsAllowed {
							r.Log.Info("*** Hint system is unhealthy ***", "system",
								system, "cluster-status",
								r.Cfg.PrettyHealthInfo(j.Namespace, false))
						} else {
							return hints, CriticalErrorf(
								"Hint system: %s is unhealthy. Cluster status: %s",
								system, r.Cfg.PrettyHealthInfo(j.Namespace, false))
						}
					}
					return hints, CriticalErrorf(
						"Hint system: %s not found for current NS assignment. Existing systems: %v. Cluster status: %s",
						system, r.Cfg.GetAssignedSystemsList(j.Namespace), r.Cfg.PrettyHealthInfo(j.Namespace, false))
				}
			}
		} else if k == wsapisv1.SchedulerHintAllowNG {
			hints.AllowNodegroups = strings.Split(v, ",")
			totalNgCount := j.GetTotalNodegroupCount()
			if len(hints.AllowNodegroups) < totalNgCount {
				return hints, CriticalErrorf(
					"Job requested %d systems %d nodegroups in total but only specified %d hint node group(s)",
					int(j.Spec.NumWafers), totalNgCount, len(hints.AllowNodegroups))
			}
			for _, ng := range hints.AllowNodegroups {
				groupMap := r.Cfg.GetAssignedGroupsMap(j.Namespace)
				if _, ok := groupMap[ng]; !ok {
					return hints, CriticalErrorf(
						"Value error for scheduler hint allow-nodegroups: group '%s' not found. Existing nodegroups: %s",
						ng, r.Cfg.GetAssignedGroupsList(j.Namespace))
				}
			}
		} else if k == wsapisv1.SchedulerHintWGProps {
			// weight-group-props is a comma separated list of key:value pairs
			for _, wgkv := range strings.Split(v, ",") {
				kv := strings.Split(wgkv, ":")
				if len(kv) != 2 {
					return hints, CriticalErrorf("invalid weight-group-props list: must pass props in form <key>:<value>")
				}
				if kv[0] == wsapisv1.MemxPopNodegroupProp {
					continue // ignore user override of memx-pop
				}
				hints.WeightGroupProps[kv[0]] = []string{kv[1]}
			}
		} else {
			return hints, CriticalErrorf("unknown scheduler-hint '%s'", hint)
		}
	}
	if common.EnableStrictAllowSystems && j.Spec.NumWafers > 0 && len(hints.AllowSystems) == 0 {
		return hints, CriticalErrorf("invalid job: required scheduler-hint 'allow-systems' is not set. " +
			"This cluster is configured to required specifying target systems for a job. You must set " +
			"'allow-systems' scheduler hint with precisely the set of systems you intend to target")
	}
	return hints, nil
}

func (s schedulerHints) apply(lock *rlv1.ResourceLock) {
	if len(s.AllowSystems) > 0 {
		// find any system resource requests and add a property.name matcher
		for rqId := range lock.Spec.ResourceRequests {
			req := &lock.Spec.ResourceRequests[rqId]
			if req.Type == rlv1.SystemResourceType {
				req.Properties = common.EnsureMap(req.Properties)
				req.Properties["name"] = s.AllowSystems
			}
		}
	}

	for _, grr := range lock.Spec.GroupResourceRequests {
		// AX scheduling ignores scheduler hints from v1 networking
		if strings.HasPrefix(grr.Name, rlv1.ActivationRequestName) ||
			strings.HasPrefix(grr.Name, rlv1.ChiefRequestName) ||
			strings.HasPrefix(grr.Name, rlv1.KVStorageServerRequestName) {
			continue
		}
		if len(s.AllowNodegroups) > 0 {
			for _, rr := range grr.ResourceRequests {
				rr.Properties[resource.GroupPropertyKey] = s.AllowNodegroups
			}
		}

		// check the primary nodegroup and see if there are any node requests called "Weight", if so, add the
		// properties to the Weight request
		if !strings.HasPrefix(grr.Name, rlv1.PrimaryNgPrefix) {
			continue
		}
		for _, rr := range grr.ResourceRequests {
			if rr.Type == rlv1.NodeResourceType && rr.Name == rlv1.WeightRequestName {
				rr.Properties = common.EnsureMap(rr.Properties)
				for k, v := range s.WeightGroupProps {
					rr.Properties[k] = v
				}
			}
		}
	}
}

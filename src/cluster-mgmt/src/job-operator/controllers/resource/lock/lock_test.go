//go:build default

package lock

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"

	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	"cerebras.com/job-operator/common/resource"
)

var (
	ungroupReqAnnot = map[string]string{"note": "ungroup-request"}
	groupReqAnnot   = map[string]string{"note": "group-request"}
	sysReq          = resource.Request{
		Id:          rlv1.SystemsRequestName,
		Count:       2,
		Annotations: ungroupReqAnnot,
		Filter: resource.NewFilter(
			rlv1.SystemResourceType,
			nil,
			resource.NewSystemResourceRequest(resource.DefaultSystemPortRequest)),
	}
	coordReq = resource.Request{
		Id:          rlv1.CoordinatorRequestName,
		Count:       1,
		Annotations: ungroupReqAnnot,
		Filter:      resource.NewNodeFilter("management", 4, 4),
	}
	memNodeReq = resource.Request{
		Id:          "memory",
		Count:       2,
		Annotations: groupReqAnnot,
		Filter:      resource.NewNodeFilter("memory", 4, 4),
	}

	ngPrimaryRequest = resource.GroupRequest{
		Id:    rlv1.PrimaryNgPrefix,
		Count: 1,
		Requests: map[string]resource.Request{
			"memory": memNodeReq,
		},
	}

	ngSecondariesRequest = resource.GroupRequest{
		Id:    rlv1.SecondaryNgName,
		Count: 1,
		Requests: map[string]resource.Request{
			"memory": memNodeReq,
		},
	}
	internalSubresourceRanges = resource.SubresourceRanges{
		resource.NvmfDiskSubresource: {{Start: 0, End: 9}, {Start: 100, End: 200}},
	}
	k8sSubresourceRanges = map[string][]rlv1.Range{
		resource.NvmfDiskSubresource: {{Start: 0, End: 9}, {Start: 100, End: 200}},
	}

	actGroupRequest = resource.GroupRequest{
		Id:    string(wsapisv1.WSReplicaTypeActivation),
		Count: 1,
		Requests: map[string]resource.Request{
			string(wsapisv1.WSReplicaTypeActivation): resource.Request{
				Id:          string(wsapisv1.WSReplicaTypeActivation),
				Count:       1,
				Annotations: groupReqAnnot,
				Filter:      resource.NewNodeFilter("activation", 4, 4),
			},
		},
	}

	wgtCmdGroupRequest = resource.GroupRequest{
		Id:    rlv1.WgtCmdGroupPrefix,
		Count: 1,
		Requests: map[string]resource.Request{
			string(wsapisv1.WSReplicaTypeWeight): resource.Request{
				Id:          string(wsapisv1.WSReplicaTypeWeight),
				Count:       3,
				Annotations: groupReqAnnot,
				Filter:      resource.NewNodeFilter("activation", 4, 4),
			},
			string(wsapisv1.WSReplicaTypeCommand): resource.Request{
				Id:          string(wsapisv1.WSReplicaTypeCommand),
				Count:       1,
				Annotations: groupReqAnnot,
				Filter:      resource.NewNodeFilter("activation", 4, 4),
			},
		},
	}

	actResourceGrant = []rlv1.ResourceGrant{
		{
			Name: "0",
			Resources: []rlv1.Res{
				{Type: rlv1.NodeResourceType, Name: "ax-0"},
			},
			Annotations: groupReqAnnot,
		},
		{
			Name: "1",
			Resources: []rlv1.Res{
				{Type: rlv1.NodeResourceType, Name: "ax-0"},
			},
			Annotations: groupReqAnnot,
		},
	}

	wgtCmdResourceGrant = []rlv1.ResourceGrant{
		{
			Name: fmt.Sprintf("%s-0", string(wsapisv1.WSReplicaTypeWeight)),
			Resources: []rlv1.Res{
				{Type: rlv1.NodeResourceType, Name: "ax-0"},
				{Type: rlv1.NodeResourceType, Name: "ax-0"},
				{Type: rlv1.NodeResourceType, Name: "ax-0"},
			},
			Annotations: groupReqAnnot,
		},
		{
			Name: string(wsapisv1.WSReplicaTypeCommand),
			Resources: []rlv1.Res{
				{Type: rlv1.NodeResourceType, Name: "ax-0"},
			},
			Annotations: groupReqAnnot,
		},
	}

	testLockSystemWithNodeGroup = lock{
		Name: "with-system-nodegroup",
		ResourceRequests: map[string]resource.Request{
			rlv1.SystemsRequestName:     sysReq,
			rlv1.CoordinatorRequestName: coordReq,
		},
		GroupRequests: map[string]resource.GroupRequest{
			ngPrimaryRequest.Id:     ngPrimaryRequest,
			ngSecondariesRequest.Id: ngSecondariesRequest,
		},
		ResourceGrants: map[string]resource.Grant{
			sysReq.Id: {Request: sysReq, ResourceIds: []string{"system/system-1", "system/system-2"}},
			coordReq.Id: {
				Request:           coordReq,
				ResourceIds:       []string{"node/node-1"},
				SubresourceRanges: internalSubresourceRanges},
		},
		GroupGrants: map[string][]resource.GroupGrant{
			ngPrimaryRequest.Id: {{
				Request:     ngPrimaryRequest,
				GroupVal:    "nodegroup1",
				Annotations: groupReqAnnot,
				Grants: map[string]resource.Grant{
					"memory": {
						Request:     memNodeReq,
						ResourceIds: []string{"node/node-2", "node/node-2"}, // duplicate nodes are allowed
					},
				},
			}},
			ngSecondariesRequest.Id: {{
				Request:     ngSecondariesRequest,
				GroupVal:    "nodegroup2",
				Annotations: groupReqAnnot,
				Grants: map[string]resource.Grant{
					"memory": {
						Request:     memNodeReq,
						ResourceIds: []string{"node/node-3", "node/node-4"},
					},
				},
			}},
		},
		Obj: &rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name: "with-system-nodegroup",
			},
			Spec: rlv1.ResourceLockSpec{
				ResourceRequests: []rlv1.ResourceRequest{
					{
						Name:         rlv1.SystemsRequestName,
						Type:         rlv1.SystemResourceType,
						Count:        2,
						Properties:   map[string][]string{},
						Subresources: map[string]int{resource.SystemSubresource: 1},
						Annotations:  ungroupReqAnnot,
					},
					{
						Name:         rlv1.CoordinatorRequestName,
						Type:         rlv1.NodeResourceType,
						Count:        1,
						Properties:   map[string][]string{resource.RoleMgmtPropKey: {""}},
						Subresources: map[string]int{resource.PodSubresource: 1, resource.CPUSubresource: 4, resource.MemSubresource: 4},
						Annotations:  ungroupReqAnnot,
					},
				},
				GroupResourceRequests: []rlv1.GroupResourceRequest{
					{
						Name:  rlv1.PrimaryNgPrefix,
						Count: 1,
						ResourceRequests: []rlv1.ResourceRequest{
							{
								Name:         "memory",
								Type:         rlv1.NodeResourceType,
								Count:        2,
								Properties:   map[string][]string{resource.RoleMemoryPropKey: {""}},
								Subresources: map[string]int{resource.PodSubresource: 1, resource.CPUSubresource: 4, resource.MemSubresource: 4},
								Annotations:  groupReqAnnot,
							},
						},
					},
					{
						Name:  rlv1.SecondaryNgName,
						Count: 1,
						ResourceRequests: []rlv1.ResourceRequest{
							{
								Name:         "memory",
								Type:         rlv1.NodeResourceType,
								Count:        1,
								Properties:   map[string][]string{resource.RoleMemoryPropKey: {""}},
								Subresources: map[string]int{resource.PodSubresource: 1, resource.CPUSubresource: 4, resource.MemSubresource: 4},
								Annotations:  groupReqAnnot,
							},
						},
					},
				},
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
				ResourceGrants: []rlv1.ResourceGrant{
					{
						Name: rlv1.SystemsRequestName,
						Resources: []rlv1.Res{
							{Type: rlv1.SystemResourceType, Name: "system-1"},
							{Type: rlv1.SystemResourceType, Name: "system-2"},
						},
						Annotations: ungroupReqAnnot,
					},
					{
						Name: rlv1.CoordinatorRequestName,
						Resources: []rlv1.Res{{
							Type:              rlv1.NodeResourceType,
							Name:              "node-1",
							SubresourceRanges: k8sSubresourceRanges},
						},
						Annotations: ungroupReqAnnot,
					},
				},
				GroupResourceGrants: []rlv1.GroupResourceGrant{
					{
						Name:          rlv1.PrimaryNgPrefix,
						GroupingValue: "nodegroup1",
						Annotations:   groupReqAnnot,
						ResourceGrants: []rlv1.ResourceGrant{
							{
								Name: "memory",
								Resources: []rlv1.Res{
									{Type: rlv1.NodeResourceType, Name: "node-2"},
									{Type: rlv1.NodeResourceType, Name: "node-2"},
								},
							},
						},
					},
					{
						Name:          rlv1.SecondaryNgName,
						GroupingValue: "nodegroup2",
						Annotations:   groupReqAnnot,
						ResourceGrants: []rlv1.ResourceGrant{
							{
								Name: "memory",
								Resources: []rlv1.Res{
									{Type: rlv1.NodeResourceType, Name: "node-3"},
									{Type: rlv1.NodeResourceType, Name: "node-4"},
								},
							},
						},
					},
				},
			},
		},
	}

	testLockPrimaryGroupNoSystems = lock{
		Name: "non-standard-no-groups",
		ResourceRequests: map[string]resource.Request{
			rlv1.CoordinatorRequestName: {
				Id:          rlv1.CoordinatorRequestName,
				Count:       1,
				Annotations: ungroupReqAnnot,
				Filter:      resource.NewNodeFilter("management", 4, 4)},
		},
		ResourceGrants: map[string]resource.Grant{
			rlv1.CoordinatorRequestName: {
				Request:     coordReq,
				Annotations: ungroupReqAnnot,
				ResourceIds: []string{"node/node-1"}},
		},
		Obj: &rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name: "non-standard-no-groups",
			},
			Spec: rlv1.ResourceLockSpec{
				ResourceRequests: []rlv1.ResourceRequest{
					{
						Name:         rlv1.CoordinatorRequestName,
						Type:         rlv1.NodeResourceType,
						Count:        1,
						Properties:   map[string][]string{resource.RoleMgmtPropKey: {""}},
						Subresources: map[string]int{resource.PodSubresource: 1, resource.CPUSubresource: 4, resource.MemSubresource: 4},
						Annotations:  ungroupReqAnnot,
					},
				},
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
				ResourceGrants: []rlv1.ResourceGrant{
					{
						Name:        rlv1.CoordinatorRequestName,
						Annotations: ungroupReqAnnot,
						Resources: []rlv1.Res{
							{Type: rlv1.NodeResourceType, Name: "node-1"},
						},
					},
				},
			},
		},
	}

	testLockWgtCmdGroup = lock{
		Name: "with-wgt-cmd-group",
		ResourceRequests: map[string]resource.Request{
			"System": sysReq,
		},
		GroupRequests: map[string]resource.GroupRequest{
			actGroupRequest.Id:    actGroupRequest,
			wgtCmdGroupRequest.Id: wgtCmdGroupRequest,
		},
		ResourceGrants: map[string]resource.Grant{
			sysReq.Id: {
				Request:     sysReq,
				ResourceIds: []string{"system/system-1"},
				Annotations: ungroupReqAnnot,
			},
		},
		ActGroupRequests: []resource.GroupRequest{
			actGroupRequest,
		},
		ActGroupGrants: []resource.GroupGrant{
			{
				Request: actGroupRequest,
				Grants: map[string]resource.Grant{
					"0": {},
					"1": {},
				},
			},
		},
		WgtCmdGroupRequests: []resource.GroupRequest{
			wgtCmdGroupRequest,
		},
		WgtCmdGroupGrants: []resource.GroupGrant{
			{
				Request: wgtCmdGroupRequest,
				Grants: map[string]resource.Grant{
					"Weight-0": {},
					"Command":  {},
				},
			},
		},
		Obj: &rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name: "with-wgt-cmd-group",
			},
			Spec: rlv1.ResourceLockSpec{
				ResourceRequests: []rlv1.ResourceRequest{
					{
						Name:         rlv1.SystemsRequestName,
						Type:         rlv1.SystemResourceType,
						Count:        1,
						Properties:   map[string][]string{},
						Subresources: map[string]int{resource.SystemSubresource: 1},
						Annotations:  ungroupReqAnnot,
					},
				},
				GroupResourceRequests: []rlv1.GroupResourceRequest{
					{
						Name:  string(wsapisv1.WSReplicaTypeActivation),
						Count: 1,
						ResourceRequests: []rlv1.ResourceRequest{
							{
								Name:         string(wsapisv1.WSReplicaTypeActivation),
								Type:         rlv1.NodeResourceType,
								Count:        1,
								Properties:   map[string][]string{resource.RoleAxPropKey: {""}},
								Subresources: map[string]int{resource.PodSubresource: 1, resource.CPUSubresource: 4, resource.MemSubresource: 4},
								Annotations:  groupReqAnnot,
							},
						},
					},
					{
						Name:  rlv1.WgtCmdGroupPrefix,
						Count: 1,
						ResourceRequests: []rlv1.ResourceRequest{
							{
								Name:         string(wsapisv1.WSReplicaTypeWeight),
								Type:         rlv1.NodeResourceType,
								Count:        3,
								Properties:   map[string][]string{resource.RoleAxPropKey: {""}},
								Subresources: map[string]int{resource.PodSubresource: 1, resource.CPUSubresource: 4, resource.MemSubresource: 4},
								Annotations:  groupReqAnnot,
							},
							{
								Name:         string(wsapisv1.WSReplicaTypeCommand),
								Type:         rlv1.NodeResourceType,
								Count:        1,
								Properties:   map[string][]string{resource.RoleAxPropKey: {""}},
								Subresources: map[string]int{resource.PodSubresource: 1, resource.CPUSubresource: 4, resource.MemSubresource: 4},
								Annotations:  groupReqAnnot,
							},
						},
					},
				},
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
				ResourceGrants: []rlv1.ResourceGrant{
					{
						Name: "System",
						Resources: []rlv1.Res{
							{Type: rlv1.SystemResourceType, Name: "system-1"},
						},
						Annotations: ungroupReqAnnot,
					},
				},
				GroupResourceGrants: []rlv1.GroupResourceGrant{
					{
						Name:           fmt.Sprintf("%s-group-0", string(wsapisv1.WSReplicaTypeActivation)),
						ResourceGrants: actResourceGrant,
					},
					{
						Name:           fmt.Sprintf("%s-group", rlv1.WgtCmdGroupPrefix),
						ResourceGrants: wgtCmdResourceGrant,
					},
				},
			},
		},
	}
)

func TestLock_toK8s(t *testing.T) {
	tests := []struct {
		lock            lock
		expectedNG      []rlv1.GroupResourceGrant
		expectedSystems []rlv1.SystemGrant
	}{
		{
			lock: testLockSystemWithNodeGroup,
			expectedNG: []rlv1.GroupResourceGrant{
				{
					Name:          rlv1.PrimaryNgPrefix,
					GroupingValue: "nodegroup1",
				},
				{
					Name:          rlv1.SecondaryNgName,
					GroupingValue: "nodegroup2",
				},
			},
			expectedSystems: []rlv1.SystemGrant{{Name: "system-1"}, {Name: "system-2"}},
		},
		{
			lock:            testLockPrimaryGroupNoSystems,
			expectedNG:      nil,
			expectedSystems: nil,
		},
	}
	for _, testcase := range tests {
		t.Run(testcase.lock.Name, func(t *testing.T) {
			rl := testcase.lock.toK8s()
			t.Log(rl)
			assert.Equal(t, rlv1.LockGranted, rl.Status.State)
			assert.NotNil(t, rl.Status.ResourceGrants)
			for _, rg := range rl.Status.ResourceGrants {
				assert.Equal(t, ungroupReqAnnot, rg.Annotations)
				if rg.Name == rlv1.CoordinatorRequestName {
					res := rg.Resources[0]
					expectedSubresourceRanges := k8sSubresourceRanges
					if len(testcase.lock.ResourceGrants[rg.Name].SubresourceRanges) == 0 {
						expectedSubresourceRanges = map[string][]rlv1.Range{}
					}
					assert.Equal(t, expectedSubresourceRanges, res.SubresourceRanges)
				}
			}
			foundNgCount := 0
			for _, expectNg := range testcase.expectedNG {
				for _, gg := range rl.Status.GroupResourceGrants {
					if gg.Name == expectNg.Name {
						assert.Equal(t, expectNg.GroupingValue, gg.GroupingValue)
						foundNgCount++
					}
					assert.Equal(t, groupReqAnnot, gg.Annotations)
				}
			}
			assert.Equal(t, len(testcase.expectedNG), foundNgCount)
			assert.Equal(t, testcase.expectedSystems, rl.Status.SystemGrants)
		})
	}
}

func TestLockFromK8s_OK(t *testing.T) {
	testcases := []struct {
		lock lock
	}{
		{
			lock: testLockSystemWithNodeGroup,
		},
		{
			lock: testLockPrimaryGroupNoSystems,
		},
		{
			lock: testLockWgtCmdGroup,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.lock.Name, func(t *testing.T) {
			l, err := NewLockFromK8s(testcase.lock.Obj)
			assert.NoError(t, err)
			assert.ElementsMatch(t, common.Keys(testcase.lock.ResourceRequests), common.Keys(l.ResourceRequests))
			assert.ElementsMatch(t, common.Keys(testcase.lock.GroupRequests), common.Keys(l.GroupRequests))
			assert.ElementsMatch(t, common.Keys(testcase.lock.ResourceGrants), common.Keys(l.ResourceGrants))
			assert.ElementsMatch(t, common.Keys(testcase.lock.GroupGrants), common.Keys(l.GroupGrants))

			for _, rr := range l.ResourceRequests {
				assert.Equal(t, ungroupReqAnnot, rr.Annotations)
			}
			for _, gr := range l.GroupRequests {
				for _, req := range gr.Requests {
					assert.Equal(t, groupReqAnnot, req.Annotations)
				}
			}
			for _, rg := range l.ResourceGrants {
				assert.Equal(t, ungroupReqAnnot, rg.Annotations)
				expectedSubresourceRanges := internalSubresourceRanges
				for _, k8rg := range testcase.lock.Obj.Status.ResourceGrants {
					if k8rg.Name == rlv1.CoordinatorRequestName {
						for _, res := range k8rg.Resources {
							if len(res.SubresourceRanges) == 0 {
								expectedSubresourceRanges = resource.SubresourceRanges{}
							}
						}
					}
				}
				if rg.Request.Id == rlv1.CoordinatorRequestName {
					assert.Equal(t, expectedSubresourceRanges, rg.SubresourceRanges)
				}
			}
			for _, ggs := range l.GroupGrants {
				for _, gg := range ggs {
					assert.Equal(t, groupReqAnnot, gg.Annotations)
				}
			}

			assert.Equal(t, len(testcase.lock.ActGroupGrants), len(l.ActGroupGrants))
			if len(testcase.lock.ActGroupGrants) > 1 {
				assert.Equal(t, len(testcase.lock.ActGroupGrants[0].Grants), len(l.ActGroupGrants[0].Grants))
			}
			assert.Equal(t, len(testcase.lock.WgtCmdGroupGrants), len(l.WgtCmdGroupGrants))
			if len(testcase.lock.WgtCmdGroupGrants) > 1 {
				assert.Equal(t, len(testcase.lock.WgtCmdGroupGrants[0].Grants), len(l.WgtCmdGroupGrants[0].Grants))

			}
		})
	}
}

func TestUpdateLocksInQueue(t *testing.T) {
	// Mock locks
	lock1 := lock{Name: "lock1", CreateTime: 1, Priority: 2}
	lock2 := lock{Name: "lock2", CreateTime: 2, Priority: 2}
	lock3 := lock{Name: "lock3", CreateTime: 3, Priority: 1}
	locks := []*lock{
		&lock1, // Higher priority
		&lock2, // Same priority as lock1, but later create time
		&lock3, // Highest priority, but earlier create time
	}

	q := &queue{name: "testQueue", data: locks}

	// Resort queue with priority scheduling enabled
	for _, l := range append([]*lock(nil), locks...) {
		q.Requeue(l)
	}
	assert.Equal(t, []*lock{&lock3, &lock1, &lock2}, q.data)
}

func TestV2RequestHydration(t *testing.T) {
	numCsx := 4
	numActsPerCsx := 15
	numKvssPerCsx := 15

	chfSr := resource.NewCpuMemPodSubresources(1, 100)
	chfCount := numCsx
	chfGroupRequest := resource.GroupRequest{
		Id:    rlv1.ChiefRequestName,
		Count: 1,
		Requests: map[string]resource.Request{
			"foo": {
				Id:    "bar",
				Count: chfCount,
				Filter: resource.NewFilter(
					rlv1.NodeResourceType,
					common.RoleActivation.AsMatcherProperty(),
					chfSr,
				),
			},
		},
	}

	actSr := resource.NewCpuMemPodSubresources(10, 500)
	actCount := numCsx * numActsPerCsx
	actGroupRequest := resource.GroupRequest{
		Id:    rlv1.ActivationRequestName,
		Count: 1,
		Requests: map[string]resource.Request{
			"foo": {
				Id:    "bar",
				Count: actCount,
				Filter: resource.NewFilter(
					rlv1.NodeResourceType,
					common.RoleActivation.AsMatcherProperty(),
					actSr,
				),
			},
		},
	}
	makeActTargets := func(perStampSystems [][]string) []common.WioTarget {
		var actToCsPortMap []common.WioTarget
		sysIdx := 0
		podIdx := 0
		for i, systems := range perStampSystems {
			for j := 0; j < len(systems); j++ {
				for k := 0; k < numActsPerCsx; k++ {
					actToCsPortMap = append(actToCsPortMap, common.WioTarget{
						AllSystemIndices:     []int{sysIdx},
						StampIdx:             i,
						InStampSystemIndices: []int{sysIdx},
						TargetPorts:          []int{podIdx % 12},
					})
					podIdx += 1
				}
				sysIdx += 1
			}
		}
		return actToCsPortMap
	}

	kvssSr := resource.NewCpuMemPodSubresources(10, 500)
	kvssCount := numCsx * numKvssPerCsx
	kvssGroupRequest := resource.GroupRequest{
		Id:    rlv1.KVStorageServerRequestName,
		Count: 1,
		Requests: map[string]resource.Request{
			"foo": {
				Id:    "bar",
				Count: kvssCount,
				Filter: resource.NewFilter(
					rlv1.NodeResourceType,
					common.RoleActivation.AsMatcherProperty(),
					kvssSr,
				),
			},
		},
	}
	makeKvssTargets := func(perStampSystems [][]string) []common.WioTarget {
		var kvssToCsPortMap []common.WioTarget
		sysIdx := 0
		podIdx := 0
		for i, systems := range perStampSystems {
			for j := 0; j < len(systems); j++ {
				for k := 0; k < numActsPerCsx; k++ {
					kvssToCsPortMap = append(kvssToCsPortMap, common.WioTarget{
						AllSystemIndices:     []int{sysIdx},
						StampIdx:             i,
						InStampSystemIndices: []int{sysIdx},
						TargetPorts:          []int{podIdx % 12, (podIdx + 3) % 12},
					})
					podIdx += 1
				}
				sysIdx += 1
			}
		}
		return kvssToCsPortMap
	}

	for _, tc := range []struct {
		name            string
		perStampSystems [][]string

		// how many requests are expected in each stamp
		expectActGroupRequests []int
	}{
		{
			name: "1 stamp of 4 CSX",
			perStampSystems: [][]string{
				{"sys-0", "sys-1", "sys-2", "sys-3"},
			},

			expectActGroupRequests: []int{60},
		},
		{
			name: "1 system in a stamp and 3 systems in another stamp",
			perStampSystems: [][]string{
				{"sys-0"},
				{"sys-1", "sys-2", "sys-3"},
			},

			expectActGroupRequests: []int{15, 45},
		},
		{
			name: "4 stamps with one system each",
			perStampSystems: [][]string{
				{"sys-0"},
				{"sys-1"},
				{"sys-2"},
				{"sys-3"},
			},

			expectActGroupRequests: []int{15, 15, 15, 15},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			systemIdxMap := map[string]int{}
			var systemNames []string
			for _, ss := range tc.perStampSystems {
				for _, s := range ss {
					systemIdxMap[s] = len(systemIdxMap)
					systemNames = append(systemNames, s)
				}
			}

			hydratedChfRequests := hydrateChfRequests(tc.perStampSystems, chfGroupRequest)
			totalChfPodsRequested := 0
			for i, groupRequest := range hydratedChfRequests {
				assert.Equal(t, 1, groupRequest.Count)
				chfPodsRequested := 0
				assert.Equal(t, 1, len(groupRequest.Requests))
				for _, subReq := range groupRequest.Requests {
					// assert subresource plumbing for chief
					assert.Equal(t, chfSr, subReq.Filter.Subresources())
					chfPodsRequested += subReq.Count
				}
				assert.Equal(t, len(tc.perStampSystems[i]), chfPodsRequested)
				totalChfPodsRequested += groupRequest.Count * chfPodsRequested
			}
			assert.Equal(t, numCsx, totalChfPodsRequested)

			actTargets := makeActTargets(tc.perStampSystems)
			hydratedActRequests := hydrateWioRequests(tc.perStampSystems, actTargets, actGroupRequest, rlv1.ActivationRequestName, nil)
			validateHydratedRequest(t, hydratedActRequests, actSr, actTargets, tc.perStampSystems, systemNames, tc.expectActGroupRequests, numActsPerCsx, numCsx)

			kvssTargets := makeKvssTargets(tc.perStampSystems)
			hydratedKvssRequests := hydrateWioRequests(tc.perStampSystems, kvssTargets, kvssGroupRequest, rlv1.KVStorageServerRequestName, nil)
			validateHydratedRequest(t, hydratedKvssRequests, kvssSr, kvssTargets, tc.perStampSystems, systemNames, tc.expectActGroupRequests, numActsPerCsx, numCsx)
		})
	}
}

func validateHydratedRequest(
	t *testing.T,
	hydratedGroupRequests []resource.GroupRequest,
	sr resource.Subresources,
	wioTargets []common.WioTarget,
	perStampSystems [][]string,
	systemNames []string,
	expectedGroupRequests []int,
	numPodsPerCsx int,
	numCsx int) {

	totalPodsRequested := 0
	for i, groupRequest := range hydratedGroupRequests {
		assert.Equal(t, 1, groupRequest.Count)
		assert.Equal(t, expectedGroupRequests[i], len(groupRequest.Requests))
		actPodsRequested := 0
		for _, subReq := range groupRequest.Requests {
			// assert subresource plumbing for activation
			for k, v := range sr {
				assert.Equal(t, v, subReq.Filter.Subresources()[k])
			}
			for k, v := range subReq.Filter.Subresources() {
				if expectedVal, ok := sr[k]; ok {
					assert.Equal(t, expectedVal, v)
				}
			}

			assert.Equal(t, 1, subReq.Count)
			actPodsRequested += subReq.Count
		}
		assert.Equal(t, len(perStampSystems[i])*numPodsPerCsx, actPodsRequested)
		totalPodsRequested += groupRequest.Count * actPodsRequested

		sortedKeys := common.SortedKeys(groupRequest.Requests)
		for _, k := range sortedKeys {
			req := groupRequest.Requests[k]

			// assert cs port mapping
			podIdx, _ := strconv.Atoi(req.Id)
			sysIdx, _ := strconv.Atoi(req.Annotations[rlv1.SystemIdAnnotKey])
			csPorts := strings.Split(req.Annotations[rlv1.TargetCsPortAnnotKey], ",")

			systemName := systemNames[sysIdx]
			expectedAffinities := make(map[string]string)

			for i, csPort := range csPorts {
				port, _ := strconv.Atoi(csPort)
				assert.Equal(t, port, wioTargets[podIdx].TargetPorts[i])

				// assert affinity
				expectedAffinities[fmt.Sprintf("%s/%s-%d", resource.PortAffinityPropKeyPrefix, systemName, port)] = ""
			}
			assert.Equal(t, expectedAffinities, req.Filter.GetAffinities())
		}
	}
	assert.Equal(t, numCsx*numPodsPerCsx, totalPodsRequested)
	assert.Equal(t, len(perStampSystems), len(hydratedGroupRequests))
	assert.Equal(t, len(perStampSystems), len(expectedGroupRequests))
}

func TestV2RequestHydrationWithLinearMemoryOverride(t *testing.T) {
	numCsx := 2
	numKvssPerCsx := 5
	perStampSystems := [][]string{{"sys-0", "sys-1"}}

	// Create a basic KVSS group request with standard memory
	kvssSr := resource.NewCpuMemPodSubresources(10, 500)
	kvssCount := numCsx * numKvssPerCsx
	kvssGroupRequest := resource.GroupRequest{
		Id:    rlv1.KVStorageServerRequestName,
		Count: 1,
		Requests: map[string]resource.Request{
			"foo": {
				Id:    "bar",
				Count: kvssCount,
				Filter: resource.NewFilter(
					rlv1.NodeResourceType,
					common.RoleActivation.AsMatcherProperty(),
					kvssSr,
				),
			},
		},
	}

	// Create KVSS targets
	kvssTargets := make([]common.WioTarget, 0, numCsx*numKvssPerCsx)
	for sysIdx := 0; sysIdx < numCsx; sysIdx++ {
		for podIdx := 0; podIdx < numKvssPerCsx; podIdx++ {
			kvssTargets = append(kvssTargets, common.WioTarget{
				AllSystemIndices:     []int{sysIdx},
				StampIdx:             0,
				InStampSystemIndices: []int{sysIdx},
				TargetPorts:          []int{podIdx % 12},
			})
		}
	}

	// Set up linear memory range overrides (only using lower bounds)
	lmrConfigs := map[commonv1.ReplicaType]common.LinearMemoryConfig{
		wsapisv1.WSReplicaTypeKVStorageServer: {
			Global: common.LinearMemoryRange{
				Intercept:   104857600, // 100 MiB
				Coefficient: 10485760,  // 10 MiB
				MinX:        1,
				MaxX:        5, // Not used since we're only setting lower bounds
			},
			ReplicaCoefs: []*int64{
				common.Pointer[int64](20971520), // 20 MiB for pod 0
				nil,                             // nil for pod 1, will use global coefficient
				common.Pointer[int64](31457280), // 30 MiB for pod 2
				nil,                             // nil for pod 3, will use global coefficient
				common.Pointer[int64](41943040), // 40 MiB for pod 4
				nil,                             // nil for pod 5, will use global coefficient
				common.Pointer[int64](62914560), // 60 MiB for pod 6
				nil,                             // nil for pod 7, will use global coefficient
				common.Pointer[int64](83886080), // 80 MiB for pod 8 (180 total)
				nil,                             // nil for pod 9, will use global coefficient
			},
		},
	}

	// Hydrate the request with our overrides
	hydratedKvssRequests := hydrateWioRequests(
		perStampSystems,
		kvssTargets,
		kvssGroupRequest,
		rlv1.KVStorageServerRequestName,
		common.GetLinearMemoryConfig(lmrConfigs, wsapisv1.WSReplicaTypeKVStorageServer),
	)

	// We expect exactly one group request for our one stamp
	assert.Equal(t, 1, len(hydratedKvssRequests))

	// First group request should have all 10 pods
	group := hydratedKvssRequests[0]
	assert.Equal(t, numCsx*numKvssPerCsx, len(group.Requests))

	// Check the memory values for each pod (only lower bound)
	expectedMemory := []struct {
		podId      int
		memRequest int // in MiB
	}{
		{0, 120}, // Pod 0: 100 + 20*1
		{1, 110}, // Pod 1 (global)
		{2, 130}, // Pod 2: 100 + 30*1
		{3, 110}, // Pod 3 (global)
		{4, 140}, // Pod 4: 100 + 40*1
		{5, 110}, // Pod 5 (global)
		{6, 160}, // Pod 6: 100 + 60*1
		{7, 110}, // Pod 7 (global)
		{8, 180}, // Pod 8: 100 + 80*1
		{9, 110}, // Pod 9 (global)
	}

	for _, em := range expectedMemory {
		podIdStr := strconv.Itoa(em.podId)
		req, found := group.Requests[podIdStr]
		assert.True(t, found, "Pod %d not found in requests", em.podId)

		// Check base memory request
		memRequest, found := req.Filter.Subresources()[resource.MemSubresource]
		assert.True(t, found, "Memory request not found for pod %d", em.podId)
		assert.Equal(t, em.memRequest, memRequest, "Wrong memory request for pod %d", em.podId)
	}
}

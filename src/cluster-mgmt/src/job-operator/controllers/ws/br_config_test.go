//go:build default

package ws

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	ctrl "sigs.k8s.io/controller-runtime"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	"cerebras.com/job-operator/common"
	wscommon "cerebras.com/job-operator/common"
)

func TestBrConfigGeneration(t *testing.T) {
	cfg := wscommon.NewClusterConfigBuilder("test").
		WithCoordNode().
		WithBR(brCount).
		WithSysMemxWorkerGroups(systemCount, 5, 1).
		BuildClusterConfig()

	brLogicalTree := wscommon.BrLogicalTree{
		{
			SetId: 0,
			Layer: 1,
			IsBr:  true,
			Egresses: []*wscommon.BrInternalEgress{
				{
					SystemName: "system-0",
				},
				{
					SystemName: "system-1",
				},
				{
					SystemName: "system-2",
				},
				{
					SystemName: "system-3",
				},
			},
		},
		{
			SetId: 1,
			Layer: 1,
			IsBr:  true,
			Egresses: []*wscommon.BrInternalEgress{
				{
					SystemName: "system-4",
				},
				{
					SystemName: "system-5",
				},
				{
					SystemName: "system-6",
				},
				{
					SystemName: "system-7",
				},
			},
		},
		{
			SetId: 2,
			Layer: 2,
			IsBr:  true,
			Egresses: []*wscommon.BrInternalEgress{
				{
					BrSetId: 0,
					IsBr:    true,
				},
				{
					BrSetId: 1,
					IsBr:    true,
				},
			},
		},
	}

	oneFourModeAssign := map[string]int{
		"cs-port/nic-0": 1,
		"cs-port/nic-1": 1,
		"cs-port/nic-2": 1,
		"cs-port/nic-3": 1,
		"cs-port/nic-4": 1,
	}
	oneTwoModeAssignFirst := map[string]int{
		"cs-port/nic-0": 1,
		"cs-port/nic-1": 1,
		"cs-port/nic-2": 1,
	}
	oneTwoModeAssignSecond := map[string]int{
		"cs-port/nic-3": 1,
		"cs-port/nic-4": 1,
		"cs-port/nic-5": 1,
	}
	j := &WSJobController{Cfg: cfg, Log: ctrl.Log}
	for _, tc := range []struct {
		name            string
		skipGroup       int
		res             jobResources
		brLogicalTree   wscommon.BrLogicalTree
		disableBrMultus bool
	}{
		{
			name:      "test full br config hostnetwork",
			skipGroup: -1,
			res: jobResources{
				systems: wscommon.SortedKeys(j.Cfg.SystemMap),
				replicaToNodeMap: map[string]map[int]string{
					rlv1.BrRequestName: {
						0: "br-0", 1: "br-1", 2: "br-2", 3: "br-3", 4: "br-4", 5: "br-5", 6: "br-6", 7: "br-7",
						8: "br-8", 9: "br-9", 10: "br-10", 11: "br-11",
						12: "br-12", 13: "br-13", 14: "br-14", 15: "br-15", 16: "br-16", 17: "br-17",
						18: "br-18", 19: "br-19", 20: "br-20", 21: "br-21", 22: "br-22", 23: "br-23",
						24: "br-24", 25: "br-24", 26: "br-25", 27: "br-25", 28: "br-26", 29: "br-26",
						30: "br-27", 31: "br-27", 32: "br-28", 33: "br-28", 34: "br-29", 35: "br-29",
					},
				},
				replicaToResourceMap: map[string]map[int]map[string]int{
					rlv1.BrRequestName: {},
				},
			},
			brLogicalTree:   brLogicalTree,
			disableBrMultus: true,
		},
		{
			name:      "test full br config with multus",
			skipGroup: -1,
			res: jobResources{
				systems: wscommon.SortedKeys(j.Cfg.SystemMap),
				replicaToNodeMap: map[string]map[int]string{
					rlv1.BrRequestName: {
						0: "br-0", 1: "br-1", 2: "br-2", 3: "br-3", 4: "br-4", 5: "br-5", 6: "br-6", 7: "br-7",
						8: "br-8", 9: "br-9", 10: "br-10", 11: "br-11",
						12: "br-12", 13: "br-13", 14: "br-14", 15: "br-15", 16: "br-16", 17: "br-17",
						18: "br-18", 19: "br-19", 20: "br-20", 21: "br-21", 22: "br-22", 23: "br-23",
						24: "br-24", 25: "br-24", 26: "br-25", 27: "br-25", 28: "br-26", 29: "br-26",
						30: "br-27", 31: "br-27", 32: "br-28", 33: "br-28", 34: "br-29", 35: "br-29",
					},
				},
				replicaToResourceMap: map[string]map[int]map[string]int{
					rlv1.BrRequestName: {},
				},
			},
			brLogicalTree:   brLogicalTree,
			disableBrMultus: false,
		},
		{
			name:      "test partial br config",
			skipGroup: 0,
			res: jobResources{
				systems: wscommon.SortedKeys(j.Cfg.SystemMap),
				replicaToNodeMap: map[string]map[int]string{
					// skip group 0 case
					rlv1.BrRequestName: {
						1: "br-1", 2: "br-2", 4: "br-4", 5: "br-5", 7: "br-7",
						8: "br-8", 10: "br-10", 11: "br-11",
						13: "br-13", 14: "br-14", 16: "br-16", 17: "br-17",
						19: "br-19", 20: "br-20", 22: "br-22", 23: "br-23",
						25: "br-24", 26: "br-25", 28: "br-26", 29: "br-26",
						31: "br-27", 32: "br-28", 34: "br-29", 35: "br-29",
					},
				},
				replicaToResourceMap: map[string]map[int]map[string]int{
					rlv1.BrRequestName: {},
				},
			},
			brLogicalTree:   brLogicalTree,
			disableBrMultus: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wsapisv1.DisableMultusForBR = tc.disableBrMultus

			defer func() {
				wsapisv1.DisableMultusForBR = true
			}()

			res := tc.res
			brSetsTree := tc.brLogicalTree
			res.replicaToAnnoMap = wscommon.EnsureMap(res.replicaToAnnoMap)
			res.replicaToAnnoMap[rlv1.BrRequestName] =
				wscommon.EnsureMap(res.replicaToAnnoMap[rlv1.BrRequestName])
			treeVal, _ := json.Marshal(brSetsTree)
			res.replicaToAnnoMap[rlv1.BrRequestName][rlv1.BrLogicalTreeKey] = string(treeVal)
			// Parameterize port count based on test cfg
			portCount := common.CSVersionSpec.NumPorts
			for i := 0; i < len(brSetsTree)*portCount; i++ {
				group := common.CSVersionSpec.Group(i % portCount)
				if group == tc.skipGroup {
					continue
				}
				if i < 24 {
					res.replicaToResourceMap[rlv1.BrRequestName][i] = oneFourModeAssign
				} else if i%2 == 0 {
					res.replicaToResourceMap[rlv1.BrRequestName][i] = oneTwoModeAssignFirst
				} else {
					res.replicaToResourceMap[rlv1.BrRequestName][i] = oneTwoModeAssignSecond
				}
			}

			brCfg := j.buildBrConfig(res, "", false, 0)
			t.Logf("%+v", brCfg.BRNodes)
			t.Logf("%+v", brCfg.BRGroups)
			t.Logf("%+v", brCfg.CSNodes)

			require.Equal(t, 36, len(brCfg.BRNodes))
			require.Equal(t, 3, len(brCfg.BRGroups))
			require.Equal(t, systemCount, len(brCfg.CSNodes))

			for _, no := range brCfg.BRNodes {
				brId := no.NodeId
				port := brId % 12
				brSet := brSetsTree[brId/12]
				group := common.CSVersionSpec.Group(port)
				require.NotEqual(t, -1, no.NodeId)
				if group == tc.skipGroup {
					require.Equal(t, wsapisv1.DummyIpPort, no.ControlAddr)
					require.Equal(t, wsapisv1.DummyIp, no.IngressAddr)
				} else {
					insId := tc.res.getAssignedInstanceId(wsapisv1.WSReplicaTypeBroadcastReduce, brId)
					insIdInt, _ := strconv.Atoi(insId)
					ingPort := wsapisv1.DefaultPort
					if !tc.disableBrMultus {
						ingPort += insIdInt
					}
					require.Contains(t, no.ControlAddr, strconv.Itoa(ingPort))
				}
				require.Equal(t, len(brSet.Egresses), len(no.Egress))
				for i, egress := range no.Egress {
					lastId := -1
					require.Equal(t, brSet.Egresses[i].IsBr, egress.IsBr)
					if group == tc.skipGroup {
						require.Equal(t, wsapisv1.DummyIpPort, egress.ControlAddr)
						require.Equal(t, wsapisv1.DummyIp, egress.EgressAddr)
						continue
					}
					if egress.IsBr {
						egressBrId := brSet.Egresses[i].BrSetId*12 + port
						require.NotEqual(t, lastId, egressBrId)
						require.NotEqual(t, no.NodeId, egressBrId)
						require.Equal(t, no.GroupId, brCfg.BRNodes[egressBrId].GroupId)
						require.Equal(t, no.DomainId, brCfg.BRNodes[egressBrId].DomainId)

						if tc.disableBrMultus {
							require.Equal(t, brCfg.BRNodes[egressBrId].ControlAddr, no.Egress[i].ControlAddr)
						} else {
							require.Equal(t, strconv.Itoa(egressBrId), no.Egress[i].ControlAddr)
						}
						lastId = egressBrId
					} else {
						require.Equal(t, j.Cfg.SystemMap[brSet.Egresses[i].SystemName].CmAddr, no.Egress[i].ControlAddr)
					}
				}
			}
			topSet := brSetsTree[len(brSetsTree)-1].SetId
			for _, group := range brCfg.BRGroups {
				for _, con := range group.Connections {
					require.Equal(t, group.GroupId == tc.skipGroup, con.HasError)
					port := group.GroupId + con.DomainId*3
					if tc.disableBrMultus {
						require.Equal(t, brCfg.BRNodes[topSet*12+port].ControlAddr, con.ControlAddr)
					} else {
						require.Equal(t, strconv.Itoa(topSet*12+port), con.ControlAddr)
					}
				}
			}
		})
	}
}

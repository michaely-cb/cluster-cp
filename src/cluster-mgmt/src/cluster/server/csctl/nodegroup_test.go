//go:build !e2e

package csctl

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"cerebras.com/job-operator/common"

	"cerebras.com/cluster/server/pkg"
)

func TestNodegroupStore_List(t *testing.T) {
	for _, testcase := range []struct {
		name       string
		nodes      *pkg.NodeStoreMocks
		namespace  string
		expectRows []string
	}{
		{
			name: "single group no mgmt",
			nodes: pkg.NewNodeStoreMocks().
				MockNode("memory", "g0", 128<<10, 64).
				MockNode("worker", "g0", 64<<10, 64),
			namespace:  common.SystemNamespace,
			expectRows: []string{"g0,test,,2,1,64Gi,1,128Gi,"},
		},
		{
			name: "no nodegroup assigned namespace",
			nodes: pkg.NewNodeStoreMocks().
				MockNode("memory", "g0", 128<<10, 64).
				MockNode("worker", "g0", 64<<10, 64),
			namespace: "other",
		},
		{
			name: "one nodegroup assigned namespace",
			nodes: pkg.NewNodeStoreMocks().
				MockNode("memory", "g0", 128<<10, 64).
				MockNode("worker", "g0", 64<<10, 64),
			namespace:  "test",
			expectRows: []string{"g0,test,,2,1,64Gi,1,128Gi,"},
		},
		{
			name: "any fills in blanks",
			nodes: pkg.NewNodeStoreMocks().
				MockNode("management", "g0", 128<<10, 64).
				MockNode("memory,worker,broadcastreduce", "g0", 512<<10, 64),
			namespace:  common.SystemNamespace,
			expectRows: []string{"g0,test,,2,1,512Gi,1,512Gi,"},
		},
		{
			name: "multi group - minimum mem cpu - non group - non roles",
			nodes: pkg.NewNodeStoreMocks().
				MockNode("memory", "g0", 64<<10, 64).
				MockNode("memory", "g0", 256<<10, 128).
				MockNode("worker", "g0", 128<<10, 64).
				MockNode("worker", "g0", 256<<10, 128).
				MockNode("management", "g0", 512<<10, 128).
				MockNode("memory", "g1", 128<<10, 64).
				MockNode("worker", "g1", 128<<10, 64).
				MockNode("broadcastreduce", "", 128<<10, 64),
			namespace: common.SystemNamespace,
			expectRows: []string{
				"g0,test,,5,2,128Gi,2,64Gi,",
				"g1,test,,2,1,128Gi,1,128Gi,",
			},
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			s := NodegroupStore{
				cfgProvider:    pkg.StaticCfgProvider(testcase.nodes.BuildCfg()),
				nodeLister:     testcase.nodes,
				metricsQuerier: testcase.nodes,
			}
			rs, err := s.List(context.Background(), testcase.namespace, nil)
			require.NoError(t, err)
			rows := s.AsTable(rs)
			require.Len(t, rows.Rows, len(testcase.expectRows))
			for i, row := range rows.Rows {
				expectCells := strings.Split(testcase.expectRows[i], ",")
				for j, cell := range row.Cells {
					if expectCells[j] != "" {
						assert.Equal(t, expectCells[j], cell)
					}
				}
			}
		})
	}
}

func TestNodegroupStore_Get(t *testing.T) {
	mocks := pkg.NewNodeStoreMocks().
		MockNode("memory", "g0", 128<<10, 64).
		MockNode("worker", "g0", 64<<10, 64)

	for _, testcase := range []struct {
		name              string
		nodes             *pkg.NodeStoreMocks
		namespace         string
		expectRow         string
		expectErrContains string
	}{
		{
			name:      "get system namespace",
			nodes:     mocks,
			namespace: common.SystemNamespace,
			expectRow: "g0,test,,2,1,64Gi,1,128Gi,",
		},
		{
			name:      "get test namespace",
			nodes:     mocks,
			namespace: "test",
			expectRow: "g0,test,,2,1,64Gi,1,128Gi,",
		},
		{
			name:              "get other namespace",
			nodes:             mocks,
			namespace:         "other",
			expectErrContains: "not found",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			s := NodegroupStore{
				cfgProvider:    pkg.StaticCfgProvider(testcase.nodes.BuildCfg()),
				nodeLister:     testcase.nodes,
				metricsQuerier: testcase.nodes,
			}
			rs, err := s.Get(context.Background(), testcase.namespace, "g0")
			if err != nil {
				if testcase.expectErrContains == "" {
					t.Errorf("expected no error but got %v", err)
				} else {
					assert.Contains(t, err.Error(), testcase.expectErrContains)
				}
			} else {
				rows := s.AsTable(rs)
				require.Len(t, rows.Rows, 1)
				row := rows.Rows[0]
				expectCells := strings.Split(testcase.expectRow, ",")
				for j, cell := range row.Cells {
					if expectCells[j] != "" {
						assert.Equal(t, expectCells[j], cell)
					}
				}
			}
		})
	}
}

func TestNodegroupStore_GetNames(t *testing.T) {
	mocks := pkg.NewNodeStoreMocks().
		MockNodeNamedAddressed("mx-01", "ns1", "addr-01", "memory", "g0", 128<<10, 64).
		MockNodeNamedAddressed("mx-02", "ns1", "addr-02", "memory", "g0", 128<<10, 64).
		MockNodeNamedAddressed("mx-03", "ns1", "addr-03", "memory", "g0", 128<<10, 64).
		MockNodeNamedAddressed("wk-11", "ns1", "addr-11", "worker", "g0", 128<<10, 64).
		MockNodeNamedAddressed("wk-12", "ns1", "addr-12", "worker", "g0", 128<<10, 64).
		MockNodeNamedAddressed("mx-21", "ns2", "addr-21", "memory", "g1", 128<<10, 64).
		MockNodeNamedAddressed("mx-22", "ns2", "addr-22", "memory", "g1", 128<<10, 64)

	for _, testcase := range []struct {
		name                string
		nodes               *pkg.NodeStoreMocks
		namespace           string
		nodegroup           string
		expectedMemxNames   []string
		expectedWorkerNames []string
		expectedErrContains string
	}{
		{
			name:                "get g0 node names",
			nodes:               mocks,
			namespace:           "ns1",
			nodegroup:           "g0",
			expectedMemxNames:   []string{"mx-01", "mx-02", "mx-03"},
			expectedWorkerNames: []string{"wk-11", "wk-12"},
		},
		{
			name:                "get g1 node names",
			nodes:               mocks,
			namespace:           "ns2",
			nodegroup:           "g1",
			expectedMemxNames:   []string{"mx-21", "mx-22"},
			expectedWorkerNames: []string{},
		},
		{
			name:                "get g1 node names with wrong namespace",
			nodes:               mocks,
			namespace:           "ns1",
			nodegroup:           "g1",
			expectedErrContains: "nodegroup with name 'g1' was not found",
		},
	} {
		t.Run(testcase.name, func(t *testing.T) {
			s := NodegroupStore{
				cfgProvider:    pkg.StaticCfgProvider(testcase.nodes.BuildCfg()),
				nodeLister:     testcase.nodes,
				metricsQuerier: testcase.nodes,
			}
			rs, err := s.Get(context.Background(), testcase.namespace, testcase.nodegroup)
			if testcase.expectedErrContains != "" {
				assert.Contains(t, err.Error(), testcase.expectedErrContains)
				return
			}
			assert.NoError(t, err)
			nodegroup, ok := rs.(nodegroup)
			assert.True(t, ok)
			for key, value := range nodegroup.status.RoleStatus {
				if key == "memoryx" {
					memxNames := value.GetNames()
					slices.Sort(memxNames)
					assert.Equal(t, testcase.expectedMemxNames, memxNames)
				} else if key == "worker" {
					workerNames := value.GetNames()
					slices.Sort(workerNames)
					assert.Equal(t, testcase.expectedWorkerNames, workerNames)
				}
			}
		})
	}
}

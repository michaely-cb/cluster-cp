//go:build !e2e

package csctl

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"cerebras/pb/workflow/appliance/cluster_mgmt/csctl"

	"cerebras.com/cluster/server/pkg"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	nsfake "cerebras.com/job-operator/client-namespace/clientset/versioned/fake"
	rlfake "cerebras.com/job-operator/client-resourcelock/clientset/versioned/fake"
	rlinformer "cerebras.com/job-operator/client-resourcelock/informers/externalversions"
	systemv1fake "cerebras.com/job-operator/client-system/clientset/versioned/typed/system/v1/fake"
	wsfake "cerebras.com/job-operator/client/clientset/versioned/fake"
	wsinformer "cerebras.com/job-operator/client/informers/externalversions"
	wscommon "cerebras.com/job-operator/common"
)

func TestListClusterRes(t *testing.T) {
	schema := wscommon.NewClusterConfigBuilder("test").
		WithCoordNode().
		WithBR(2).
		WithSysMemxWorkerGroups(2, 1, 1).
		BuildClusterSchema()
	cmData, err := yaml.Marshal(schema)
	require.NoError(t, err)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      wscommon.ClusterConfigMapName,
			Namespace: wscommon.SystemNamespace,
		},
		Data: map[string]string{wscommon.ClusterConfigFile: string(cmData)},
	}

	var nsList = []*corev1.Namespace{
		{ObjectMeta: metav1.ObjectMeta{
			Name:   "test1",
			Labels: map[string]string{"user-namespace": ""}}},
		{ObjectMeta: metav1.ObjectMeta{
			Name:   "test2",
			Labels: map[string]string{"user-namespace": ""}}},
	}

	var sysList = []systemv1.System{
		{ObjectMeta: metav1.ObjectMeta{
			Name:   "system-0",
			Labels: map[string]string{"namespace": "test1", wsapisv1.PlatformVersionKey: "v1"}},
			Spec: systemv1.SystemSpec{Hostname: "system-hostname-0"},
		},
		{ObjectMeta: metav1.ObjectMeta{
			Name:   "system-1",
			Labels: map[string]string{"namespace": "test2", wsapisv1.PlatformVersionKey: "v1__v2"}}},
	}

	nsCli := nsfake.NewSimpleClientset(
		&namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "test1"},
			Status: namespacev1.NamespaceReservationStatus{
				Systems:    []string{schema.Systems[0].Name},
				Nodegroups: []string{schema.Groups[0].Name},
				Nodes:      []string{schema.Nodes[0].Name},
			},
		},
		&namespacev1.NamespaceReservation{
			ObjectMeta: metav1.ObjectMeta{Name: "test2"},
			Status: namespacev1.NamespaceReservationStatus{
				Systems:    []string{schema.Systems[1].Name},
				Nodegroups: []string{schema.Groups[1].Name},
			},
		},
	).NamespaceV1().NamespaceReservations()

	var nodesList = pkg.NewNodeStoreMocks()
	for i, node := range schema.Nodes {
		ns := "test1"
		if node.GetGroup() == schema.Groups[1].Name {
			ns = "test2"
		}
		address := fmt.Sprintf("127.0.0.%d", i)
		if i == len(schema.Nodes)-1 {
			// test the node ip missing case
			address = ""
		}
		nodesList.MockNodeNamedAddressed(node.Name, ns, address, string(node.Role), node.GetGroup(), 128<<10, 64)
		nodesList.MockNodeVersioned("v1")
	}

	var lockList = []runtime.Object{
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-1",
				Namespace: "test1",
			},
			Spec: rlv1.ResourceLockSpec{
				QueueName: rlv1.ExecuteQueueName,
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
				ResourceGrants: []rlv1.ResourceGrant{
					{
						Name: rlv1.SystemsRequestName,
						Resources: []rlv1.Res{
							{
								Name: "system-0",
							},
						},
					},
				},
				GroupResourceGrants: []rlv1.GroupResourceGrant{
					{
						ResourceGrants: []rlv1.ResourceGrant{
							{
								Name: rlv1.NodeResourceType,
								Resources: []rlv1.Res{
									{
										Name: "group0-memory-0",
									},
								},
							},
							{
								Name: rlv1.NodeResourceType,
								Resources: []rlv1.Res{
									{
										Name: "br-1",
									},
								},
							},
						},
					},
				},
			},
		},
		&rlv1.ResourceLock{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-2",
				Namespace: "test1",
			},
			Spec: rlv1.ResourceLockSpec{
				QueueName: rlv1.ExecuteQueueName,
			},
			Status: rlv1.ResourceLockStatus{
				State: rlv1.LockGranted,
				GroupResourceGrants: []rlv1.GroupResourceGrant{
					{
						ResourceGrants: []rlv1.ResourceGrant{
							{
								Name: rlv1.NodeResourceType,
								Resources: []rlv1.Res{
									{
										Name: "group0-memory-0",
									},
								},
							},
							{
								Name: rlv1.NodeResourceType,
								Resources: []rlv1.Res{
									{
										Name: "group0-memory-0",
									},
								},
							},
						},
					},
				},
			},
		},
	}

	var jobList = []runtime.Object{
		&wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-1",
				Namespace: "test1",
				Labels: map[string]string{
					"labels.k8s.cerebras.com/a": "b",
					"labels.k8s.cerebras.com/c": "d",
				},
			},
		},
		&wsapisv1.WSJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "job-2",
				Namespace: "test1",
			},
		},
	}

	k8s := fake.NewSimpleClientset()
	sysC := systemv1fake.FakeSystemV1{Fake: &k8stesting.Fake{}}

	// init ns
	for _, ns := range nsList {
		_, err := k8s.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	// init cluster CM
	cm, err = k8s.CoreV1().ConfigMaps(wscommon.SystemNamespace).Create(context.Background(), cm, metav1.CreateOptions{})
	require.NoError(t, err)
	// init systems
	sysRes := &systemv1.SystemList{Items: sysList}
	sysC.AddReactor(
		"list",
		"systems",
		func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
			return true, sysRes, nil
		},
	)
	// init locks
	rlClient := rlfake.NewSimpleClientset(lockList...)
	factory := rlinformer.NewSharedInformerFactoryWithOptions(rlClient, 0)
	rlInformer, _ := factory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("resourcelocks"))
	factory.Start(context.Background().Done())
	factory.WaitForCacheSync(context.Background().Done())

	// init jobs
	jobClient := wsfake.NewSimpleClientset(jobList...)
	jobFactory := wsinformer.NewSharedInformerFactoryWithOptions(jobClient, 0)
	jobInformer, _ := jobFactory.ForResource(wsapisv1.SchemeGroupVersion.WithResource("wsjobs"))
	jobFactory.Start(context.Background().Done())
	jobFactory.WaitForCacheSync(context.Background().Done())

	// init store
	s := ClusterStore{
		sysClient:      sysC.Systems(),
		cfgProvider:    pkg.NewK8sClusterCfgProvider(k8s, nsCli),
		lockInformer:   rlInformer,
		wsJobInformer:  jobInformer,
		nodeLister:     nodesList,
		metricsQuerier: nodesList,
	}

	// list by NS, SX will only show if running jobs
	rs, err := s.List(context.Background(), "test1", nil)
	require.NoError(t, err)
	rows := s.AsTable(rs)
	t.Log(rows)
	expectRows := []string{
		//"Name;Session;Hostname;Type;Version;CPU;MEM;CPU_USE;MEM_USE;JobIds;JobLabels;State;Notes",
		"system-0;test1;system-hostname-0;system;v1;n/a;n/a;n/a;n/a;test1/job-1;ok;",
		"0-coord-0;test1;0-coord-0;coordinator;v1;64;128Gi;unknown;unknown;;ok;",
		"group0-memory-0;test1;group0-memory-0;memoryx;v1;64;128Gi;unknown;unknown;test1/job-1,test1/job-2;ok;",
		"br-1;test1;br-1;swarmx;v1;64;128Gi;unknown;unknown;test1/job-1;ok;",
		"group0-worker-0;test1;group0-worker-0;worker;v1;64;128Gi;unknown;unknown;;ok;",
	}
	for i, r := range expectRows {
		expectCells := strings.Split(r, ";")
		for j, cell := range rows.Rows[i].Cells {
			assert.Equal(t, expectCells[j], cell)
		}
	}

	// list by role+version
	rs, err = s.List(context.Background(), wscommon.SystemNamespace, &csctl.GetOptions{
		Role:    "system",
		Version: "v1",
	})
	require.NoError(t, err)
	rows = s.AsTable(rs)
	t.Log(rows)
	expectRows = []string{
		//"Name;Session;Hostname;Type;Version;CPU;MEM;CPU_USE;MEM_USE;JobIds;JobLabels;State;Notes",
		"system-0;test1;system-hostname-0;system;v1;n/a;n/a;n/a;n/a;test1/job-1;ok;",
		"system-1;test2;system-1;system;v1,v2;n/a;n/a;n/a;n/a;;ok;",
	}
	for i, r := range expectRows {
		expectCells := strings.Split(r, ";")
		for j, cell := range rows.Rows[i].Cells {
			assert.Equal(t, expectCells[j], cell)
		}
	}

	// list by version not equal
	rs, err = s.List(context.Background(), wscommon.SystemNamespace, &csctl.GetOptions{
		Role:    "system",
		Version: "!v2",
	})
	require.NoError(t, err)
	rows = s.AsTable(rs)
	t.Log(rows)
	expectRows = []string{
		//"Name;Session;Hostname;Type;Version;CPU;MEM;CPU_USE;MEM_USE;JobIds;JobLabels;State;Notes",
		"system-0;test1;system-hostname-0;system;v1;n/a;n/a;n/a;n/a;test1/job-1;ok;",
	}
	for i, r := range expectRows {
		expectCells := strings.Split(r, ";")
		for j, cell := range rows.Rows[i].Cells {
			assert.Equal(t, expectCells[j], cell)
		}
	}

	// list all
	rs, err = s.List(context.Background(), wscommon.SystemNamespace, nil)
	require.NoError(t, err)
	rows = s.AsTable(rs)
	t.Log(rows)
	expectRows = []string{
		//"Name;Session;Hostname;Type;Version;CPU;MEM;CPU_USE;MEM_USE;JobIds;JobLabels;State;Notes",
		"system-0;test1;system-hostname-0;system;v1;n/a;n/a;n/a;n/a;test1/job-1;ok;",
		"system-1;test2;system-1;system;v1,v2;n/a;n/a;n/a;n/a;;ok;",
		"0-coord-0;test1;0-coord-0;coordinator;v1;64;128Gi;unknown;unknown;;ok;",
		"group0-memory-0;test1;group0-memory-0;memoryx;v1;64;128Gi;unknown;unknown;test1/job-1,test1/job-2;ok;",
		"br-0;test1;br-0;swarmx;v1;64;128Gi;unknown;unknown;;ok;",
		"br-1;test1;br-1;swarmx;v1;64;128Gi;unknown;unknown;test1/job-1;ok;",
		"group0-worker-0;test1;group0-worker-0;worker;v1;64;128Gi;unknown;unknown;;ok;",
		"group1-worker-0;test2;group1-worker-0;worker;v1;64;128Gi;unknown;unknown;;ok;",
	}
	for i, r := range expectRows {
		expectCells := strings.Split(r, ";")
		for j, cell := range rows.Rows[i].Cells {
			assert.Equal(t, expectCells[j], cell)
		}
	}
}

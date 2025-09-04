//go:build integration && configmap_gen

package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/google/uuid"
	netv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
	wsresource "cerebras.com/job-operator/common/resource"
)

func (t *TestSuite) TestConfigMapGeneration() {
	ctx := context.Background()

	for _, testcase := range []struct {
		name                    string
		ns                      string
		clusterMode             bool
		disableMultus           bool
		canServerGenerateConfig bool
		operatorUpgrade         bool
	}{
		{"test system NS jobs", wscommon.DefaultNamespace, true, false, false, false},
		{"test cluster mode user NS jobs", "cluster-user-namespace", true, false, false, false},
		{"test hostnetwork user NS jobs", "cluster-user-hostnetwork", true, true, false, false},
		{"test server generated config", "server-generated-config", true, true, true, false},
		{"test operator upgrade", "operator-backfill-config", true, true, true, true},
		{"test non-cluster mode user NS jobs", "isolation-user-namespace", false, false, false, false},
	} {
		t.Run("config_generation/update_success"+testcase.name, func() {
			wscommon.EnableClusterMode = testcase.clusterMode
			wscommon.DisableMultus = testcase.disableMultus
			wsapisv1.DisableMultusForBR = testcase.disableMultus
			t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus

			sysList := &systemv1.SystemList{}
			require.NoError(t.T(), k8sClient.List(context.Background(), sysList))
			require.Equal(t.T(), systemCount, len(sysList.Items))

			if testcase.ns != wscommon.DefaultNamespace {
				if !testcase.clusterMode {
					wscommon.Namespace = testcase.ns
				}
				t.createNsIfNotExists(testcase.ns)
				t.unassignEverythingFromNS(wscommon.DefaultNamespace)
				t.assignEverythingToNS(testcase.ns, systemCount)

				// Check session resource metrics
				assert.True(t.T(), wscommon.SessionHasResourceMovementForTesting(testcase.ns, wsresource.SystemType, "system-0", "add"))
				assert.True(t.T(), wscommon.SessionHasResourceCountForTesting(testcase.ns, wsresource.SystemType, float64(systemCount)))

				require.Eventually(t.T(), func() bool {
					t.T().Log(t.CfgMgr.Cfg.GetNsSysNameMap())
					return len(t.CfgMgr.Cfg.GetAssignedSystemsList(testcase.ns)) == systemCount
				}, eventuallyTimeout, eventuallyTick)
			}
			defer func() {
				if testcase.ns != wscommon.DefaultNamespace {
					wscommon.Namespace = wscommon.DefaultNamespace
					wscommon.EnableClusterMode = true

					t.unassignEverythingFromNS(testcase.ns)
					t.assignEverythingToNS(wscommon.DefaultNamespace, systemCount)

					require.Eventually(t.T(), func() bool {
						t.T().Log(t.CfgMgr.Cfg.GetNsSysNameMap())
						return len(t.CfgMgr.Cfg.GetAssignedSystemsList(wscommon.Namespace)) == systemCount
					}, eventuallyTimeout, eventuallyTick)

					require.NoError(t.T(), k8sClient.Delete(ctx, &corev1.Namespace{ObjectMeta: ctrl.ObjectMeta{Name: testcase.ns}}))
				}
				wsapisv1.DisableMultusForBR = true
				wscommon.DisableMultus = true
				t.JobReconciler.JobController.Config.EnableMultus = !wscommon.DisableMultus
			}()

			if testcase.ns != wscommon.DefaultNamespace {
				assertClusterServerIngressSetup(t.T(), testcase.ns)
				defer assertClusterServerIngressCleanup(t.T(), testcase.ns)
			}
			job := newWsExecuteJob(testcase.ns, testcase.ns+"-config-update", 5)

			policy := commonv1.CleanPodPolicyRunning
			if testcase.ns == wscommon.DefaultNamespace {
				policy = commonv1.CleanPodPolicyNone
			}
			job.Spec.RunPolicy.CleanPodPolicy = &policy
			if testcase.canServerGenerateConfig {
				job.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
			}
			require.NoError(t.T(), k8sClient.Create(ctx, job))
			defer assertWsjobCleanedUp(t.T(), job)

			clusterDetailsConfigMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job.Name),
					Namespace: job.Namespace,
				},
			}
			cdConfig := &commonpb.ClusterDetails{}
			options := protojson.UnmarshalOptions{
				AllowPartial:   true,
				DiscardUnknown: true,
			}

			if testcase.canServerGenerateConfig {
				// expect cluster details config not to exist in the first few seconds
				require.Never(t.T(), func() bool {
					err := k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
					return !apierrors.IsNotFound(err)
				}, 3*time.Second, eventuallyTick)

				if !testcase.operatorUpgrade {
					// simulating how server would generate the configmap
					require.Eventually(t.T(), func() bool {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(job), job)
						return err == nil
					}, eventuallyTimeout, eventuallyTick)

					filename := "testdata/5_sys_cluster_details_req.json"

					// simulate cluster server creating the config
					clusterDetailsData, err := ioutil.ReadFile(filename)
					require.NoError(t.T(), err)

					clusterDetailsConfigMap = &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job.Name),
							Namespace: job.Namespace,
							Labels: map[string]string{
								"job-name":             job.Name,
								wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeClusterDetails,
							},
							OwnerReferences: []metav1.OwnerReference{{
								APIVersion:         "jobs.cerebras.com/v1",
								BlockOwnerDeletion: wscommon.Pointer(true),
								Controller:         wscommon.Pointer(true),
								Kind:               "WSJob",
								Name:               job.Name,
								UID:                job.ObjectMeta.UID,
							}},
						},
						Data: map[string]string{
							wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.InitSignalVal,
						},
					}

					compressed, err := wscommon.Gzip(clusterDetailsData)
					require.NoError(t.T(), err)

					clusterDetailsConfigMap.BinaryData = wscommon.EnsureMap(clusterDetailsConfigMap.BinaryData)
					clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName] = compressed

					err = options.Unmarshal(clusterDetailsData, cdConfig)
					require.NoError(t.T(), err)
					var actToCsxDomains []string
					for _, taskInfo := range cdConfig.Tasks {
						if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_ACT {
							for _, taskMap := range taskInfo.TaskMap {
								actToCsxDomains = append(actToCsxDomains, wscommon.SerializeWioTaskMap(taskMap))
							}
						}
					}

					compressed, err = wscommon.Gzip([]byte(strings.Join(actToCsxDomains, ",")))
					require.NoError(t.T(), err)
					clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsActToCsxDomains] = compressed

					err = k8sClient.Create(ctx, clusterDetailsConfigMap)
					require.NoError(t.T(), err)

					// cluster details config generated
					require.Eventually(t.T(), func() bool {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
						return err == nil
					}, eventuallyTimeout, eventuallyTick)
				} else {
					clusterDetailsConfigMap = &corev1.ConfigMap{
						ObjectMeta: metav1.ObjectMeta{
							Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job.Name),
							Namespace: job.Namespace,
						},
					}

					// cluster details config eventually generated by operator to simulate the operator upgrade case
					require.Eventually(t.T(), func() bool {
						err := k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
						return err == nil
					}, wscommon.DefaultClusterDetailsGenerationTimeout, eventuallyTick)
				}
			}

			brConfigMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf(wsapisv1.BrConfigName, job.Name),
					Namespace: job.Namespace,
				},
			}
			// br config generated
			require.Eventually(t.T(), func() bool {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(brConfigMap), brConfigMap)
				return err == nil
			}, eventuallyTimeout, eventuallyTick)
			assert.Equal(t.T(), wsapisv1.ConfigMapTypeBRConfig, brConfigMap.Labels[wsapisv1.ConfigMapType])

			brConfig := wscommon.BRConfig{}
			t.T().Log(brConfigMap)
			data, err := wscommon.Gunzip(brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName])
			require.NoError(t.T(), err)
			err = json.Unmarshal(data, &brConfig)
			require.NoError(t.T(), err)

			if !testcase.canServerGenerateConfig || testcase.operatorUpgrade {
				// cluster details config generated
				require.Eventually(t.T(), func() bool {
					err := k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
					return err == nil
				}, eventuallyTimeout, eventuallyTick)
				assert.Equal(t.T(), wsapisv1.ConfigMapTypeClusterDetails, clusterDetailsConfigMap.Labels[wsapisv1.ConfigMapType])

				data, err = wscommon.Gunzip(clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName])
				require.NoError(t.T(), err)
				err = options.Unmarshal(data, cdConfig)
				require.NoError(t.T(), err)
			}

			pods := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 24+countNonBrPods(job))
			// mock multus ip assignment
			if !testcase.disableMultus {
				for _, po := range pods.Items {
					status := []netv1.NetworkStatus{
						{
							Name:      "cilium",
							Interface: "eth0",
							IPs:       []string{""},
						},
						{
							Name:      wscommon.GetNadByIndex(0),
							Interface: "net1",
							IPs: []string{
								"10.10.10.10",
							},
						},
						{
							Name:      wscommon.GetNadByIndex(1),
							Interface: "net2",
							IPs: []string{
								"10.10.10.11",
							},
						},
						{
							Name:      wscommon.GetNadByIndex(2),
							Interface: "net3",
							IPs: []string{
								"10.10.10.12",
							},
						},
						{
							Name:      wscommon.GetNadByIndex(3),
							Interface: "net4",
							IPs: []string{
								"10.10.10.13",
							},
						},
						{
							Name:      wscommon.GetNadByIndex(4),
							Interface: "net5",
							IPs: []string{
								"10.10.10.14",
							},
						},
					}
					netStatus, _ := json.Marshal(status)
					po.Annotations = wscommon.EnsureMap(po.Annotations)
					po.Annotations[netv1.NetworkStatusAnnot] = string(netStatus)
					require.NoError(t.T(), k8sClient.Update(ctx, &po))
				}
			}

			// svc/endpoint only ready after pod ready
			assertServiceCountEventually(t.T(), job.Namespace, job.Name, "", 1)
			assertEndpointCountEventually(t.T(), job.Namespace, job.Name, "", 0)

			// mock init started
			brMap := map[string]corev1.Pod{}
			pods = getJobPods(t.T(), job.Namespace, job.Name, "")
			for _, po := range pods.Items {
				po.Status = corev1.PodStatus{
					Phase:  corev1.PodPending,
					HostIP: fmt.Sprintf("127.0.%s.0", po.Labels["replica-index"]),
					PodIP:  fmt.Sprintf("127.0.%s.0", po.Labels["replica-index"]),
					InitContainerStatuses: []corev1.ContainerStatus{
						{
							State: corev1.ContainerState{
								Running: &corev1.ContainerStateRunning{
									StartedAt: metav1.Now(),
								},
							},
						},
					},
					StartTime: &metav1.Time{
						Time: time.Now(),
					},
				}
				require.NoError(t.T(), k8sClient.Status().Update(ctx, &po))
				if po.Labels["replica-type"] == strings.ToLower(rlv1.BrRequestName) {
					brMap[po.Labels["replica-index"]] = po
				}
			}

			// validate cluster details generated/updated
			require.Eventually(t.T(), func() bool {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
				if err != nil {
					return false
				}
				return clusterDetailsConfigMap.Data[wsapisv1.ClusterDetailsConfigUpdatedSignal] == wsapisv1.UpdatedSignalVal
			}, eventuallyTimeout, eventuallyTick)
			data, err = wscommon.Gunzip(clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName])
			require.NoError(t.T(), err)
			err = options.Unmarshal(data, cdConfig)
			require.NoError(t.T(), err)
			for _, task := range cdConfig.Tasks {
				if task.TaskType == commonpb.ClusterDetails_TaskInfo_CRD {
					addr := "10.10.10.10:9000"
					if testcase.disableMultus {
						addr = "127.0.0.0:9000"
					}
					assert.Equal(t.T(), addr, task.TaskMap[0].AddressBook.TaskIpPort)
				}
			}

			// validate br config updated
			require.Eventually(t.T(), func() bool {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(brConfigMap), brConfigMap)
				if err != nil {
					return false
				}
				return brConfigMap.Data[wsapisv1.BrConfigUpdatedSignal] == wsapisv1.UpdatedSignalVal
			}, eventuallyTimeout, eventuallyTick)
			data, err = wscommon.Gunzip(brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName])
			require.NoError(t.T(), err)
			err = json.Unmarshal(data, &brConfig)
			require.NoError(t.T(), err)

			brGrant, _ := t.JobReconciler.JobResourceCache.Get(job.NamespacedName())
			nodeGrants := brGrant.(jobResources).replicaToNodeMap[rlv1.BrRequestName]
			for _, br := range brConfig.BRNodes {
				// validate pod/container resource/env/anno update
				for _, container := range brMap[strconv.Itoa(br.NodeId)].Spec.Containers {
					if container.Name == wsapisv1.DefaultContainerName {
						var brInsEnv string
						for _, env := range container.Env {
							if env.Name == wsapisv1.BrInstanceIdEnvVarName {
								brInsEnv = env.Value
							}
						}
						if br.NodeId < 12 || br.NodeId%2 == 0 {
							assert.Equal(t.T(), "0", brInsEnv, br.NodeId)
						} else {
							assert.Equal(t.T(), "1", brInsEnv, br.NodeId)
						}
					}
				}
				if !testcase.disableMultus {
					if br.NodeId < 12 {
						assert.Contains(t.T(),
							brMap[strconv.Itoa(br.NodeId)].Annotations[netv1.NetworkAttachmentAnnot],
							strings.Join([]string{
								wscommon.GetNadByIndex(0),
								wscommon.GetNadByIndex(1),
								wscommon.GetNadByIndex(2),
								wscommon.GetNadByIndex(3),
								wscommon.GetNadByIndex(4)}, ","), br.NodeId)
					} else if br.NodeId%2 == 0 {
						assert.Contains(t.T(),
							brMap[strconv.Itoa(br.NodeId)].Annotations[netv1.NetworkAttachmentAnnot],
							strings.Join([]string{
								wscommon.GetNadByIndex(0),
								wscommon.GetNadByIndex(1),
								wscommon.GetNadByIndex(2)}, ","), br.NodeId)
					} else {
						assert.Contains(t.T(),
							brMap[strconv.Itoa(br.NodeId)].Annotations[netv1.NetworkAttachmentAnnot],
							strings.Join([]string{
								wscommon.GetNadByIndex(3),
								wscommon.GetNadByIndex(4),
								wscommon.GetNadByIndex(5)}, ","), br.NodeId)
					}
				}

				// Check a few exported info metrics.
				assert.True(t.T(), wscommon.JobHasNicForTesting(job.Namespace, job.Name,
					testcase.ns+"-config-update-broadcastreduce-9", "br-21", "nic0", "127.0.21.0"))
				assert.True(t.T(), wscommon.JobHasNicForTesting(job.Namespace, job.Name,
					testcase.ns+"-config-update-broadcastreduce-9", "br-21", "nic1", "127.0.21.1"))
				assert.True(t.T(), wscommon.JobHasNicForTesting(job.Namespace, job.Name,
					testcase.ns+"-config-update-broadcastreduce-9", "br-21", "nic2", "127.0.21.2"))
				assert.True(t.T(), wscommon.JobHasNicForTesting(job.Namespace, job.Name,
					testcase.ns+"-config-update-broadcastreduce-7", "br-19", "nic2", "127.0.19.2"))

				// validate br config map format
				grantNics := brGrant.(jobResources).getAssignedNicsForReplica(wsapisv1.WSReplicaTypeBroadcastReduce, br.NodeId)
				assert.Equal(t.T(), len(grantNics), len(br.Egress)+1)
				ingressIp := t.JobReconciler.Cfg.NodeMap[nodeGrants[br.NodeId]].GetNicAddress(grantNics[0])
				ingressPort := wsapisv1.DefaultPort
				if testcase.disableMultus {
					insId := brGrant.(jobResources).getAssignedInstanceId(wsapisv1.WSReplicaTypeBroadcastReduce, br.NodeId)
					insIdInt, _ := strconv.Atoi(insId)
					ingressPort = wsapisv1.DefaultPort + insIdInt
				} else {
					ingressIp = "10.10.10.10"
				}
				assert.Equal(t.T(), ingressIp, br.IngressAddr)
				assert.Equal(t.T(), fmt.Sprintf("%s:%d", ingressIp, ingressPort), br.ControlAddr)
				for i, eg := range br.Egress {
					var egAddr string
					if testcase.disableMultus {
						egAddr = t.JobReconciler.Cfg.NodeMap[nodeGrants[br.NodeId]].GetNicAddress(grantNics[i+1])
					} else {
						egAddr = fmt.Sprintf("10.10.10.1%d", i+1)
					}
					assert.Equal(t.T(), egAddr, eg.EgressAddr)
				}
			}

			// update pod to running
			pods = getJobPods(t.T(), job.Namespace, job.Name, "")
			for _, po := range pods.Items {
				if po.Labels["replica-type"] != strings.ToLower(rlv1.BrRequestName) {
					po.Status = corev1.PodStatus{
						Phase: corev1.PodRunning,
						Conditions: []corev1.PodCondition{
							{
								Type:   corev1.PodReady,
								Status: corev1.ConditionTrue,
							},
						},
					}
					require.NoError(t.T(), k8sClient.Status().Update(ctx, &po))
				}
			}
			// ep will be created by controller which doesn't exist in envtest
			if !testcase.disableMultus {
				assertEndpointCountEventually(t.T(), job.Namespace, job.Name, "", 1)
			}

			// update pod to fail
			pods = getJobPods(t.T(), job.Namespace, job.Name, "")
			for _, po := range pods.Items {
				if po.Labels["replica-type"] == strings.ToLower(rlv1.BrRequestName) {
					t.T().Log("error out pod " + po.Name)
					po.Status = corev1.PodStatus{
						Phase: corev1.PodFailed,
					}
					require.NoError(t.T(), k8sClient.Status().Update(ctx, &po))
					break
				}
			}
			assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
			if *job.Spec.RunPolicy.CleanPodPolicy == commonv1.CleanPodPolicyNone {
				assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 24+countNonBrPods(job))
				// explicit cleanup resource lock since envtest don't have controller manager for GC
				_ = cleanupLock(t.T(), job.Namespace, job.Name)
			} else if *job.Spec.RunPolicy.CleanPodPolicy == commonv1.CleanPodPolicyRunning {
				assertPodCountEventually(t.T(), job.Namespace, job.Name, "", 1)
			}
			_ = k8sClient.DeleteAllOf(ctx, &corev1.ConfigMap{}, client.InNamespace(wscommon.SystemNamespace),
				client.MatchingLabels{
					wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeNICLock,
				})
		})
	}

	t.Run("br_config_generation_for_1_system_request_success", func() {
		job := newWsExecuteJob(wscommon.Namespace, "ws-1-system", 1)
		require.NoError(t.T(), k8sClient.Create(ctx, job))
		defer assertWsjobCleanedUp(t.T(), job)

		brConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.BrConfigName, job.Name),
				Namespace: job.Namespace,
			},
		}

		// br config generated
		require.Eventually(t.T(), func() bool {
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(brConfigMap), brConfigMap)
			if err != nil && apierrors.IsNotFound(err) {
				return false
			}
			require.NoError(t.T(), err)
			return true
		}, eventuallyTimeout, eventuallyTick)

		brConfig := wscommon.BRConfig{}
		data, err := wscommon.Gunzip(brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName])
		require.NoError(t.T(), err)
		err = json.Unmarshal(data, &brConfig)
		require.NoError(t.T(), err)
		//t.T().Log(brConfig)

		// br config will only contain 1 CS2 without nodes/groups
		assert.Equal(t.T(), 1, len(brConfig.CSNodes))
		assert.Nil(t.T(), brConfig.BRNodes)
		assert.Nil(t.T(), brConfig.BRGroups)

		pods := assertPodCountEventually(t.T(), job.Namespace, job.Name, "", countNonBrPods(job))
		for _, pod := range pods.Items {
			pod.Status = corev1.PodStatus{
				Phase: corev1.PodRunning,
			}
			require.NoError(t.T(), k8sClient.Status().Update(ctx, &pod))
		}

		wscommon.JobHasNicForTesting(job.Namespace, job.Name,
			wscommon.Namespace+"-worker-1", "group1-worker-1", "nic1", "127.4.1.1")
		wscommon.JobHasNicForTesting(job.Namespace, job.Name,
			wscommon.Namespace+"-command-2", "group1-memory-4", "nic1", "127.5.1.1")
	})

	for idx, testcase := range []struct {
		GroupSizeOnEachSystem []int
		useOldClusterServer   bool
		expectFail            bool
		expectWseGroupId      []int
	}{
		{[]int{4, 4, 4, 4, 4, 4}, true, false, []int{0, 0, 0, 0, 0, 0}},
		{[]int{0, 1, 2, 3, 4, 3}, true, false, []int{0, 0, 0, 0, 0, 0}},
		{[]int{0, 0, 0, 0, 0, 0}, false, false, []int{0, 0, 0, 0, 0, 0}},
		{[]int{4, 4, 4, 4, 4, 4}, false, false, []int{0, 1, 2, 3, 0, 1}},
		{[]int{6, 6, 6, 6, 6, 6}, false, false, []int{0, 1, 2, 3, 4, 5}},
		{[]int{0, 2, 2, 2, 2, 2}, false, true, []int{}},
		{[]int{0, 1, 2, 2, 2, 2}, false, true, []int{}},
	} {
		t.Run(fmt.Sprintf("br_config_generation_with_wse_group_size_test%d", idx), func() {
			numSystems := len(testcase.GroupSizeOnEachSystem)

			jobName := fmt.Sprintf("wsjob-with-wse-group-size-test%d", idx)
			job := newWsExecuteJob(wscommon.Namespace, jobName, uint32(numSystems))
			if !testcase.useOldClusterServer {
				job.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ExpertParallelOnBr].String()
			}
			require.NoError(t.T(), k8sClient.Create(ctx, job))
			defer assertWsjobCleanedUp(t.T(), job)

			// Create cluster details config map with expert parallel group size
			cdConfig := &commonpb.ClusterDetails{
				Tasks: []*commonpb.ClusterDetails_TaskInfo{
					{
						TaskType: commonpb.ClusterDetails_TaskInfo_WSE,
						TaskMap:  []*commonpb.ClusterDetails_TaskInfo_TaskMap{},
					},
				},
			}
			for i := 0; i < numSystems; i++ {
				cdConfig.Tasks[0].TaskMap = append(cdConfig.Tasks[0].TaskMap, &commonpb.ClusterDetails_TaskInfo_TaskMap{
					TaskId:      &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{TaskId: int32(i), WseId: int32(i), WseIds: []int32{int32(i)}},
					AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{WseGroupSize: int32(testcase.GroupSizeOnEachSystem[i])},
				})
			}
			options := protojson.MarshalOptions{EmitUnpopulated: true}
			serializedClusterDetails, _ := options.Marshal(cdConfig)
			compressedCd, _ := wscommon.Gzip(serializedClusterDetails)
			clusterDetailsConfigMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job.Name),
					Namespace: job.Namespace,
					Labels: map[string]string{
						"job-name":             job.Name,
						wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeClusterDetails,
					},
					OwnerReferences: []metav1.OwnerReference{{
						APIVersion:         "jobs.cerebras.com/v1",
						BlockOwnerDeletion: wscommon.Pointer(true),
						Controller:         wscommon.Pointer(true),
						Kind:               "WSJob",
						Name:               job.Name,
						UID:                job.ObjectMeta.UID,
					}},
				},
				Data: map[string]string{
					wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.InitSignalVal,
				},
				BinaryData: map[string][]byte{
					wsapisv1.ClusterDetailsConfigCompressedFileName: compressedCd,
				},
			}
			require.NoError(t.T(), k8sClient.Create(ctx, clusterDetailsConfigMap))

			brConfigMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf(wsapisv1.BrConfigName, job.Name),
					Namespace: job.Namespace,
				},
			}

			if testcase.expectFail {
				job = assertWsJobFailEventually(t.T(), job.Namespace, job.Name)
				lastCondition := job.Status.Conditions[len(job.Status.Conditions)-1]
				assert.Contains(t.T(), lastCondition.Message, "inconsistent WSE group size")
				return
			}
			// br config generated
			require.Eventually(t.T(), func() bool {
				err := k8sClient.Get(ctx, client.ObjectKeyFromObject(brConfigMap), brConfigMap)
				if err != nil && apierrors.IsNotFound(err) {
					return false
				}
				require.NoError(t.T(), err)
				return true
			}, eventuallyTimeout, eventuallyTick)

			// Verify BR config content
			brConfig := wscommon.BRConfig{}
			data, err := wscommon.Gunzip(brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName])
			require.NoError(t.T(), err)
			err = json.Unmarshal(data, &brConfig)
			require.NoError(t.T(), err)

			// Verify CSNodes count and expert parallel group indexing
			assert.Equal(t.T(), numSystems, len(brConfig.CSNodes))
			for i, csNode := range brConfig.CSNodes {
				assert.Equal(t.T(), i, csNode.NodeId, "NodeId should match index")
				assert.Equal(t.T(), testcase.expectWseGroupId[i], csNode.WseGroupId,
					"WseGroupId should be %d for system %d", testcase.expectWseGroupId[i], i)
			}
		})
	}

	for _, testcase := range []struct {
		numSys int
	}{
		{0},
		{1},
		{2},
	} {
		t.Run(fmt.Sprintf("cluster details config generation success with %d systems", testcase.numSys), func() {
			jobName := fmt.Sprintf("wsjob-cluster-details-%d", testcase.numSys)
			var wsjob *wsapisv1.WSJob
			if testcase.numSys > 0 {
				wsjob = newWsExecuteJob(wscommon.Namespace, jobName, uint32(testcase.numSys))
			} else {
				wsjob = newWsCompileJob(wscommon.Namespace, jobName, uint32(testcase.numSys))
			}
			require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
			defer assertWsjobCleanedUp(t.T(), wsjob)

			requireClusterDetailsCreatedEventually(t.T(), jobName, wsjob.Namespace)
		})
	}
}

func (t *TestSuite) TestResourceDetailsGenerationV1Network() {
	ctx := context.Background()
	t.Run("resource_details_created_for_compile_job_v1_network", func() {
		jobName := "wsjob-resource-details"
		wsjob := newWsCompileJob(wscommon.Namespace, jobName, 0)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		defer assertWsjobCleanedUp(t.T(), wsjob)

		cm := requireClusterDetailsCreatedEventually(t.T(), jobName, wsjob.Namespace)
		t.T().Logf("resourceDetails: %s", cm.Data[wsapisv1.WsJobResourceDetailsName])
		rd := &ResourceDetails{}
		rdv2 := &ResourceDetailsV2{}
		err := json.Unmarshal([]byte(cm.Data[wsapisv1.WsJobResourceDetailsName]), rd)
		require.NoError(t.T(), err)

		err = json.Unmarshal([]byte(cm.Data[wsapisv1.WsJobResourceDetailsV2Name]), rdv2)
		require.NoError(t.T(), err)

		findTierByType := func(tiers []*NodeGroupTier, tierType string) *NodeGroupTier {
			for _, tier := range tiers {
				if tier.Type == tierType {
					return tier
				}
			}
			return nil
		}

		assert.Greater(t.T(), rd.LargestMemoryPrimaryNodeGroup.MemoryxEffectiveMemoryBytes, int64(0))
		assert.Greater(t.T(), rd.LargestMemoryPrimaryNodeGroup.MemoryxMemoryBytes, int64(0))
		assert.Equal(t.T(), rd.LargestMemoryPrimaryNodeGroup.MemoryxCount, memxPrimaryCount)

		assert.Greater(t.T(), rd.SmallestMemorySecondaryNodeGroup.MemoryxEffectiveMemoryBytes, int64(0))
		assert.Greater(t.T(), rd.SmallestMemorySecondaryNodeGroup.MemoryxMemoryBytes, int64(0))
		assert.Equal(t.T(), rd.SmallestMemorySecondaryNodeGroup.MemoryxCount, memxSecondaryV1Count)

		assert.Equal(t.T(), 3, len(rdv2.Nodegroups))
		regularTier := findTierByType(rdv2.Nodegroups, "regular")
		assert.Equal(t.T(), 8, regularTier.Count)

		largeTier := findTierByType(rdv2.Nodegroups, "large")
		assert.Equal(t.T(), 0, largeTier.Count)

		pods := assertPodCountEventually(t.T(), wsjob.Namespace, wsjob.Name, "", 1)
		for _, pod := range pods.Items {
			foundResourceDetailsEnvVar := false
			foundResourceDetailsV2EnvVar := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.WsJobResourceDetailsFPEnv {
					foundResourceDetailsEnvVar = true
				}
				if env.Name == wsapisv1.WsJobResourceDetailsV2FPEnv {
					foundResourceDetailsV2EnvVar = true
				}
			}
			assert.True(t.T(), foundResourceDetailsEnvVar && foundResourceDetailsV2EnvVar)
		}
	})
}

func (t *TestSuite) TestResourceDetailsGenerationV2Network() {
	ctx := context.Background()
	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	t.Run("resource_details_fails_job_if_sanity_check_fails", func() {
		defer func(orig int) {
			memxRegularMemBytes = orig
		}(memxRegularMemBytes)
		// cluster configured to expect 2x the default memory
		memxRegularMemBytes = wscommon.DefaultNodeMem << 21

		jobName := "wsjob-resource-details-fail"
		wsjob := newWsCompileJob(wscommon.Namespace, jobName, 0)
		require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
		defer assertWsjobCleanedUp(t.T(), wsjob)

		wsjob = assertWsJobFailEventually(t.T(), wscommon.Namespace, jobName)
		failMsg := wsjob.Status.Conditions[len(wsjob.Status.Conditions)-1].Message
		assert.Contains(t.T(), failMsg, "invalid cluster resource configuration")
	})

	t.Run("unhealthy_nodes_do_not_affect_resource_details.json", func() {
		// launch a compile, get the resource config, then mark half the primary group's nodes as unhealthy
		// launch a second compile and ensure the results are the same
		var runCompileForResourceDetails func(string) *ResourceDetails
		runCompileForResourceDetails = func(jobName string) *ResourceDetails {
			wsjob := newWsCompileJob(wscommon.Namespace, jobName, 0)
			require.NoError(t.T(), k8sClient.Create(ctx, wsjob))
			defer assertWsjobCleanedUp(t.T(), wsjob)

			cm := requireClusterDetailsCreatedEventually(t.T(), jobName, wsjob.Namespace)
			rd := &ResourceDetails{}
			err := json.Unmarshal([]byte(cm.Data[wsapisv1.WsJobResourceDetailsName]), rd)
			require.NoError(t.T(), err)
			rdv2 := &ResourceDetailsV2{}
			err = json.Unmarshal([]byte(cm.Data[wsapisv1.WsJobResourceDetailsV2Name]), rdv2)
			require.NoError(t.T(), err)

			return rd
		}

		rd0 := runCompileForResourceDetails("wsjob-resource-details-0")

		// set half of the primary NG nodes as unhealthy
		nodes := &corev1.NodeList{}
		t.NoError(k8sClient.List(ctx, nodes, client.MatchingLabels{
			wscommon.GroupLabelKey:               rd0.LargestMemoryPrimaryNodeGroup.Name,
			fmt.Sprintf(RoleLabelKeyF, "memory"): "",
		}))
		numNodes := len(nodes.Items)
		t.Greater(numNodes, 1)
		for n := 0; n < numNodes/2; n++ {
			node := nodes.Items[n]
			nodeptr := &node
			nodeptr.Spec.Unschedulable = true
			t.CfgMgr.UpdateNode(nodeptr)
		}
		defer func() {
			for n := 0; n < numNodes/2; n++ {
				t.CfgMgr.UpdateNode(&nodes.Items[n])
			}
		}()
		t.Require().Eventually(func() bool {
			return t.ResourceController.Cfg.Resources.Filter(
				wsresource.NewSchedulableFilter()).Size() < t.ResourceController.Cfg.Resources.Size()
		}, eventuallyTimeout, eventuallyTick)

		// ensure that healthy/unhealthy results are the same
		rd1 := runCompileForResourceDetails("wsjob-resource-details-1")
		assert.Equal(t.T(), rd0.LargestMemoryPrimaryNodeGroup, rd1.LargestMemoryPrimaryNodeGroup)
	})

	t.Run("resource_details_created_for_compile_job_v2_network_operator_generated_cluster_details", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 1, 12, 4, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))

		job0 := newWsCompileJob(wscommon.Namespace, "v2-resource-details", 0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		defer assertWsjobCleanedUp(t.T(), job0)

		cm := requireClusterDetailsCreatedEventually(t.T(), job0.Name, job0.Namespace)
		t.T().Logf("resourceDetails: %s", cm.Data[wsapisv1.WsJobResourceDetailsName])
		rd := &ResourceDetails{}
		err := json.Unmarshal([]byte(cm.Data[wsapisv1.WsJobResourceDetailsName]), rd)
		require.NoError(t.T(), err)
		rdv2 := &ResourceDetailsV2{}
		err = json.Unmarshal([]byte(cm.Data[wsapisv1.WsJobResourceDetailsV2Name]), rdv2)
		require.NoError(t.T(), err)

		findTierByType := func(tiers []*ActivationNodeTier, tierType string) *ActivationNodeTier {
			for _, tier := range tiers {
				if tier.Type == tierType {
					return tier
				}
			}
			return nil
		}

		assert.Greater(t.T(), rd.LargestMemoryPrimaryNodeGroup.MemoryxEffectiveMemoryBytes, int64(0))
		assert.Greater(t.T(), rd.LargestMemoryPrimaryNodeGroup.MemoryxMemoryBytes, int64(0))
		assert.Equal(t.T(), memxPrimaryCount, rd.LargestMemoryPrimaryNodeGroup.MemoryxCount)

		assert.Greater(t.T(), rd.ActivationNode.ActivationCountPerCsx, 0)
		assert.Greater(t.T(), rd.ActivationNode.ActivationEffectiveCountPerCsx, 0)
		assert.Greater(t.T(), rd.ActivationNode.ActivationMemoryBytes, int64(0))
		assert.Greater(t.T(), rd.ActivationNode.ActivationEffectiveMemoryBytes, int64(0))
		assert.Greater(t.T(), rd.ActivationNode.NicBandwidthBps, int64(0))
		assert.Greater(t.T(), rd.ActivationNode.ActivationBandwidthBps, int64(0))
		assert.Greater(t.T(), rd.ActivationNode.NicEffectiveBandwidthBps, int64(0))
		assert.Greater(t.T(), rd.ActivationNode.ActivationEffectiveBandwidthBps, int64(0))
		assert.Equal(t.T(), int64(nicCountPerAx), rd.ActivationNode.NicCount)
		assert.Equal(t.T(), int64(nicCountPerAx), rd.ActivationNode.NicEffectiveCount)

		// Setting the secondary nodegroup in V2 networking clusters only for backward compatibility
		// TODO: In rel-2.5, set SmallestMemorySecondaryNodeGroup only in V1 networking clusters
		assert.Greater(t.T(), rd.SmallestMemorySecondaryNodeGroup.MemoryxEffectiveMemoryBytes, int64(0))
		assert.Greater(t.T(), rd.SmallestMemorySecondaryNodeGroup.MemoryxMemoryBytes, int64(0))
		assert.Equal(t.T(), memxSecondaryV1Count, rd.SmallestMemorySecondaryNodeGroup.MemoryxCount)

		assert.Equal(t.T(), 1, len(rdv2.ActivationNodes))
		regularTier := findTierByType(rdv2.ActivationNodes, "regular")
		assert.Equal(t.T(), 4, regularTier.Count)
		assert.Greater(t.T(), regularTier.ActivationCountPerCsx, 0)
		assert.Greater(t.T(), regularTier.ActivationEffectiveCountPerCsx, 0)
		assert.Greater(t.T(), regularTier.ActivationMemoryBytes, int64(0))
		assert.Greater(t.T(), regularTier.ActivationEffectiveMemoryBytes, int64(0))
		assert.Greater(t.T(), regularTier.NicBandwidthBps, int64(0))
		assert.Greater(t.T(), regularTier.NicEffectiveBandwidthBps, int64(0))
		assert.Equal(t.T(), int64(nicCountPerAx), regularTier.NicCount)
		assert.Equal(t.T(), int64(nicCountPerAx), regularTier.NicEffectiveCount)

		pods := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, "", 1)
		for _, pod := range pods.Items {
			foundResourceDetailsEnvVar := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.WsJobResourceDetailsFPEnv {
					foundResourceDetailsEnvVar = true
				}
			}
			assert.True(t.T(), foundResourceDetailsEnvVar)
		}

		// cluster configured to expect 2x the default memory
		axMemoryBytes = wscommon.DefaultNodeMem << 21
		job1 := newWsCompileJob(wscommon.Namespace, "v2-resource-details-fail", 0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job1))
		defer assertWsjobCleanedUp(t.T(), job1)

		job1 = assertWsJobFailEventually(t.T(), job1.Namespace, job1.Name)
		failMsg := job1.Status.Conditions[len(job1.Status.Conditions)-1].Message
		assert.Contains(t.T(), failMsg, "invalid cluster resource configuration")
		// reset axMemoryBytes
		axMemoryBytes = wscommon.DefaultNodeMem << 20

		// remove nodegroups from session and should see the resource details be skipped
		nsr := t.createNsIfNotExists(wscommon.Namespace)
		nsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:     namespacev1.DebugRemoveMode,
				NodegroupNames: []string{"group0", "group1", "group2", "group3"},
			},
			RequestUid: uuid.NewString(),
		}
		t.Require().NoError(k8sClient.Update(t.Ctx, nsr))
		t.ensureNsUpdateSuccess(nsr)

		job2 := newWsCompileJob(wscommon.Namespace, "v2-resource-details-skip", 0)
		require.NoError(t.T(), k8sClient.Create(context.Background(), job2))
		defer assertWsjobCleanedUp(t.T(), job2)

		cm = requireClusterDetailsCreatedEventually(t.T(), job2.Name, job2.Namespace)
		_, ok := cm.Data[wsapisv1.WsJobResourceDetailsName]
		require.False(t.T(), ok)
		_, ok = cm.Data[wsapisv1.WsJobResourceDetailsV2Name]
		require.False(t.T(), ok)
	})

	t.Run("resource_details_created_for_compile_job_v2_network_server_generated_cluster_details", func() {
		deleteNodes(t.T())
		v2Cfg := wscommon.NewTestClusterConfigSchema(4, 1, 12, 4, 0, workersPerGroup, memoryXPerGroup).
			SplitNStamps(1, 4)
		t.NoError(wscommon.CreateK8sNodes(v2Cfg, t.JobReconciler.Client))
		t.NoError(t.CfgMgr.Initialize(v2Cfg))

		job0 := newWsCompileJob(wscommon.Namespace, "v2-resource-details-server-generated", 0)
		job0.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()
		require.NoError(t.T(), k8sClient.Create(context.Background(), job0))
		defer assertWsjobCleanedUp(t.T(), job0)

		cdConfig := &commonpb.ClusterDetails{
			Tasks: []*commonpb.ClusterDetails_TaskInfo{
				{
					TaskType: commonpb.ClusterDetails_TaskInfo_CRD,
					TaskMap: []*commonpb.ClusterDetails_TaskInfo_TaskMap{
						{
							TaskId:      &commonpb.ClusterDetails_TaskInfo_TaskMap_TaskId{TaskId: 0, WseIds: []int32{-1}},
							AddressBook: &commonpb.ClusterDetails_TaskInfo_TaskMap_AddressBook{TaskIpPort: "127.0.0.1:9000"},
						},
					},
				},
			},
		}
		options := protojson.MarshalOptions{EmitUnpopulated: true}
		serializedClusterDetails, _ := options.Marshal(cdConfig)
		compressedCd, _ := wscommon.Gzip(serializedClusterDetails)
		clusterDetailsConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, job0.Name),
				Namespace: job0.Namespace,
				Labels: map[string]string{
					"job-name":             job0.Name,
					wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeClusterDetails,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         "jobs.cerebras.com/v1",
					BlockOwnerDeletion: wscommon.Pointer(true),
					Controller:         wscommon.Pointer(true),
					Kind:               "WSJob",
					Name:               job0.Name,
					UID:                job0.ObjectMeta.UID,
				}},
			},
			Data: map[string]string{
				wsapisv1.ClusterDetailsConfigUpdatedSignal: wsapisv1.InitSignalVal,
			},
			BinaryData: map[string][]byte{
				wsapisv1.ClusterDetailsConfigCompressedFileName: compressedCd,
			},
		}
		err := k8sClient.Create(t.Ctx, clusterDetailsConfigMap)
		require.NoError(t.T(), err)

		pods := assertPodCountEventually(t.T(), job0.Namespace, job0.Name, "", 1)
		for _, pod := range pods.Items {
			foundResourceDetailsEnvVar := false
			for _, env := range pod.Spec.Containers[0].Env {
				if env.Name == wsapisv1.WsJobResourceDetailsFPEnv {
					foundResourceDetailsEnvVar = true
				}
			}
			assert.True(t.T(), foundResourceDetailsEnvVar)

			pod.Status = corev1.PodStatus{
				Phase: corev1.PodPending,
				PodIP: "127.0.0.1",
				InitContainerStatuses: []corev1.ContainerStatus{{
					State: corev1.ContainerState{
						Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()},
					},
				}},
				StartTime: &metav1.Time{
					Time: time.Now(),
				},
			}
			require.NoError(t.T(), k8sClient.Status().Update(t.Ctx, &pod))
		}

		require.Eventually(t.T(), func() bool {
			cm := requireClusterDetailsCreatedEventually(t.T(), job0.Name, job0.Namespace)
			rd := &ResourceDetails{}
			err1 := json.Unmarshal([]byte(cm.Data[wsapisv1.WsJobResourceDetailsName]), rd)
			rdv2 := &ResourceDetailsV2{}
			err2 := json.Unmarshal([]byte(cm.Data[wsapisv1.WsJobResourceDetailsV2Name]), rdv2)
			return err1 == nil && err2 == nil
		}, eventuallyTimeout, eventuallyTick)
	})
}

//go:build scheduler_simulation

package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	k8syaml "sigs.k8s.io/yaml"

	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonv1 "github.com/kubeflow/common/pkg/apis/common/v1"

	namespacev1 "cerebras.com/job-operator/apis/namespace/v1"
	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wscommon "cerebras.com/job-operator/common"
)

func (t *TestSuite) TestSchedulerSimulation() {
	// (OPTIONAL) Build-only Mode
	// Try to read the variable "CB_ISS_BUILD_ONLY"
	_, buildOnly := os.LookupEnv("CB_ISS_BUILD_ONLY")
	if buildOnly {
		t.T().Logf("CB_ISS_BUILD_ONLY is set, running in build-only mode")
		// If build-only mode is set, skip the test
		t.T().Skip("Skipping test in build-only mode")
	} else {
		t.T().Logf("CB_ISS_BUILD_ONLY is NOT set, running the test")
	}

	// (OPTIONAL) Debug Mode
	// Try to read the variable "CB_ISS_DEBUG"
	_, debugMode := os.LookupEnv("CB_ISS_DEBUG")
	if debugMode {
		t.T().Logf("CB_ISS_DEBUG is set, debug mode enabled")
	} else {
		t.T().Logf("CB_ISS_DEBUG is NOT set, debug mode is disabled")
	}

	wsapisv1.IsV2Network = true
	wsapisv1.UseAxScheduling = true
	defer func() {
		wsapisv1.IsV2Network = false
		wsapisv1.UseAxScheduling = false
		wsapisv1.AxCountPerSystem = 0
	}()

	createInferenceConfigMap := func(execJob *wsapisv1.WSJob, testDataFileName string) {
		clusterDetailsConfigMap := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, execJob.Name),
				Namespace: execJob.Namespace,
			},
		}
		cdConfig := &commonpb.ClusterDetails{}
		options := protojson.UnmarshalOptions{
			AllowPartial:   true,
			DiscardUnknown: true,
		}

		// expect cluster details config not to exist in the first few seconds
		require.Never(t.T(), func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
			return !apierrors.IsNotFound(err)
		}, 3*time.Second, eventuallyTick)

		// simulating how server would generate the configmap
		require.Eventually(t.T(), func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(execJob), execJob)
			return err == nil
		}, eventuallyTimeout, eventuallyTick)

		// simulate cluster server creating the config
		clusterDetailsData, err := ioutil.ReadFile(testDataFileName)
		require.NoError(t.T(), err)

		clusterDetailsConfigMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf(wsapisv1.ClusterDetailsConfigName, execJob.Name),
				Namespace: execJob.Namespace,
				Labels: map[string]string{
					"job-name":             execJob.Name,
					wsapisv1.ConfigMapType: wsapisv1.ConfigMapTypeClusterDetails,
				},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion:         "jobs.cerebras.com/v1",
					BlockOwnerDeletion: wscommon.Pointer(true),
					Controller:         wscommon.Pointer(true),
					Kind:               "WSJob",
					Name:               execJob.Name,
					UID:                execJob.ObjectMeta.UID,
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

		var kvssToCsxDomains []string
		var actToCsxDomains []string
		var swDriverToCsxDomains []string
		appendAllWioTaskMaps := func(taskInfo *commonpb.ClusterDetails_TaskInfo, targetList *[]string) {
			for _, taskMap := range taskInfo.TaskMap {
				*targetList = append(*targetList, wscommon.SerializeWioTaskMap(taskMap))
			}
		}
		for _, taskInfo := range cdConfig.Tasks {
			if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_KVSS {
				appendAllWioTaskMaps(taskInfo, &kvssToCsxDomains)
			} else if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_ACT {
				appendAllWioTaskMaps(taskInfo, &actToCsxDomains)
			} else if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_SWD {
				appendAllWioTaskMaps(taskInfo, &swDriverToCsxDomains)
			}
		}

		compressed, err = wscommon.Gzip([]byte(strings.Join(actToCsxDomains, ",")))
		require.NoError(t.T(), err)
		clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsActToCsxDomains] = compressed

		if len(kvssToCsxDomains) != 0 {
			compressed, err = wscommon.Gzip([]byte(strings.Join(kvssToCsxDomains, ",")))
			require.NoError(t.T(), err)
			clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsKvssToCsxDomains] = compressed
		}

		if len(swDriverToCsxDomains) != 0 {
			compressed, err = wscommon.Gzip([]byte(strings.Join(swDriverToCsxDomains, ",")))
			require.NoError(t.T(), err)
			clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsSWDriverToCsxDomains] = compressed
		}

		require.NoError(t.T(), k8sClient.Create(context.Background(), clusterDetailsConfigMap))

		clusterDetailsConfigMapJson, err := json.MarshalIndent(clusterDetailsConfigMap, "", "    ")
		require.NoError(t.T(), err, "Failed to marshal clusterDetailsConfigMap to JSON")
		t.T().Logf("clusterDetailsConfigMap:\n%s", clusterDetailsConfigMapJson)

		// cluster details config generated
		require.Eventually(t.T(), func() bool {
			err := k8sClient.Get(context.Background(), client.ObjectKeyFromObject(clusterDetailsConfigMap), clusterDetailsConfigMap)
			return err == nil
		}, eventuallyTimeout, eventuallyTick)
	}

	assertInferenceJobSharing := func(lock *rlv1.ResourceLock, shouldEnable bool) {
		annot := lock.GetAnnotations()

		// If no annotations, treat as disabled
		enabledInLock := false
		if annot != nil {
			enabledInLock = annot[wsapisv1.InferenceJobSharingAnnotKey] == strconv.FormatBool(true)
		}

		assert.True(t.T(), enabledInLock == shouldEnable)
	}

	getCDTaskMapsByType := func(clusterDetails *commonpb.ClusterDetails, taskType commonpb.ClusterDetails_TaskInfo_TaskType) []*commonpb.ClusterDetails_TaskInfo_TaskMap {
		tasks := wscommon.Filter(func(detail *commonpb.ClusterDetails_TaskInfo) bool {
			return detail.TaskType == taskType
		}, clusterDetails.Tasks)
		var taskMaps []*commonpb.ClusterDetails_TaskInfo_TaskMap
		for _, task := range tasks {
			taskMaps = append(taskMaps, task.TaskMap...)
		}
		return taskMaps
	}

	getCDTaskResourceInfoByType := func(clusterDetails *commonpb.ClusterDetails, taskType commonpb.ClusterDetails_TaskInfo_TaskType) *commonpb.ResourceInfo {
		tasks := wscommon.Filter(func(detail *commonpb.ClusterDetails_TaskInfo) bool {
			return detail.TaskType == taskType
		}, clusterDetails.Tasks)
		require.Len(t.T(), tasks, 1, "Expected exactly one task of type %s in cluster details", taskType)

		taskResourceInfo := tasks[0].ResourceInfo
		return taskResourceInfo
	}

	createNewInferenceJobByClusterDetails := func(ns, name string, clusterDetails *commonpb.ClusterDetails, numSys uint32, replicaTypeCount *map[commonv1.ReplicaType]int) *wsapisv1.WSJob {
		job := newWsJobTemplate(ns, name, numSys)

		delete(job.Spec.WSReplicaSpecs, wsapisv1.WSReplicaTypeCommand)
		delete(job.Spec.WSReplicaSpecs, wsapisv1.WSReplicaTypeWeight)
		delete(job.Spec.WSReplicaSpecs, wsapisv1.WSReplicaTypeWorker)
		delete(job.Spec.WSReplicaSpecs, wsapisv1.WSReplicaTypeBroadcastReduce)

		job.Labels[wsapisv1.JobTypeLabelKey] = wsapisv1.JobTypeExecute
		job.Labels[wsapisv1.JobModeLabelKey] = wsapisv1.JobModeInference
		job.Annotations[wsapisv1.EnablePartialSystemKey] = "false"
		job.Annotations[wsapisv1.SemanticClusterServerVersion] = wsapisv1.SoftwareFeatureSemverMap[wsapisv1.ParallelExperts].String()

		// TODO: Remove the following actToCsxDomains propagation, as
		// it should be handled by createInferenceConfigMap
		var actToCsxDomains [][]int
		for _, taskInfo := range clusterDetails.Tasks {
			if taskInfo.TaskType == commonpb.ClusterDetails_TaskInfo_ACT {
				for _, taskMap := range taskInfo.TaskMap {
					domainsRaw := taskMap.AddressBook.WseDomains
					domains := make([]int, len(domainsRaw))
					for i, domain := range domainsRaw {
						domains[i] = int(domain)
					}
					actToCsxDomains = append(actToCsxDomains, domains)
				}
			}
		}
		job.Spec.ActToCsxDomains = actToCsxDomains

		applyClusterDetailsToJobSpec := func(spec *commonv1.ReplicaSpec, clusterDetails *commonpb.ClusterDetails, taskType commonpb.ClusterDetails_TaskInfo_TaskType) {
			// A proper Cluster Details should have all resources set
			resourceInfo := getCDTaskResourceInfoByType(clusterDetails, taskType)

			// Apply Template.Annotations

			if spec.Template.Annotations == nil {
				spec.Template.Annotations = make(map[string]string)
			}

			if resourceInfo.MemoryBytes > 0 {
				spec.Template.Annotations[wsapisv1.PodMemoryBaseKey] = strconv.FormatInt(resourceInfo.MemoryBytes, 10)
			}

			if resourceInfo.MemoryBytesUpperBound > 0 {
				spec.Template.Annotations[wsapisv1.PodMemoryUpperBoundKey] = strconv.FormatInt(resourceInfo.MemoryBytesUpperBound, 10)
			}

			if resourceInfo.MemoryBytesMaxLimit > 0 {
				spec.Template.Annotations[wsapisv1.PodMemoryMaxLimitKey] = strconv.FormatInt(resourceInfo.MemoryBytesMaxLimit, 10)
			}

			// Apply Resources

			for idx, _ := range spec.Template.Spec.Containers {
				container := &spec.Template.Spec.Containers[idx]

				t.T().Logf("Applying resources for container %s", taskType)
				if container.Resources.Requests == nil {
					container.Resources.Requests = make(corev1.ResourceList)
				}
				if resourceInfo.MemoryBytes > 0 {
					quantity := *resource.NewQuantity(resourceInfo.MemoryBytes, resource.BinarySI)
					if taskType == commonpb.ClusterDetails_TaskInfo_CRD {
						container.Resources.Limits[corev1.ResourceMemory] = quantity
					} else {
						container.Resources.Requests[corev1.ResourceMemory] = quantity
					}
				}

				container.Resources.Requests[corev1.ResourceCPU] = *resource.NewMilliQuantity(resourceInfo.CpuMillicore, resource.DecimalSI)
			}
		}

		for rt, count := range *replicaTypeCount {
			if count == 0 {
				continue
			}

			job.Spec.WSReplicaSpecs[rt] = wsjobContainerSpec(ns, rt, numSys)
			job.Spec.WSReplicaSpecs[rt].Replicas = int32Ptr(count)
			applyClusterDetailsToJobSpec(job.Spec.WSReplicaSpecs[rt], clusterDetails, wsapisv1.ClusterDetailsTaskTypeMapping[rt.Lower()])
		}

		return job
	}

	getJobPodsEventually := func(ns, jobName string) *corev1.PodList {
		var pods *corev1.PodList
		require.Eventually(t.T(), func() bool {
			pods = getJobPods(t.T(), ns, jobName, "")
			return true
		}, 3*eventuallyTimeout, eventuallyTick)
		return pods
	}

	getClusterSchemaFromCMFile := func(filePath string) *wscommon.ClusterSchema {
		// Read the cluster schema file
		k8sConfigMapYaml, err := ioutil.ReadFile(filePath)
		require.NoError(t.T(), err, "Failed to read k8s config map file")
		if debugMode {
			t.T().Logf("K8S Config Map YAML:\n%s", k8sConfigMapYaml)
		}

		k8sConfigMap := &corev1.ConfigMap{}
		err = k8syaml.Unmarshal(k8sConfigMapYaml, k8sConfigMap)
		require.NoError(t.T(), err, "Failed to unmarshal k8s config map")
		clusterSchema, err := wscommon.NewClusterSchemaFromCM(k8sConfigMap)
		require.NoError(t.T(), err, "Failed to create cluster schema from k8s config map")

		// HACK: Remove nodes that have role RoleInferenceDriver assigned
		// TODO: Remove the hack
		newNodeList := []*wscommon.Node{}
		for _, node := range clusterSchema.Nodes {
			if !node.HasRole(wscommon.RoleInferenceDriver) {
				newNodeList = append(newNodeList, node)
			} else {
				t.T().Logf("HACK: Node %s has role %s assigned, which is not supported in simulation mode. Removing it from the cluster schema.", node.Name, wscommon.RoleInferenceDriver.String())
			}
		}
		clusterSchema.Nodes = newNodeList

		return &clusterSchema
	}

	createK8sNode := func(node *wscommon.Node, rawK8sNode *corev1.Node) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: node.Name,
				Labels: map[string]string{
					wscommon.GroupLabelKey:  node.GetV2Group(),
					node.Role.AsNodeLabel(): "",
				},
			},
			Status: corev1.NodeStatus{
				Addresses:   rawK8sNode.Status.Addresses,
				Capacity:    rawK8sNode.Status.Capacity,
				Allocatable: rawK8sNode.Status.Allocatable,
			},
		}
	}

	createK8sNodesFromReference := func(cfg *wscommon.ClusterSchema, rawK8sNodes *corev1.NodeList, apiClient client.Client) (*corev1.NodeList, error) {
		// Track the ndoes that are in the raw NodeList
		nameToRawK8sNode := map[string]*corev1.Node{}
		for nodeIdx := range rawK8sNodes.Items {
			rawK8sNode := &rawK8sNodes.Items[nodeIdx]
			nameToRawK8sNode[rawK8sNode.ObjectMeta.Name] = rawK8sNode
		}

		// Track which nodes are added to the new NodeList
		nodeAdded := map[string]bool{}

		nodeList := &corev1.NodeList{}
		for _, node := range cfg.Nodes {
			if debugMode {
				nodeJson, err := json.MarshalIndent(node, "", "    ")
				require.NoError(t.T(), err, "Failed to marshal node to JSON")
				t.T().Logf("Creating k8s node for node:\n%s", nodeJson)
			}

			k8Node := createK8sNode(node, nameToRawK8sNode[node.Name])
			nodeList.Items = append(nodeList.Items, *k8Node)

			if debugMode {
				k8NodeJson, err := json.MarshalIndent(k8Node, "", "    ")
				require.NoError(t.T(), err, "Failed to marshal k8s node to JSON")
				t.T().Logf("Creating k8s node:\n%s", k8NodeJson)
			}

			err := apiClient.Create(context.TODO(), k8Node)
			if err != nil && !apierrors.IsAlreadyExists(err) {
				return nil, err
			}

			nodeAdded[node.Name] = true
		}

		for rawK8sNodeName, _ := range nameToRawK8sNode {
			if _, ok := nodeAdded[rawK8sNodeName]; !ok {
				t.T().Logf("WARNING: K8s Node %s (from raw K8sNodeList) is not found in ClusterSchema, skipping it in the new K8sNodeList as well", rawK8sNodeName)
			}
		}
		return nodeList, nil
	}

	convertClusterSchemaToK8sConfigMap := func(clusterSchema *wscommon.ClusterSchema) *corev1.ConfigMap {
		clusterSchemaYaml, err := k8syaml.Marshal(clusterSchema)
		require.NoError(t.T(), err, "Failed to marshal cluster schema to YAML")
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				ResourceVersion: clusterSchema.ResourceVersion,
			},
			Data: map[string]string{
				wscommon.ClusterConfigFile: string(clusterSchemaYaml),
			},
		}
		return cm
	}

	reserveNamespaceFromReference := func(nsrReference *namespacev1.NamespaceReservation, clusterSchema *wscommon.ClusterSchema) string {
		namespaceName := nsrReference.ObjectMeta.Name
		nsr := t.createNsIfNotExists(namespaceName)

		requestNodeNames := nsrReference.Status.Nodes
		requestNodeNamesSet := map[string]bool{}
		for _, nodeName := range requestNodeNames {
			requestNodeNamesSet[nodeName] = true
		}

		// Get AX, Mgmt, Crd and IX node names from the nsrReference
		requestActivationNodeNames := []string{}
		requestCoordinatorNodeNames := []string{}
		requestManagementNodeNames := []string{}
		requestInferenceDriverNodeNames := []string{}

		// Loop through all nodes
		for _, node := range clusterSchema.Nodes {
			if _, ok := requestNodeNamesSet[node.Name]; ok {
				if node.HasRole(wscommon.RoleActivation) {
					requestActivationNodeNames = append(requestActivationNodeNames, node.Name)
				}
				if node.HasRole(wscommon.RoleManagement) {
					requestManagementNodeNames = append(requestManagementNodeNames, node.Name)
				}
				if node.HasRole(wscommon.RoleCoordinator) {
					requestCoordinatorNodeNames = append(requestCoordinatorNodeNames, node.Name)
				}
				if node.HasRole(wscommon.RoleInferenceDriver) {
					requestInferenceDriverNodeNames = append(requestInferenceDriverNodeNames, node.Name)
				}
			}
		}

		requestSystemNames := nsrReference.Status.Systems
		nsr.Spec.ReservationRequest = namespacev1.ReservationRequest{
			RequestParams: namespacev1.RequestParams{
				UpdateMode:               namespacev1.DebugSetMode,
				SystemNames:              requestSystemNames,
				ActivationNodeNames:      requestActivationNodeNames,
				CoordinatorNodeNames:     requestCoordinatorNodeNames,
				ManagementNodeNames:      requestManagementNodeNames,
				InferenceDriverNodeNames: requestInferenceDriverNodeNames,
				ParallelExecuteCount:     wscommon.Pointer(nsrReference.Status.CurrentResourceSpec.ParallelExecuteCount),
			},
			RequestUid: uuid.NewString(),
		}

		// Copy labels
		nsr.Labels = wscommon.CopyMap(nsrReference.Labels)

		if debugMode {
			nsrJson, err := json.MarshalIndent(nsr, "", "    ")
			require.NoError(t.T(), err, "Failed to marshal request nsr to JSON")
			t.T().Logf("Request NSR JSON:\n%s", nsrJson)
		}

		// Send out the request
		t.Require().NoError(k8sClient.Update(t.Ctx, nsr))
		t.ensureNsUpdateSuccess(nsr)

		return namespaceName
	}

	t.Run("inference_scheduling_simulator", func() {
		deleteNodes(t.T())

		// env set up //
		// clusterDetailsFile
		// (REQUIRED) Get the cluster details file from environment variable
		clusterDetailsFilesStr := os.Getenv("CB_ISS_CLUSTER_DETAILS_FILES")
		// assert clusterDetailsFilesStr is not empty
		assert.NotEmpty(t.T(), clusterDetailsFilesStr, "CB_ISS_CLUSTER_DETAILS_FILES environment variable must be set")
		// print clusterDetailsFile
		t.T().Logf("Cluster details files (CB_ISS_CLUSTER_DETAILS_FILES): %s", clusterDetailsFilesStr)
		clusterDetailsFiles := strings.Split(clusterDetailsFilesStr, ":")
		for jobIdx, clusterDetailsFile := range clusterDetailsFiles {
			t.T().Logf("\t[Job %d] Cluster details file: %s", jobIdx, clusterDetailsFile)
		}

		// (OPTIONAL) Get the k8s config map file from environment variable
		clusterK8sConfigMapFile := os.Getenv("CB_ISS_CLUSTER_K8S_CONFIGMAP_FILE")
		t.T().Logf("Cluster k8s configmap file (CB_ISS_CLUSTER_K8S_CONFIGMAP_FILE): %s", clusterK8sConfigMapFile)
		// (OPTIONAL) Get the k8s nodelist file from environment variable
		clusterK8sNodeListFile := os.Getenv("CB_ISS_CLUSTER_K8S_NODELIST_FILE")
		t.T().Logf("Cluster k8s nodelist file (CB_ISS_CLUSTER_K8S_NODELIST_FILE): %s", clusterK8sNodeListFile)
		// Either both are set or none are set
		bothSetOrNoneSet := (len(clusterK8sConfigMapFile) > 0) == (len(clusterK8sNodeListFile) > 0)
		assert.True(t.T(), bothSetOrNoneSet, "Either both CB_ISS_CLUSTER_K8S_CONFIGMAP_FILE and CB_ISS_CLUSTER_K8S_NODELIST_FILE must be set or none of them should be set")
		// (OPTIONAL) Get the nsr file from environment variable
		clusterNsrFile := os.Getenv("CB_ISS_CLUSTER_NSR_FILE")
		t.T().Logf("Cluster nsr file (CB_ISS_CLUSTER_NSR_FILE): %s", clusterNsrFile)
		assert.True(t.T(), len(clusterNsrFile) == 0 || (len(clusterK8sConfigMapFile) > 0 && len(clusterNsrFile) > 0),
			"CB_ISS_CLUSTER_NSR_FILE must be set only if both CB_ISS_CLUSTER_K8S_CONFIGMAP_FILE and CB_ISS_CLUSTER_K8S_NODELIST_FILE are set")

		// (REQUIRED) Get the simulation result dir from environment variable
		simulationResultDir := os.Getenv("CB_ISS_RESULT_DIR")
		// assert simulationResultDir is not empty
		assert.NotEmpty(t.T(), simulationResultDir, "CB_ISS_RESULT_DIR environment variable must be set")
		// print simulationResultDir
		t.T().Logf("Simulation output file (CB_ISS_RESULT_DIR): %s", simulationResultDir)
		err := os.MkdirAll(simulationResultDir, os.ModePerm)
		require.NoError(t.T(), err, "Failed to create simulation result directory")

		// Set up cluster //

		// Figure out the total number of systems and management nodes required
		totalSystemsRequested := 0
		totalCrdsRequested := 0
		t.T().Logf("Processing cluster details files to determine total systems and CRDs requested")
		for jobIdx, clusterDetailsFile := range clusterDetailsFiles {
			clusterDetailsJson, err := ioutil.ReadFile(clusterDetailsFile)
			require.NoError(t.T(), err)
			clusterDetails := &commonpb.ClusterDetails{}
			options := protojson.UnmarshalOptions{
				AllowPartial:   true,
				DiscardUnknown: true,
			}
			err = options.Unmarshal(clusterDetailsJson, clusterDetails)
			require.NoError(t.T(), err)

			numSys := len(getCDTaskMapsByType(clusterDetails, commonpb.ClusterDetails_TaskInfo_WSE))
			numCrd := len(getCDTaskMapsByType(clusterDetails, commonpb.ClusterDetails_TaskInfo_CRD))
			t.T().Logf("\t[Job %d] requests for numSys: %d, numCrd: %d", jobIdx, numSys, numCrd)

			totalSystemsRequested += numSys
			totalCrdsRequested += numCrd
		}
		t.T().Logf("Total systems requested: %d, total CRDs requested: %d", totalSystemsRequested, totalCrdsRequested)

		var prunedK8sNodeList *corev1.NodeList
		var prunedK8sConfigMapYaml []byte
		namespace := wscommon.Namespace
		expectedJobSharingEnabled := false
		// Set up the cluster config
		if len(clusterK8sConfigMapFile) > 0 {
			// Create the cluster config based on the cluster schema file and cluster k8s nodes file

			// Load the cluster schema from file
			clusterSchema := getClusterSchemaFromCMFile(clusterK8sConfigMapFile)
			if debugMode {
				clusterSchemaJson, err := json.MarshalIndent(clusterSchema, "", "    ")
				require.NoError(t.T(), err, "Failed to marshal cluster k8s configmap to JSON")
				t.T().Logf("Cluster Schema JSON:\n%s", clusterSchemaJson)
			}
			// Marshal it into bytes to avoid being modified by CfgMgr
			prunedK8sConfigMapYaml_, err := k8syaml.Marshal(convertClusterSchemaToK8sConfigMap(clusterSchema))
			require.NoError(t.T(), err, "Failed to marshal pruned k8s configmap to YAML")
			prunedK8sConfigMapYaml = prunedK8sConfigMapYaml_

			// Read the k8s nodes file
			k8sNodeListYaml, err := ioutil.ReadFile(clusterK8sNodeListFile)
			require.NoError(t.T(), err, "Failed to read cluster k8s nodelist file")
			k8sNodeList := &corev1.NodeList{}
			require.NoError(t.T(), k8syaml.Unmarshal(k8sNodeListYaml, k8sNodeList), "Failed to unmarshal k8s nodelist")

			// Set up the k8s nodes
			prunedK8sNodeList, err = createK8sNodesFromReference(clusterSchema, k8sNodeList, t.JobReconciler.Client)
			require.NoError(t.T(), err)

			// Load the cluster schema
			t.NoError(t.CfgMgr.Initialize(*clusterSchema))

			// Read the NSR file if provided

			// Replay the reference namespace reservation
			if len(clusterNsrFile) > 0 {
				nsrYaml, err := ioutil.ReadFile(clusterNsrFile)
				require.NoError(t.T(), err, "Failed to read cluster nsr file")
				refNsr := &namespacev1.NamespaceReservation{}
				require.NoError(t.T(), k8syaml.Unmarshal(nsrYaml, refNsr), "Failed to unmarshal nsr")
				if debugMode {
					refNsrJson, err := json.MarshalIndent(refNsr, "", "    ")
					require.NoError(t.T(), err, "Failed to marshal nsr to JSON")
					t.T().Logf("NSR JSON:\n%s", refNsrJson)
				}

				t.unassignEverythingFromNS(namespace)
				namespace = reserveNamespaceFromReference(refNsr, clusterSchema)
				t.T().Logf("Actively using namespace %s", namespace)

				nsr := t.createNsIfNotExists(namespace)
				if nsr.Labels != nil {
					if val, ok := nsr.Labels[wscommon.InferenceJobSharingSessionLabelKey]; ok && val == "true" {
						expectedJobSharingEnabled = true
					}
				}

				assertClusterServerIngressSetup(t.T(), namespace)
				defer assertClusterServerIngressCleanup(t.T(), namespace)
			} else {
				// Default namespace reservation behavior

				// Turn on session level job sharing
				expectedJobSharingEnabled = true
				t.updateSessionLabel(namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
				defer func() {
					t.updateSessionLabel(namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
				}()

				// TODO: [CS-55] Looks like all resources are already reserved.
				//       Waiting for proper inference-only cluster support.
				// nsr is optional in test anyway.
				// systemCount := len(clusterSchema.Systems)
				// t.assignEverythingToNS(namespace, systemCount)

				// TODO: TRY t.createNsIfNotExists(namespaceName)
			}
		} else {
			// Create the cluster config based on default configurations

			systemCount := totalSystemsRequested
			coordCount := totalCrdsRequested
			brCount := 0
			axCount := systemCount
			ixCount := 0
			// Total num switches split across all AX nodes.
			// Each AX talks to 2 domains
			nSwitches := 4
			v2Cfg := wscommon.NewTestClusterConfigSchema(systemCount, coordCount, brCount, axCount, ixCount, workersPerGroup, memoryXPerGroup).SplitNStamps(1, nSwitches)
			k8sNodeParam := &wscommon.ClusterConfigK8sNodeParam{
				DefaultNodeCpu: 64000,      // milli
				DefaultNodeMem: 755 * 1024, // MiB
				LargeNodeMem:   wscommon.LargeNodeMem,
				XLargeNodeMem:  wscommon.XLargeNodeMem,
			}
			if debugMode {
				clusterSchemaJson, err := json.MarshalIndent(v2Cfg, "", "    ")
				require.NoError(t.T(), err, "Failed to marshal cluster schema to JSON")
				t.T().Logf("Cluster Schema JSON:\n%s", clusterSchemaJson)
			}
			// Marshal it into bytes to avoid being modified by CfgMgr
			prunedK8sConfigMapYaml_, err := k8syaml.Marshal(convertClusterSchemaToK8sConfigMap(&v2Cfg))
			require.NoError(t.T(), err, "Failed to marshal pruned k8s configmap to YAML")
			prunedK8sConfigMapYaml = prunedK8sConfigMapYaml_

			prunedK8sNodeList, err = wscommon.CreateK8sNodesWithExplicitNodeParam(v2Cfg, t.JobReconciler.Client, k8sNodeParam)
			t.NoError(err)
			t.NoError(t.CfgMgr.Initialize(v2Cfg))
			t.assignEverythingToNS(namespace, systemCount)

			// Turn on session level job sharing
			expectedJobSharingEnabled = true
			t.updateSessionLabel(namespace, wscommon.InferenceJobSharingSessionLabelKey, "true")
			defer func() {
				t.updateSessionLabel(namespace, wscommon.InferenceJobSharingSessionLabelKey, "false")
			}()

			// TODO: TRY t.createNsIfNotExists(namespaceName)
		}

		// Dump pruned NamespaceReservation
		prunedNsr := t.createNsIfNotExists(namespace)
		if debugMode {
			prunedNsrJson, err := json.MarshalIndent(prunedNsr, "", "    ")
			require.NoError(t.T(), err, "Failed to marshal pruned nsr to JSON")
			t.T().Logf("Pruned NSR JSON:\n%s", prunedNsrJson)
		}
		prunedNsrYaml, err := k8syaml.Marshal(prunedNsr)
		require.NoError(t.T(), err, "Failed to marshal pruned nsr to YAML")
		prunedNsrOutputFile := filepath.Join(simulationResultDir, "pruned_cluster_nsr.yaml")
		err = os.WriteFile(prunedNsrOutputFile, prunedNsrYaml, 0644)
		require.NoError(t.T(), err)
		t.T().Logf("Pruned cluster nsr YAML file written: %s", prunedNsrOutputFile)

		// Dump pruned ConfigMap
		prunedClusterK8sConfigMapOutputFile := filepath.Join(simulationResultDir, "pruned_cluster_k8s_configmap.yaml")
		err = os.WriteFile(prunedClusterK8sConfigMapOutputFile, prunedK8sConfigMapYaml, 0644)
		require.NoError(t.T(), err)
		t.T().Logf("Pruned cluster k8s configmap YAML file written: %s", prunedClusterK8sConfigMapOutputFile)

		// Dump pruned k8sNodeList
		prunedK8sNodeListYaml, err := k8syaml.Marshal(prunedK8sNodeList)
		require.NoError(t.T(), err, "Failed to marshal pruned k8s nodel list to YAML")
		prunedClusterK8sNodelistOutputFile := filepath.Join(simulationResultDir, "pruned_cluster_k8s_nodelist.yaml")
		err = os.WriteFile(prunedClusterK8sNodelistOutputFile, prunedK8sNodeListYaml, 0644)
		require.NoError(t.T(), err)
		t.T().Logf("Pruned cluster k8s nodelist YAML file written: %s", prunedClusterK8sNodelistOutputFile)

		// Set up inference job //

		for jobIdx, clusterDetailsFile := range clusterDetailsFiles {
			t.T().Logf("*******************************************************************************************")
			t.T().Logf("[Job %d] Simulating cluster details file: %s", jobIdx, clusterDetailsFile)

			// Parse the cluster details file
			clusterDetailsJson, err := ioutil.ReadFile(clusterDetailsFile)
			require.NoError(t.T(), err)
			clusterDetails := &commonpb.ClusterDetails{}
			options := protojson.UnmarshalOptions{
				AllowPartial:   true,
				DiscardUnknown: true,
			}
			err = options.Unmarshal(clusterDetailsJson, clusterDetails)
			require.NoError(t.T(), err)

			numSys := len(getCDTaskMapsByType(clusterDetails, commonpb.ClusterDetails_TaskInfo_WSE))
			numKvss := len(getCDTaskMapsByType(clusterDetails, commonpb.ClusterDetails_TaskInfo_KVSS))
			numAct := len(getCDTaskMapsByType(clusterDetails, commonpb.ClusterDetails_TaskInfo_ACT))
			numChf := len(getCDTaskMapsByType(clusterDetails, commonpb.ClusterDetails_TaskInfo_CHF))
			numCrd := len(getCDTaskMapsByType(clusterDetails, commonpb.ClusterDetails_TaskInfo_CRD))
			t.T().Logf("numSys: %d, numKvss: %d, numAct: %d, numChf: %d, numCrd: %d", numSys, numKvss, numAct, numChf, numCrd)

			// Create the WSJob spec
			replicaTypeCount := &map[commonv1.ReplicaType]int{
				wsapisv1.WSReplicaTypeChief:           numChf,
				wsapisv1.WSReplicaTypeActivation:      numAct,
				wsapisv1.WSReplicaTypeKVStorageServer: numKvss,
				wsapisv1.WSReplicaTypeCoordinator:     numCrd,
			}

			jobName := "sim-" + time.Now().Format("0102150405") + "-" + strconv.Itoa(jobIdx)
			inferenceJob := createNewInferenceJobByClusterDetails(namespace, jobName, clusterDetails, uint32(numSys), replicaTypeCount)
			inferenceJobJson, err := json.MarshalIndent(inferenceJob, "", "    ")
			require.NoError(t.T(), err)
			t.T().Logf("Inference WSJob JSON:\n%s", inferenceJobJson)

			// Send the WSJob spec

			require.NoError(t.T(), k8sClient.Create(context.Background(), inferenceJob))
			defer assertWsjobCleanedUp(t.T(), inferenceJob)

			// Create and send the ConfigMap
			createInferenceConfigMap(inferenceJob, clusterDetailsFile)

			// Scheduling should be triggered by now //
			lock := assertLockExists(t.T(), inferenceJob.Namespace, inferenceJob.Name)
			assertLocked(t.T(), lock)
			assertInferenceJobSharing(lock, expectedJobSharingEnabled)

			// Fetch the scheduling result //

			// Convert resource lock to JSON
			resourceLockJson, err := json.MarshalIndent(lock, "", "    ")
			require.NoError(t.T(), err)
			if debugMode {
				t.T().Logf("Resource lock JSON:\n%s", resourceLockJson)
			}
			resourceLockFileName := fmt.Sprintf("resource_lock_%d.json", jobIdx)
			resourceLockOutputFile := filepath.Join(simulationResultDir, resourceLockFileName)
			err = os.WriteFile(resourceLockOutputFile, resourceLockJson, 0644)
			require.NoError(t.T(), err)
			t.T().Logf("Resource lock JSON file written: %s", resourceLockOutputFile)

			// Fetch PODs
			pods := getJobPodsEventually(inferenceJob.Namespace, inferenceJob.Name)
			assert.True(t.T(), len(pods.Items) > 0, "Expected at least one pod to be created for job in %s", inferenceJob.Namespace)
			t.T().Logf("PODs created: %d", len(pods.Items))

			// Convert PODs to JSON
			podlistJson, err := json.MarshalIndent(pods, "", "    ")
			require.NoError(t.T(), err)
			if debugMode {
				t.T().Logf("PodList JSON:\n%s", podlistJson)
			}
			podlistFileName := fmt.Sprintf("podlist_%d.json", jobIdx)
			podlistOutputFile := filepath.Join(simulationResultDir, podlistFileName)
			err = os.WriteFile(podlistOutputFile, podlistJson, 0644)
			require.NoError(t.T(), err)
			t.T().Logf("PodList JSON file written: %s", podlistOutputFile)

			assertPodCountEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name, rlv1.ChiefRequestName, numChf)
			assertPodCountEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name, rlv1.ActivationRequestName, numAct)
			assertPodCountEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name, rlv1.CoordinatorRequestName, numCrd)
			assertPodCountEventually(t.T(), inferenceJob.Namespace, inferenceJob.Name, rlv1.KVStorageServerRequestName, numKvss)
			t.T().Logf("PodList verified: %d CHFs, %d ACTs, %d CRDs, %d KVSSs", numChf, numAct, numCrd, numKvss)

			t.T().Logf("[Job %d] Simulation completed successfully", jobIdx)
		}
	})
}

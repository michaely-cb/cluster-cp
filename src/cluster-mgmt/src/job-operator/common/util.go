/*
Copyright 2022 Cerebras Systems, Inc..

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package common

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"

	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	commonpb "cerebras/pb/workflow/appliance/common"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	kubeclientset "k8s.io/client-go/kubernetes"

	commonapisv1 "github.com/kubeflow/common/pkg/apis/common/v1"
	commonutil "github.com/kubeflow/common/pkg/util"

	rlv1 "cerebras.com/job-operator/apis/resourcelock/v1"
	systemv1 "cerebras.com/job-operator/apis/system/v1"
	wsapisv1 "cerebras.com/job-operator/apis/ws/v1"
	wsv1 "cerebras.com/job-operator/client/clientset/versioned/typed/ws/v1"
)

// FmtLogJson is intended for logging
func FmtLogJson(element any) string {
	b, err := json.Marshal(element)
	if err != nil {
		return "Tried to format JSON but failed, " + err.Error()
	}
	return strings.ReplaceAll(string(b), "\"", "'")
}

func FormatBoolToStringIfTrue(b bool) string {
	if b {
		return "true"
	}
	return ""
}

// FlatMapV returns a list containing all the elements of the given list
func FlatMapV[T any](lists ...[]T) []T {
	var rv []T
	for _, list := range lists {
		rv = append(rv, list...)
	}
	return rv
}

// FlatMap returns a list containing all the elements of the given list
func FlatMap[T any](lists [][]T) []T {
	var rv []T
	for _, list := range lists {
		rv = append(rv, list...)
	}
	return rv
}

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

func CopyMapNested[K comparable, V any](m map[K]map[K]V) map[K]map[K]V {
	if m == nil {
		return nil
	}

	clone := make(map[K]map[K]V, len(m))
	for k, v := range m {
		clone[k] = CopyMap(v)
	}
	return clone
}

func CopySlice[V any](s []V) []V {
	if s == nil {
		return nil
	}
	return Ternary(s == nil, nil, append([]V{}, s...))
}

func Contains[A comparable](vals []A, target A) bool {
	for i := 0; i < len(vals); i++ {
		if vals[i] == target {
			return true
		}
	}
	return false
}

func GetIndex[A comparable](vals []A, target A) int {
	for i := 0; i < len(vals); i++ {
		if vals[i] == target {
			return i
		}
	}
	return -1
}

// ListToMap returns a map K->V of eles transformed by mapper
func ListToMap[A any, K comparable, V any](mapper func(a A) (K, V), e []A) map[K]V {
	rv := map[K]V{}
	for _, ele := range e {
		k, v := mapper(ele)
		rv[k] = v
	}
	return rv
}

// Filter returns a list of elements from eles matching the given predicate
func Filter[T any](predicate func(v T) bool, eles []T) []T {
	var rv []T
	for _, ele := range eles {
		if predicate(ele) {
			rv = append(rv, ele)
		}
	}
	return rv
}

// Keys returns the keys of a map
func Keys[K comparable, V any](m map[K]V) []K {
	rv := make([]K, 0, len(m))
	for k := range m {
		rv = append(rv, k)
	}
	return rv
}

// MapBool returns a map where the list values are keys to a true value
func MapBool[K comparable](l []K) map[K]bool {
	rv := make(map[K]bool, len(l))
	for _, k := range l {
		rv[k] = true
	}
	return rv
}

// SortedKeys returns the sorted keys of a map by key comparison
func SortedKeys[K constraints.Ordered, V any](m map[K]V) []K {
	rv := make([]K, 0, len(m))
	for k := range m {
		rv = append(rv, k)
	}
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

// for chaining
func Sorted[K constraints.Ordered](l []K) []K {
	sort.Slice(l, func(i, j int) bool {
		return l[i] < l[j]
	})
	return l
}

// Values returns the values of a map
func Values[K comparable, V any](m map[K]V) []V {
	rv := make([]V, 0, len(m))
	for _, v := range m {
		rv = append(rv, v)
	}
	return rv
}

// ValuesBySortedKey returns ordered values of a map based on key sort
func ValuesBySortedKey[K constraints.Ordered, V any](m map[K]V) []V {
	rv := make([]V, 0, len(m))
	for _, k := range SortedKeys(m) {
		rv = append(rv, m[k])
	}
	return rv
}

// Unique returns the unique items of a slice
func Unique[K comparable](s []K) []K {
	m := map[K]bool{}
	for _, v := range s {
		m[v] = true
	}
	return Keys(m)
}

func Get[K comparable, V any](m map[K]V, k K, orElse V) V {
	rv := orElse
	if v, ok := m[k]; ok {
		rv = v
	}
	return rv
}

// PointerList maps a list of T to *T
func PointerList[T any](eles []T) []*T {
	if eles == nil {
		return nil
	}
	rv := make([]*T, 0, len(eles))
	for i := range eles {
		rv = append(rv, &eles[i])
	}
	return rv
}

func Pointer[T any](ele T) *T {
	return &ele
}

func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
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

// MustSplitPropsVals splits strings of form "key:value1,value2" into map[key] = {value1, value2}
func MustSplitPropsVals(props []string) map[string][]string {
	rv, err := SplitProps(props)
	if err != nil {
		panic(err)
	}
	return SplitValues(rv)
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

func Ternary[V any](condition bool, valueOnConditionTrue, valueOtherwise V) V {
	if condition {
		return valueOnConditionTrue
	}
	return valueOtherwise
}

func ScaleAndRoundUp(dividend, divisor, scale int64) (int64, error) {
	if divisor == 0 {
		return 0, fmt.Errorf("divisor cannot be 0")
	}
	// Note: Calculate the ratio first, then multiply by the new memory value
	// This avoids potential overflow from multiplying large numbers
	ratio := float64(scale) / float64(divisor)
	result := ratio * float64(dividend)
	return int64(math.Floor(result + 0.5)), nil
}

// mapValuesToKeyList takes a map of maps and returns a map to list of keys of the inner map
func mapValuesToKeyList[T any](m map[string]map[string]T) map[string][]string {
	rv := map[string][]string{}
	for k, v := range m {
		rv[k] = SortedKeys(v)
	}
	return rv
}

func StringElementsEquals(a, b []string) bool {
	if a == nil || b == nil || len(a) != len(b) {
		return false
	}
	sort.Strings(a)
	sort.Strings(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func Merge[K comparable, V any](ma map[K]V, mb map[K]V) map[K]V {
	m := CopyMap(ma)
	m = EnsureMap(m)
	for k, v := range mb {
		m[k] = v
	}
	return m
}

func MaxInArray(nums []int) int {
	if len(nums) == 0 {
		return -1
	}
	res := nums[0]
	for _, num := range nums {
		if num > res {
			res = num
		}
	}
	return res
}

func GetSystemHealth(system *systemv1.System) (bool, map[string]bool, string) {
	healthy, errorPorts, msg := getInstanceHealth(
		"system", system.Name, system.Spec.Unschedulable, system.Status.Conditions)
	return healthy, errorPorts, msg
}

// GetNodeHealth checks whether a node is schedulable
func GetNodeHealth(node *corev1.Node, cfgNode *Node) (bool, map[string]bool, string) {
	healthy := true
	errNICs := map[string]bool{}
	msg := ""

	// CBSD-9228
	// In very rare occasion, it could be possible that the K8s node was added but PB3 was not done on such a node
	if cfgNode == nil {
		healthy = false
		msg = "node does not exist in cluster config"
		return healthy, errNICs, msg
	}
	healthy, errNICs, msg = getInstanceHealth(
		"node", node.Name, node.Spec.Unschedulable, node.Status.Conditions)
	if healthy || len(errNICs) == 0 {
		return healthy, errNICs, msg
	}
	// special branch override health status if NIC errors should be ignored
	if len(errNICs) != 0 && cfgNode.ShouldIgnoreNICErrors(errNICs) {
		healthy = true
	}
	return healthy, errNICs, msg
}

// getInstanceHealth returns healthy when the instance is schedulable and meets one of the following conditions:
// 1. For systems, no system errors or system port errors
// 2. For nodes, no node errors, switch port errors or node nic errors
func getInstanceHealth(
	instanceType,
	name string,
	unschedulable bool,
	conditions []corev1.NodeCondition,
) (bool, map[string]bool, string) {
	errNICs := map[string]bool{}
	healthy := true
	instanceError := false
	var msgs []string
	// this branch can be removed at rel-2.6
	if unschedulable {
		msgs = append(msgs, fmt.Sprintf("cli tool manual update: %s %s was set to error", instanceType, name))
		logrus.Warnf("%s %s is marked unschedulable", instanceType, name)
		healthy = false
		return healthy, errNICs, strings.Join(msgs, "; ")
	}

	for _, cond := range conditions {
		if !strings.HasPrefix(string(cond.Type), ClusterMgmtConditionPrefix) &&
			!strings.HasPrefix(string(cond.Type), CsadmConditionPrefix) &&
			!strings.HasPrefix(string(cond.Type), ClusterDeployConditionPrefix) {
			continue
		}
		if cond.Status != corev1.ConditionTrue {
			// Always capture messages from csadm conditions if they exist
			if strings.HasPrefix(string(cond.Type), CsadmConditionPrefix) && cond.Message != "" {
				msgs = append(msgs, cond.Message)
			}
			continue
		}

		logrus.Warnf("%s %s has error condition: %v; ", instanceType, name, cond)
		if strings.HasPrefix(string(cond.Type), ClusterMgmtConditionPrefix) {
			msgs = append(msgs, fmt.Sprintf("alert fired: %s", cond.Message))
		} else if strings.HasPrefix(string(cond.Type), ClusterDeployConditionPrefix) {
			msgs = append(msgs, fmt.Sprintf("cluster deploy error: %s", cond.Message))
		} else {
			msgs = append(msgs, cond.Message)
		}
		healthy = false
		// system/node/switch level error is more critical, skip port/NIC level checks if met
		if strings.Contains(string(cond.Type), SystemErrorAlertType) ||
			strings.Contains(string(cond.Type), NodeErrorAlertType) ||
			strings.Contains(string(cond.Type), NodeSwitchPortAlertType) ||
			strings.Contains(string(cond.Type), ClusterDeployNotAppliedType) {
			instanceError = true
		} else if strings.Contains(string(cond.Type), NodeNICAlertType) ||
			strings.Contains(string(cond.Type), SystemPortAlertType) {
			strs := strings.Split(string(cond.Type), "_")
			if len(strs) > 1 {
				errNICs[strs[1]] = true
			}
		}
	}

	// erase port/nic error information if there's a more serious instance level error
	// this is to simplify the reloading logic to check whether it's a NIC level or instance level error
	if instanceError {
		errNICs = map[string]bool{}
	}
	return healthy, errNICs, strings.Join(msgs, "; ")
}

func EmitResourceAlertEvent(client client.Client, recorder record.EventRecorder, rsName, message string) {
	wsjob := &wsapisv1.WSJob{}
	for jobName, res := range JobResourcesTracker {
		if _, ok := res[rsName]; ok {
			msg := fmt.Sprintf("alert is firing for job %s scheduled resource %s, alert: %s",
				jobName, rsName, message)
			logrus.Warnf(msg)
			if client != nil {
				if client.Get(context.Background(), jobName, wsjob) == nil {
					recorder.Event(wsjob, corev1.EventTypeWarning, AlertIsFiring, message)
				}
			}
		}
	}
}

type JobEvent struct {
	Name          string
	Reason        string
	Message       string
	LastTimestamp *metav1.Time
}

// The following routine attempts to use the most recent warning event to enrich error message for failed pods
// Typically we only have one or limited few failed pods returned from the wsjob status.
// This is only a best-effort approach, instead of a guarantee, to enrich error context. Therefore, we will
// ignore any error occurred in this routine.
func GetJobFailureEvents(client kubeclientset.Interface, wsjob *wsapisv1.WSJob, excludedReasons map[string]bool) []JobEvent {
	jobLevelEvents := GetWarningEventsForJob(client, wsjob)
	podLevelEvents := GetWarningEventsForPods(client, wsjob)

	var events []JobEvent
	for _, e := range append(jobLevelEvents, podLevelEvents...) {
		if _, ok := excludedReasons[e.Reason]; !ok {
			events = append(events, e)
		}
	}

	// Sort by timestamp
	sort.Slice(events, func(i, j int) bool {
		if events[i].LastTimestamp.IsZero() {
			return true
		}
		if events[j].LastTimestamp.IsZero() {
			return false
		}
		return events[i].LastTimestamp.Time.Before(events[j].LastTimestamp.Time)
	})

	return events
}

func GetWarningEventsForJob(client kubeclientset.Interface, wsjob *wsapisv1.WSJob) []JobEvent {
	var jobEvents []JobEvent

	// get job level events
	for _, event := range GetEvents(client, wsjob.Namespace, wsjob.Name, corev1.EventTypeWarning) {
		if event.Message == "" && event.Reason == "" {
			continue
		}
		jobEvents = append(jobEvents, JobEvent{
			Name:          event.InvolvedObject.Name,
			Reason:        event.Reason,
			Message:       event.Message,
			LastTimestamp: &event.LastTimestamp,
		})
	}
	logrus.Warnf("Warning job event(s) in %s: %+v", wsjob.Name, jobEvents)
	return jobEvents
}

func GetWarningEventsForPods(client kubeclientset.Interface, wsjob *wsapisv1.WSJob) []JobEvent {
	var failedPods []commonapisv1.FailedPodStatus
	var jobEvents []JobEvent

	// get failed pods events
	for _, v := range wsjob.Status.ReplicaStatuses {
		for _, fps := range v.FailedPodStatuses {
			failedPods = append(failedPods, fps)
		}
	}
	sort.Slice(failedPods, func(i, j int) bool {
		if failedPods[i].FinishedAt.IsZero() {
			return true
		}
		if failedPods[j].FinishedAt.IsZero() {
			return false
		}
		return failedPods[i].FinishedAt.Time.Before(failedPods[j].FinishedAt.Time)
	})
	// keep 3 at most
	if len(failedPods) > 3 {
		failedPods = failedPods[:3]
	}
	for _, fps := range failedPods {
		// append error replica status/log as default event
		jobEvents = append(jobEvents, JobEvent{
			Name:          fps.Name,
			Reason:        fps.Message,
			Message:       fps.Log,
			LastTimestamp: fps.FinishedAt,
		})
		podName := fps.Name
		podEventCount := 0
		for _, event := range GetEvents(client, wsjob.Namespace, podName, corev1.EventTypeWarning) {
			// skip dns lookup warning
			if event.Message == "" && event.Reason == "" {
				continue
			}
			jobEvents = append(jobEvents, JobEvent{
				Name:          event.InvolvedObject.Name,
				Reason:        event.Reason,
				Message:       event.Message,
				LastTimestamp: &event.LastTimestamp,
			})
			podEventCount += 1
		}
	}
	logrus.Warnf("Warning pod event(s) in %s: %+v", wsjob.Name, jobEvents)
	return jobEvents
}

func GetEvents(client kubeclientset.Interface, ns, name string, eventType string) []corev1.Event {
	selectorQuery := fmt.Sprintf("involvedObject.name=%s,type=%s", name, eventType)
	selector, err := fields.ParseSelector(selectorQuery)
	if err != nil {
		logrus.Warnf("Failed to parse selector query %s: %+v", selectorQuery, err)
		return nil
	}
	options := metav1.ListOptions{FieldSelector: selector.String(), Limit: 100}
	events, err := client.CoreV1().Events(ns).List(context.Background(), options)
	if err != nil {
		logrus.Warnf("Failed to list events for pod %s: %+v", name, err)
		return nil
	}
	sort.Slice(events.Items, func(i, j int) bool {
		return events.Items[i].LastTimestamp.Time.After(events.Items[j].LastTimestamp.Time)
	})
	logrus.Warnf("%s events from resource %s: %+v", eventType, name, events)
	// return max 3 events
	if len(events.Items) > 3 {
		return events.Items[:3]
	}
	return events.Items
}

// check for eviction caused pod hanging or termination
func EvictionCheck(client kubeclientset.Interface, namespace, name string) string {
	selectorQuery := "type=Warning,reason=Evicted"
	selector, err := fields.ParseSelector(selectorQuery)
	if err != nil {
		logrus.Warnf("Failed to parse selector query %s: %s", selectorQuery, err)
		return ""
	}
	options := metav1.ListOptions{FieldSelector: selector.String(), Limit: 10}
	events, err := client.CoreV1().Events(namespace).List(context.Background(), options)
	if err != nil {
		logrus.Warnf("Failed to list events in ns %s by options: %v: %s", namespace, options, err)
		return ""
	}
	for _, e := range events.Items {
		if strings.Contains(e.InvolvedObject.Name, name) {
			msg := fmt.Sprintf("%s evicted by %s: %s",
				e.InvolvedObject.Name, events.Items[0].ReportingInstance, events.Items[0].Message)
			logrus.Warnf(msg)
			return msg
		}
	}
	return ""
}

func IsServiceReady(client kubeclientset.Interface, namespace, service string) (bool, error) {
	// Get Endpoints associated with the Service
	endpoints, err := client.CoreV1().Endpoints(namespace).Get(context.TODO(), service, metav1.GetOptions{})
	if err != nil {
		logrus.Errorf("Get endpoints error: %s", err)
		return false, err
	}

	// Check if any subset of Endpoints is ready
	for _, subset := range endpoints.Subsets {
		if len(subset.Addresses) > 0 && subset.Addresses[0].IP != "" {
			return true, nil
		}
	}
	logrus.Infof("Endpoints not ready: %s", service)
	return false, nil
}

func GetPodLogs(clientSet kubeclientset.Interface, namespace, pod, container string, tail int64) (string, error) {
	// by default limit to average 200 bytes per line
	options := &corev1.PodLogOptions{
		Container: container,
	}
	if tail > 0 {
		options.TailLines = Pointer(tail)
		options.LimitBytes = Pointer(tail * 200)
	}

	req := clientSet.CoreV1().Pods(namespace).GetLogs(pod, options)
	podLogs, err := req.Stream(context.Background())
	if err != nil {
		return "", err
	}
	defer podLogs.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func PodHasCondition(pod *corev1.Pod, condition corev1.PodConditionType) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == condition {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// IsCriticalK8sError returns true if error is API error with status too large or invalid (which is also returned
// when entities are too large
func IsCriticalK8sError(err error) bool {
	if status := apierrors.APIStatus(nil); errors.As(err, &status) {
		reason := status.Status().Reason
		if reason == metav1.StatusReasonInvalid || reason == metav1.StatusReasonRequestEntityTooLarge {
			return true
		}
	}
	return false
}

// In most cases, we always have a coordinator in jobs. One exception as of Dec 2023 is that,
// SDK execute jobs will only have a worker pod.
func GetWsjobImageVersion(wsjob *wsapisv1.WSJob) string {
	cbcoreImage := ""
	if wsjob.IsSdk() && !wsjob.IsCompile() {
		if spec := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeWorker]; spec != nil {
			if containers := spec.Template.Spec.Containers; len(containers) > 0 {
				cbcoreImage = containers[0].Image
			}
		}
	} else {
		if spec := wsjob.Spec.WSReplicaSpecs[wsapisv1.WSReplicaTypeCoordinator]; spec != nil {
			if containers := spec.Template.Spec.Containers; len(containers) > 0 {
				cbcoreImage = containers[0].Image
			}
		}
	}
	tokens := strings.Split(cbcoreImage, ":")
	if len(tokens) <= 1 {
		logrus.Warnf("invalid cbcore image version in job %s: %s", wsjob.NamespacedName(), cbcoreImage)
		return cbcoreImage
	}
	return tokens[len(tokens)-1]
}

func StandardV2StampsDetected() bool {
	return !wsapisv1.CrossStampSwitchesDetected && wsapisv1.SystemStampsDetected
}

func GetNadByIndex(i int) string {
	if i == 0 {
		return DataNetAttachDef
	} else if i == 1 {
		// for backwards compatible
		return SecondNetAttachDef
	}
	return fmt.Sprintf("%s-%d", DataNetAttachDef, i)
}

func GetBrConfigData(brConfigMap *corev1.ConfigMap) []byte {
	compressedData, ok := brConfigMap.BinaryData[wsapisv1.BrConfigCompressedFileName]
	if !ok {
		// this should not happen
		logrus.Warnf("compressed br config does not exist")
	}
	out, err := Gunzip(compressedData)
	if err != nil {
		logrus.Warnf("unable to uncompress br config json:%s", err)
	}
	return out
}

func GetClusterDetailsData(clusterDetailsConfigMap *corev1.ConfigMap) []byte {
	compressedData, ok := clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsConfigCompressedFileName]
	if !ok {
		// this should not happen
		logrus.Warnf("compressed cluster details does not exist")
	}
	out, err := Gunzip(compressedData)
	if err != nil {
		logrus.Warnf("unable to uncompress cluster details json: %s", err)
	}
	return out
}

func GetActToCsxDomainsData(clusterDetailsConfigMap *corev1.ConfigMap) ([]byte, string) {
	compressedData, ok := clusterDetailsConfigMap.BinaryData[wsapisv1.ClusterDetailsActToCsxDomains]
	if !ok {
		// this should not happen
		logrus.Warnf("compressed cluster details does not exist")
	}
	out, err := Gunzip(compressedData)
	if err != nil {
		logrus.Warnf("unable to uncompress cluster details json: %s", err)
	}
	encoded := base64.StdEncoding.EncodeToString(compressedData)
	return out, encoded
}

func SerializeWioTaskMap(taskMap *commonpb.ClusterDetails_TaskInfo_TaskMap) string {
	var domains []string
	var systemIndices []string
	for _, domain := range taskMap.AddressBook.WseDomains {
		domains = append(domains, fmt.Sprintf("%d", domain))
	}
	for _, systemIdx := range taskMap.TaskId.WseIds {
		systemIndices = append(systemIndices, fmt.Sprintf("%d", systemIdx))
	}
	// "0-1@2-3" stands for the activation needs to talk to domain 0 and domain 1
	// on both the second and third system
	return strings.Join([]string{
		strings.Join(domains, "-"),
		strings.Join(systemIndices, "-"),
	}, "@")
}

func ParseTargetedDomainsAnnot(annot string) ([]WioTarget, error) {
	// parse error should never happen, handling it just to be safe
	parseError := "received an unrecognized WioTarget to CSX domains input: %s"

	var result []WioTarget
	decoded, err := Base64DecodeGunzip(annot)
	if err != nil {
		// this should not happen
		return nil, err
	}
	// input: "3@0-1,2@0-1,1@2-3,0@3-4"
	// - act-0 talks to domain 3 of 0th and 1st system
	// - act-1 talks to domain 2 of 0th and 1st system
	for _, targetStr := range strings.Split(decoded, ",") {
		tokens := strings.Split(targetStr, "@")
		if len(tokens) != 2 {
			return nil, fmt.Errorf(parseError, decoded)
		}
		target := WioTarget{}
		for _, domainStr := range strings.Split(tokens[0], "-") {
			if domain, err := strconv.Atoi(domainStr); err != nil {
				return nil, fmt.Errorf(parseError, decoded)
			} else {
				target.Domains = append(target.Domains, domain)
			}
		}
		for _, sysIdxStr := range strings.Split(tokens[1], "-") {
			if sysIdx, err := strconv.Atoi(sysIdxStr); err != nil {
				return nil, fmt.Errorf(parseError, decoded)
			} else {
				target.AllSystemIndices = append(target.AllSystemIndices, sysIdx)
			}
		}
		result = append(result, target)
	}
	return result, nil
}

func Gzip(conf []byte) ([]byte, error) {
	if len(conf) == 0 {
		return nil, nil
	}
	var w bytes.Buffer
	buf := gzip.NewWriter(&w)
	if _, err := buf.Write(conf); err != nil {
		return nil, err
	}
	buf.Close()
	return w.Bytes(), nil
}

func Gunzip(b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
	reader, err := gzip.NewReader(buf)
	if err != nil {
		return nil, err
	}
	uncompressed := new(strings.Builder)
	_, err = io.Copy(uncompressed, reader)
	if err != nil {
		return nil, err
	}
	return []byte(uncompressed.String()), nil
}

func Base64DecodeGunzip(encoded string) (string, error) {
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	uncompressed, err := Gunzip(decoded)
	return string(uncompressed), nil
}

func ParseLabelVersions(labelVal string) []string {
	return strings.Split(labelVal, wsapisv1.LabelSeparator)
}

func GetPlatformVersionAsString(labelVal string) string {
	return strings.Join(ParseLabelVersions(labelVal), ",")
}

func GetAffinityRequest(annotations map[string]string, isAnti bool) (map[string]string, map[string]bool) {
	var res map[string]string
	var isJobProp map[string]bool
	for k, v := range annotations {
		nodePropAffinityKey := wsapisv1.AffinityPrefix
		jobPropAffinityKey := wsapisv1.JobAffinityPrefix
		if isAnti {
			nodePropAffinityKey = wsapisv1.AntiAffinityPrefix
			jobPropAffinityKey = wsapisv1.JobAntiAffinityPrefix
		}
		if strings.HasPrefix(k, nodePropAffinityKey) {
			res = EnsureMap(res)
			k = k[len(nodePropAffinityKey):]
			res[k] = v
		} else if strings.HasPrefix(k, jobPropAffinityKey) {
			res = EnsureMap(res)
			k = k[len(jobPropAffinityKey):]
			res[k] = v
			isJobProp = EnsureMap(isJobProp)
			isJobProp[k] = true
		}
	}
	return res, isJobProp
}

// Deprecated: keep for legacy version
// todo: remove at rel-2.7
func AnnotateCanceledLease(lease *coordv1.Lease, status, reason, ctx string) {
	lease.Annotations = EnsureMap(lease.Annotations)
	lease.Labels = EnsureMap(lease.Labels)
	lease.Annotations[wsapisv1.CancelWithStatusKey] = status
	lease.Annotations[wsapisv1.CancelJobContextKey] = ctx
	lease.Labels[wsapisv1.CancelReasonKey] = reason
	return
}

func GenCancelAnnotate(status, reason, message string) map[string]string {
	annotations := make(map[string]string)
	annotations[wsapisv1.CancelWithStatusKey] = status
	annotations[wsapisv1.CancelReasonKey] = reason
	if status != wsapisv1.CancelWithStatusSucceeded {
		ctxMsg := fmt.Sprintf("job terminated by client with status: %s, reason: %s", status, reason)
		if message != "" {
			ctxMsg = fmt.Sprintf("%s, message: %s", ctxMsg, message)
		}
		annotations[wsapisv1.CancelJobContextKey] = ctxMsg
	}
	return annotations
}

// GetAnnotationPatch generates a JSON patch for annotations from the given map.
func GetAnnotationPatch(annotations map[string]string) []byte {
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": annotations,
		},
	}
	patchBytes, _ := json.Marshal(patch)
	return patchBytes
}

// GetLabelPatch generates a JSON patch for labels from the given map.
func GetLabelPatch(labels map[string]string) []byte {
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": labels,
		},
	}
	patchBytes, _ := json.Marshal(patch)
	return patchBytes
}

type wsjobSpecPatch struct {
	Priority *int `json:"priority,omitempty"`
}
type wsjobObjPatch struct {
	Spec *wsjobSpecPatch `json:"spec,omitempty"`
}

func PatchWsjobPriority(ctx context.Context, wsV1Interface wsv1.WsV1Interface,
	namespace, name string, priorityVal int) error {
	k8sPatch := wsjobObjPatch{Spec: &wsjobSpecPatch{Priority: Pointer(priorityVal)}}
	reqBody, err := json.Marshal(k8sPatch)
	if err != nil {
		return err
	}
	_, err = wsV1Interface.WSJobs(namespace).Patch(
		ctx, name, types.MergePatchType, reqBody, metav1.PatchOptions{})
	return err
}

func ReservingLockName(lock string) string {
	return "reserving-" + lock
}

func LogProtoAsJson(m interface{}, emitUnpopulated bool) interface{} {
	if msg, ok := m.(proto.Message); ok {
		options := protojson.MarshalOptions{
			EmitUnpopulated: emitUnpopulated,
		}
		// Using "protojson" to marshal gives slightly different outputs (+/- whitespaces)
		// in CI compared to local environment. Using "encoding/json" to ensure consistency
		// for now and revisit after toolchain upgrade.
		if protoJson, err := options.Marshal(msg); err == nil {
			var jsonMap map[string]interface{}
			if err := json.Unmarshal(protoJson, &jsonMap); err == nil {
				if compactJson, err := json.Marshal(jsonMap); err == nil {
					return string(compactJson)
				}
			}
		}
	}
	return m
}

func SortJobs(
	wsjobs []*wsapisv1.WSJob,
	locks []*rlv1.ResourceLock,
	orderBy string,
	sortBy string,
	allStates bool,
	uid *int64,
	limit int32,
) (jobs []*wsapisv1.WSJob, jobQueueMap map[string]string, lockMap map[string]*rlv1.ResourceLock) {
	// build lock index
	lockMap = map[string]*rlv1.ResourceLock{}
	for _, l := range locks {
		lockMap[l.Name] = l
	}

	// try to simulate the `reservingIsEnabled` check in scheduler
	workflowGrantedExecutes := map[string]int{}
	reservationInherited := map[string]bool{}
	if !allStates {
		for _, w := range wsjobs {
			if w.IsExecute() && commonutil.HasScheduled(w.Status) {
				workflowGrantedExecutes[w.GetWorkflowId()]++
			}
			if w.ParentJobName() != "" && commonutil.IsActive(w.Status) {
				reservationInherited[w.ParentJobName()] = true
			}
		}
	}
	reservationEnabled := func(w *wsapisv1.WSJob) bool {
		if w.IsCompile() && workflowGrantedExecutes[w.GetWorkflowId()] == 0 {
			return false
		}
		if reservationInherited[ReservingLockName(w.Name)] {
			return false
		}
		return true
	}

	// filter jobs by state/user
	jobQueueMap = map[string]string{}
	for _, w := range wsjobs {
		jobQueueMap[w.Name] = w.GetJobType()
		if l := lockMap[w.Name]; l != nil {
			jobQueueMap[w.Name] = l.Spec.QueueName
		}
		jobStateFound := allStates ||
			!commonutil.IsEnded(w.Status) ||
			(w.IsReserved() && reservationEnabled(w)) // only show enabled reservation
		jobUidFound := uid == nil || *uid < 0 || w.Spec.User.Uid == *uid
		if !jobStateFound || !jobUidFound {
			continue
		}
		jobs = append(jobs, w)
	}

	// List jobs in order of scheduling priority. (highest priority on top)
	// For example:
	// job1 p0 5m
	// job2 p0 1m
	// job3 p1 65m
	sort.Slice(jobs, func(i, j int) bool {
		jobI := jobs[i]
		jobJ := jobs[j]
		// for backwards compatible, use not equal to account for default empty case
		if sortBy != Age {
			// ended jobs on top if all states
			if commonutil.IsEnded(jobI.Status) != commonutil.IsEnded(jobJ.Status) {
				if orderBy != Ascending {
					return commonutil.IsEnded(jobI.Status)
				} else {
					return commonutil.IsEnded(jobJ.Status)
				}
			}
			// running job before pending job
			if commonutil.HasScheduled(jobI.Status) != commonutil.HasScheduled(jobJ.Status) {
				if orderBy != Ascending {
					return commonutil.HasScheduled(jobI.Status)
				} else {
					return commonutil.HasScheduled(jobJ.Status)
				}
			}
			// queue compare
			if jobQueueMap[jobI.Name] != jobQueueMap[jobJ.Name] {
				if orderBy != Ascending {
					return GetIndex(rlv1.OrderedQueues, jobQueueMap[jobI.Name]) <
						GetIndex(rlv1.OrderedQueues, jobQueueMap[jobJ.Name])
				} else {
					return GetIndex(rlv1.OrderedQueues, jobQueueMap[jobI.Name]) >
						GetIndex(rlv1.OrderedQueues, jobQueueMap[jobJ.Name])
				}
			}
			// session compare
			if jobI.Namespace != jobJ.Namespace {
				return jobI.Namespace < jobJ.Namespace
			}
			// priority compare
			if jobI.GetPriority() != jobJ.GetPriority() {
				if orderBy != Ascending {
					return jobI.GetPriority() < jobJ.GetPriority()
				} else {
					return jobI.GetPriority() > jobJ.GetPriority()
				}
			}
		}
		// create timestamp compare
		if orderBy != Ascending {
			return jobI.CreationTimestamp.Time.Before(jobJ.CreationTimestamp.Time)
		} else {
			return jobI.CreationTimestamp.Time.After(jobJ.CreationTimestamp.Time)
		}
	})

	// Apply max results limit if specified
	// Note:
	//  We're showing the last N jobs instead of the first N jobs, because the jobs are in FIFO order
	//  However, when we support pagination, we should switch to showing the first page instead (and return the "next page token")
	if limit > 0 && len(jobs) > int(limit) {
		jobs = jobs[len(jobs)-int(limit):]
	}
	return jobs, jobQueueMap, lockMap
}

func GetIngressAuthority(ingress *networkingv1.Ingress) string {
	// ingress.Spec.Rules[0].Host is the specific ingress host which will have some non-predetermined
	// SUBDOMAIN.SERVICE_DOMAIN where subdomain is the job id. The client will use this in their GRPC client's
	// "authority" configuration which will be used in the HTTP/2 ":authority" header, eventually routing the
	// request through the mgmt at SERVICE_DOMAIN, to the ingress at SUBDOMAIN.SERVICE_DOMAIN.
	// e.g. wsjob-xxxx-coordinator-0.cluster-server.multibox-xx.cerebrassc.local
	return ingress.Spec.Rules[0].Host
}

func GetInferenceRelatedReplicaTypes() []commonapisv1.ReplicaType {
	return []commonapisv1.ReplicaType{wsapisv1.WSReplicaTypeKVStorageServer, wsapisv1.WSReplicaTypeActivation, wsapisv1.WSReplicaTypeChief, wsapisv1.WSReplicaTypeSWDriver}
}

// GetPreciseMiBFromBytes converts bytes to mebibytes (MiB) using binary conversion.
// Returns the exact floating-point result of bytes / 2^20 (1,048,576).
func GetPreciseMiBFromBytes(bytes int64) float64 {
	return float64(bytes) / (1 << 20)
}

func ValueOrDefault[K comparable, V any](m map[K]V, key K, defaultVal V) V {
	if m != nil {
		if v, ok := m[key]; ok {
			return v
		}
	}
	return defaultVal
}

func GetReplicaName(rtype string, index int) string {
	return fmt.Sprintf("%s-%d", strings.ToLower(rtype), index)
}

type Set[T comparable] struct {
	data map[T]struct{}
}

func NewSet[T comparable]() *Set[T] {
	return &Set[T]{data: make(map[T]struct{})}
}

func (s *Set[T]) EnsureInstantiation() {
	s.data = EnsureMap(s.data)
}

func (s *Set[T]) Emplace(item T) {
	if !s.Contains(item) {
		s.data[item] = struct{}{}
	}
}

func (s *Set[T]) Remove(item T) {
	delete(s.data, item)
}

func (s *Set[T]) Contains(item T) bool {
	_, exists := s.data[item]
	return exists
}

func (s *Set[T]) Size() int {
	return len(s.data)
}

func (s *Set[T]) IsEmpty() bool {
	return len(s.data) == 0
}

// ToSlice returns an unsorted slice of all elements in the set
func (s *Set[T]) ToSlice() []T {
	if s.data == nil {
		return []T{}
	}

	result := make([]T, 0, len(s.data))
	for item := range s.data {
		result = append(result, item)
	}

	return result
}

func AppendUnique[T comparable](slice []T, item T) []T {
	for _, existing := range slice {
		if existing == item {
			return slice
		}
	}
	return append(slice, item)
}

func GcdInt64(a, b int64) int64 {
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

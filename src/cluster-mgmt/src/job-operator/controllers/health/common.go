package health

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/go-logr/logr"
	"github.com/prometheus/alertmanager/api/v2/models"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"

	wscommon "cerebras.com/job-operator/common"
)

// merge existing conditions with conditions generated based on alerts
func getUpdatedConditions(alertCache AlertCache, obj runtime.Object, existingConditions []corev1.NodeCondition,
	log logr.Logger, recorder record.EventRecorder, client client.Client,
	alertList ...string) (bool, []corev1.NodeCondition, error) {
	var updatedConditions []corev1.NodeCondition
	objName := alertList[0]

	generatedConditions := map[corev1.NodeConditionType]*corev1.NodeCondition{}
	alertDetected := false
	alerts, ts, fetchErr := alertCache.Get(alertList...)
	currentTime := ts.Rfc3339Copy() // convert to second level
	if fetchErr == nil {
		for _, al := range alerts {
			condition, err := generateConditionByAlert(al, currentTime)
			if err != nil {
				return false, nil, err
			}
			alertDetected = true
			generatedConditions[condition.Type] = condition
		}
	}

	// update existing conditions if matching alerts detected/gone
	for _, existing := range existingConditions {
		// skip if not cluster mgmt owned conditions
		if !strings.HasPrefix(string(existing.Type), wscommon.ClusterMgmtConditionPrefix) {
			updatedConditions = append(updatedConditions, existing)
			// emit event for default condition that's not alerted yet for faster propagation
			if existing.Status == corev1.ConditionTrue &&
				(existing.Type == corev1.NodeDiskPressure || existing.Type == corev1.NodeMemoryPressure) {
				alertExists := false
				for cond := range generatedConditions {
					if strings.Contains(string(cond), string(existing.Type)) {
						alertExists = true
						break
					}
				}
				if !alertExists {
					wscommon.EmitResourceAlertEvent(client, recorder, objName,
						fmt.Sprintf("Node is under %s", existing.Type))
				}
			}
			continue
		}
		condLog := log.WithValues("condition", existing.Type, "oldStatus", existing.Status)
		generatedCondition, alertMatch := generatedConditions[existing.Type]

		// mark conditions as Unknown if alerts fetchErr
		if fetchErr != nil {
			log.Error(fetchErr, "last fetch on alerts failed")
			if existing.Status != statusUnknown {
				condLog.WithValues("newStatus", statusUnknown).Info("updating existing condition with new status")
				existing.LastTransitionTime = currentTime
			}
			existing.Status = statusUnknown
			existing.Reason = wscommon.AlertsUnavailable
			existing.Message = ""
			existing.LastHeartbeatTime = currentTime
			updatedConditions = append(updatedConditions, existing)
			continue
		}

		// update status/timestamp if existing condition match alert generated condition
		if alertMatch {
			condLog.WithValues("type", existing.Type).Info("alert found for existing condition")
			if existing.Status != generatedCondition.Status {
				condLog.WithValues("newStatus", generatedCondition.Status).Info("updating existing condition to true")
				recorder.Event(obj, corev1.EventTypeWarning, wscommon.AlertIsFiring, generatedCondition.Message)
				// generate event to wsjob using this resource
				wscommon.EmitResourceAlertEvent(client, recorder, objName, generatedCondition.Message)
			} else {
				generatedCondition.LastTransitionTime = existing.LastTransitionTime
			}
			updatedConditions = append(updatedConditions, *generatedCondition)
			delete(generatedConditions, existing.Type)
			continue
		} else {
			// update status to false if alerts no loner exists
			if existing.Status != statusFalse {
				condLog.WithValues("newStatus", statusFalse).Info("updating existing condition to false")
				existing.LastTransitionTime = currentTime
			}
			existing.Status = statusFalse
			existing.Reason = wscommon.AlertIsNotFiring
			existing.Message = ""
			existing.LastHeartbeatTime = currentTime
			if currentTime.Sub(existing.LastTransitionTime.Time) > conditionTTL {
				condLog.Info("deleting condition after ttl")
				continue
			}
			updatedConditions = append(updatedConditions, existing)
			continue
		}
	}

	// append remaining generatedConditions that don't match existing conditions in sorted order
	var conditionTypes []string
	for conditionType := range generatedConditions {
		conditionTypes = append(conditionTypes, string(conditionType))
	}
	sort.Strings(conditionTypes)
	for _, conditionType := range conditionTypes {
		condition := generatedConditions[corev1.NodeConditionType(conditionType)]
		condLog := log.WithValues("condition", condition.Type, "newStatus", condition.Status)
		condLog.V(1).Info("adding new condition")
		recorder.Event(obj, corev1.EventTypeWarning, wscommon.AlertIsFiring, condition.Message)
		// generate event to wsjob using this resource
		wscommon.EmitResourceAlertEvent(client, recorder, objName, condition.Message)
		updatedConditions = append(updatedConditions, *condition)
	}
	return alertDetected, updatedConditions, nil
}

func generateConditionByAlert(alert *models.GettableAlert, currentTime v1.Time) (*corev1.NodeCondition, error) {
	alertType := alert.Labels[alertTypeLabel]
	if alertType == "" {
		return nil, errors.New("no alertType label found")
	}
	wscommon.Log.Debugf("generating new condition: %s", alertType)
	conditionType := fmt.Sprintf("%s%s", wscommon.ClusterMgmtConditionPrefix, alertType)
	if alertType == wscommon.NodeNICAlertType || alertType == wscommon.SystemPortAlertType {
		nic := alert.Labels[wscommon.NicNameAlertLabel]
		conditionType = fmt.Sprintf("%s_%s", conditionType, nic)
	}
	condition := &corev1.NodeCondition{
		Type:               corev1.NodeConditionType(conditionType),
		Status:             statusTrue,
		LastHeartbeatTime:  currentTime,
		LastTransitionTime: currentTime,
		Reason:             wscommon.AlertIsFiring,
		Message:            alert.Annotations[alertSummaryAnnotation],
	}
	return condition, nil
}

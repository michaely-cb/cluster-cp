package alertrouter

import (
	"context"
	"fmt"
	"log"
	"strings"

	wsv1 "cerebras.com/job-operator/apis/ws/v1"
	wsversion "cerebras.com/job-operator/client/clientset/versioned"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

var alertPayloadJobIDLabels = []string{"name", "wsjobID"}
var alertPayloadSessionIDLabels = []string{"namespace", "sessionID"}

// JobContactInfoRetriever defines the interface for retrieving job contact information
type JobContactInfoRetriever interface {
	GetJobContacts(ctx context.Context, logger logrus.FieldLogger, labels map[string]string, severity int32) (map[ContactType][]string, error)
}

// WSClientContactInfoRetriever implements JobContactInfoRetriever using a real WS client
type WSClientContactInfoRetriever struct {
	client *wsversion.Clientset
}

func NewWSClientContactInfoRetriever() (*WSClientContactInfoRetriever, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatalf("Failed to create k8s client: %v", err)
	}
	wsClient, err := wsversion.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	return &WSClientContactInfoRetriever{client: wsClient}, nil
}

// findLabelValue searches through a list of possible label keys and returns the first matching value
func findLabelValue(labels map[string]string, possibleKeys []string) (string, bool) {
	for _, key := range possibleKeys {
		if value, ok := labels[key]; ok {
			return value, true
		}
	}
	return "", false
}

func (w *WSClientContactInfoRetriever) GetJobContacts(ctx context.Context, logger logrus.FieldLogger, labels map[string]string, severity int32) (map[ContactType][]string, error) {
	// Lookup labels for both jobID and sessionID using the configured label lists
	jobID, ok := findLabelValue(labels, alertPayloadJobIDLabels)
	if !ok {
		return nil, fmt.Errorf("no job ID found in labels, tried: %v", alertPayloadJobIDLabels)
	}
	sessionID, ok := findLabelValue(labels, alertPayloadSessionIDLabels)
	if !ok {
		return nil, fmt.Errorf("no session ID found in labels, tried: %v", alertPayloadSessionIDLabels)
	}
	// Extract the jobName from the full resource path
	parts := strings.Split(jobID, "/")
	if len(parts) == 0 {
		return nil, fmt.Errorf("invalid jobID format: empty string")
	}
	jobName := parts[len(parts)-1]
	if jobName == "" {
		return nil, fmt.Errorf("invalid jobID format: no job name found")
	}

	wsJob, err := w.client.WsV1().WSJobs(sessionID).Get(ctx, jobName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get WSJob: %v", err)
	}

	contacts := make(map[ContactType][]string)

	// Process each notification in the job spec
	for _, notification := range wsJob.Spec.Notifications {
		// If severity threshold is specified, only add contacts if the alert severity meets or exceeds it
		if notification.SeverityThreshold != nil && severity > *notification.SeverityThreshold {
			logger.Debugf("skipping contact %s because severity %d is greater than threshold %d", notification.Target, severity, *notification.SeverityThreshold)
			continue
		}

		// Add contact based on notification type
		switch notification.NotificationType {
		case wsv1.NotificationTypeEmail:
			contacts[ContactTypeEmail] = append(contacts[ContactTypeEmail], notification.Target)
		case wsv1.NotificationTypeSlack:
			contacts[ContactTypeSlack] = append(contacts[ContactTypeSlack], notification.Target)
		case wsv1.NotificationTypePagerduty:
			contacts[ContactTypePagerDuty] = append(contacts[ContactTypePagerDuty], notification.Target)
		}
	}

	return contacts, nil
}

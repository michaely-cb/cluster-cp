package alertrouter

import (
	"context"

	"github.com/sirupsen/logrus"
)

// GetNotificationTargets determines the notification targets for an alert based on its category,
// severity, and for job alerts, the job owner's email
// - clusterOperators is the list of cluster operator notification [with targets, and optional severity threshold, and optional specific rules, and optional job/cluster restriction
// - contactInfoRetriever wraps a wsClient that will query etcd to get notification targets for a given wsjob
// - rules is the classification (severity, category) for each alert
// Returns a map of contact type to list of addresses, taking into account business rules for routing
func GetNotificationTargets(
	ctx context.Context,
	clusterOperators []ContactInfo,
	alertType AlertType,
	severity int,
	alertName string,
	labels map[string]string,
	contactInfoRetriever JobContactInfoRetriever,
	logger logrus.FieldLogger,
) (map[ContactType][]string, error) {
	// Get contacts based on category and severity
	notificationTargets := GetClusterOperatorContacts(clusterOperators, alertType, severity, alertName)

	// For job alerts, add the job owner's email
	if alertType == AlertTypeJob {
		jobContacts, err := contactInfoRetriever.GetJobContacts(ctx, logger, labels, int32(severity))
		if err != nil {
			logger.WithFields(logrus.Fields{
				"error": err,
			}).Error("Failed to lookup job contacts")
			// Even if we can't query the wsjob, we might have 'job' contacts in the cluster operator list, so continue here to notify them.
		}
		// Merge job contacts with cluster operator contacts
		for contactType, addresses := range jobContacts {
			notificationTargets[contactType] = append(notificationTargets[contactType], addresses...)
		}
	}

	return notificationTargets, nil
}

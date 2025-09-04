package alertrouter

// This file defines unit tests for the routing logic; first by some specific paths, then a randomized test for a combination of them.

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestGetNotificationTargets(t *testing.T) {
	// Test data setup
	contacts := []ContactInfo{
		// Email contacts with different severity thresholds
		{
			Type:              ContactTypeEmail,
			Address:           "critical@example.com",
			SeverityThreshold: IntPtr(1),
			AlertType:         AlertTypeCluster,
		},
		{
			Type:              ContactTypeEmail,
			Address:           "warning@example.com",
			SeverityThreshold: IntPtr(2),
			AlertType:         AlertTypeCluster,
		},
		{
			Type:              ContactTypeEmail,
			Address:           "info@example.com",
			SeverityThreshold: IntPtr(3),
			AlertType:         AlertTypeCluster,
		},
		{
			Type:              ContactTypeEmail,
			Address:           "debug@example.com",
			SeverityThreshold: IntPtr(4),
			AlertType:         AlertTypeCluster,
		},
		{
			Type:              ContactTypeEmail,
			Address:           "trace@example.com",
			SeverityThreshold: IntPtr(5),
			AlertType:         AlertTypeCluster,
		},

		// Slack contacts
		{
			Type:              ContactTypeSlack,
			Address:           "https://hook.slack.com/services/T123/B456/ABCDEF123456",
			SeverityThreshold: IntPtr(1),
		},
		{
			Type:              ContactTypeSlack,
			Address:           "https://hook.slack.com/services/T123/B789/GHIJKL789012",
			SeverityThreshold: IntPtr(3),
		},

		// PagerDuty contacts
		{
			Type:              ContactTypePagerDuty,
			Address:           "critical-service-key",
			SeverityThreshold: IntPtr(1),
			AlertType:         AlertTypeCluster,
		},

		// Job-specific contacts
		{
			Type:      ContactTypeEmail,
			Address:   "jobs@example.com",
			AlertType: AlertTypeJob,
		},
		{
			Type:              ContactTypeSlack,
			Address:           "https://hook.slack.com/services/T123/B101/MNOPQR345678",
			AlertType:         AlertTypeJob,
			SeverityThreshold: IntPtr(2),
		},

		// Rule-ID override contacts
		{
			Type:      ContactTypeEmail,
			Address:   "specific-rule-cluster@example.com",
			AlertName: "SpecificClusterAlert",
		},
		{
			Type:      ContactTypeSlack,
			Address:   "https://hook.slack.com/services/T123/B202/STUVWX901234",
			AlertName: "SpecificClusterAlert",
		},
		{
			Type:      ContactTypeEmail,
			Address:   "specific-rule-job@example.com",
			AlertName: "SpecificJobAlert",
		},

		// Category override contacts - receives both job and cluster alerts
		{
			Type:      ContactTypeEmail,
			Address:   "both-categories@example.com",
			AlertType: "", // Empty category means receive all
		},
		// Contact that explicitly specifies Category as "all"
		{
			Type:              ContactTypeEmail,
			Address:           "explicit-all@example.com",
			AlertType:         "all",
			SeverityThreshold: IntPtr(2),
		},
	}

	mockRetriever := &mockJobContactInfoRetriever{
		contacts: map[ContactType][]string{
			ContactTypeEmail: {"job-owner@example.com"},
		},
		jobID:     "default/test-job",
		sessionID: "default",
	}

	logger := logrus.New()

	tests := []struct {
		name          string
		category      AlertType
		severity      int
		alertName     string
		labels        map[string]string
		expectedEmail []string
		expectedSlack []string
		expectedPD    []string
		retriever     *mockJobContactInfoRetriever
		wantErr       bool
	}{
		{
			name:      "Critical cluster alert",
			category:  AlertTypeCluster,
			severity:  1,
			alertName: "ClusterCritical",
			expectedEmail: []string{
				"critical@example.com",
				"warning@example.com",
				"info@example.com",
				"debug@example.com",
				"trace@example.com",
				"both-categories@example.com",
				"explicit-all@example.com",
			},
			expectedSlack: []string{
				"https://hook.slack.com/services/T123/B456/ABCDEF123456",
				"https://hook.slack.com/services/T123/B789/GHIJKL789012",
			},
			expectedPD: []string{
				"critical-service-key",
			},
			retriever: mockRetriever,
		},
		{
			name:      "Warning level job alert",
			category:  AlertTypeJob,
			severity:  2,
			alertName: "JobWarning",
			labels: map[string]string{
				"wsjobID":   "default/test-job",
				"sessionID": "default",
			},
			expectedEmail: []string{
				"jobs@example.com",
				"both-categories@example.com",
				"job-owner@example.com",
				"explicit-all@example.com",
			},
			expectedSlack: []string{
				"https://hook.slack.com/services/T123/B101/MNOPQR345678",
				"https://hook.slack.com/services/T123/B789/GHIJKL789012",
			},
			retriever: mockRetriever,
		},
		{
			name:      "Rule-ID override clusteralert",
			category:  AlertTypeCluster,
			severity:  3,
			alertName: "SpecificClusterAlert",
			expectedEmail: []string{
				"specific-rule-cluster@example.com",
				"info@example.com",
				"debug@example.com",
				"trace@example.com",
				"both-categories@example.com",
			},
			expectedSlack: []string{
				"https://hook.slack.com/services/T123/B202/STUVWX901234",
				"https://hook.slack.com/services/T123/B789/GHIJKL789012",
			},
			retriever: mockRetriever,
		},
		{
			name:      "Low severity cluster alert",
			category:  AlertTypeCluster,
			severity:  4,
			alertName: "LowSeverity",
			expectedEmail: []string{
				"debug@example.com",
				"trace@example.com",
				"both-categories@example.com",
			},
			expectedSlack: []string{},
			retriever:     mockRetriever,
		},
		{
			name:      "Rule-ID override job alert",
			category:  AlertTypeJob,
			severity:  1,
			alertName: "SpecificJobAlert",
			labels: map[string]string{
				"wsjobID":   "default/test-job",
				"sessionID": "default",
			},
			expectedEmail: []string{
				"job-owner@example.com",
				"specific-rule-job@example.com",
				"jobs@example.com",
				"both-categories@example.com",
				"explicit-all@example.com",
			},
			expectedSlack: []string{
				"https://hook.slack.com/services/T123/B101/MNOPQR345678",
				"https://hook.slack.com/services/T123/B456/ABCDEF123456",
				"https://hook.slack.com/services/T123/B789/GHIJKL789012",
			},
			retriever: mockRetriever,
		},
		{
			name:      "Job alert with retriever error",
			category:  AlertTypeJob,
			severity:  1,
			alertName: "JobCritical",
			expectedEmail: []string{
				"jobs@example.com",
				"both-categories@example.com",
				"explicit-all@example.com",
			},
			expectedSlack: []string{
				"https://hook.slack.com/services/T123/B101/MNOPQR345678",
				"https://hook.slack.com/services/T123/B456/ABCDEF123456",
				"https://hook.slack.com/services/T123/B789/GHIJKL789012",
			},
			retriever: &mockJobContactInfoRetriever{
				err: assert.AnError,
			},
			wantErr: false, // Though retriever fails, we don't fail getting contacts [since we fallback to the operator job contats]
		},
		{
			name:      "AlertType 'all' receives cluster alerts",
			category:  AlertTypeCluster,
			severity:  2,
			alertName: "ClusterWarning",
			expectedEmail: []string{
				"warning@example.com",
				"info@example.com",
				"debug@example.com",
				"trace@example.com",
				"both-categories@example.com",
				"explicit-all@example.com",
			},
			expectedSlack: []string{
				"https://hook.slack.com/services/T123/B789/GHIJKL789012",
			},
			retriever: mockRetriever,
		},
		{
			name:      "AlertType 'all' receives job alerts",
			category:  AlertTypeJob,
			severity:  2,
			alertName: "JobWarning",
			labels: map[string]string{
				"wsjobID":   "default/test-job",
				"sessionID": "default",
			},
			expectedEmail: []string{
				"jobs@example.com",
				"both-categories@example.com",
				"job-owner@example.com",
				"explicit-all@example.com",
			},
			expectedSlack: []string{
				"https://hook.slack.com/services/T123/B101/MNOPQR345678",
				"https://hook.slack.com/services/T123/B789/GHIJKL789012",
			},
			retriever: mockRetriever,
		},
		{
			name:      "Alert without existing mapping",
			category:  AlertTypeCluster, // Should default to cluster category
			severity:  3,                // Should default to severity 3
			alertName: "UnmappedAlert",  // An alert name that doesn't exist in mappings
			expectedEmail: []string{
				"info@example.com",
				"debug@example.com",
				"trace@example.com",
				"both-categories@example.com",
			},
			expectedSlack: []string{
				"https://hook.slack.com/services/T123/B789/GHIJKL789012",
			},
			retriever: mockRetriever,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			targets, err := GetNotificationTargets(
				context.Background(),
				contacts,
				tt.category,
				tt.severity,
				tt.alertName,
				tt.labels,
				tt.retriever,
				logger,
			)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.expectedEmail != nil {
				assert.ElementsMatch(t, tt.expectedEmail, targets[ContactTypeEmail])
			}
			if tt.expectedSlack != nil {
				assert.ElementsMatch(t, tt.expectedSlack, targets[ContactTypeSlack])
			}
			if tt.expectedPD != nil {
				assert.ElementsMatch(t, tt.expectedPD, targets[ContactTypePagerDuty])
			}
		})
	}
}

func TestRandomizedNotificationTargets(t *testing.T) {
	// Generate random contacts
	clusterContacts := generateRandomClusterContacts(100)
	jobContacts := generateRandomJobContacts(10)

	// Create test scenarios
	scenarios := []struct {
		category  AlertType
		severity  int
		alertName string
	}{
		{AlertTypeCluster, 1, "CriticalAlert"},
		{AlertTypeJob, 2, "JobWarning"},
		{AlertTypeCluster, 3, "InfoAlert"},
		{AlertTypeJob, 1, "JobCritical"},
		{AlertTypeCluster, 4, "DebugAlert"},
	}

	jobContactsMap := make(map[ContactType][]string)
	for _, contact := range jobContacts {
		jobContactsMap[contact.Type] = append(jobContactsMap[contact.Type], contact.Address)
	}
	mockRetriever := &mockJobContactInfoRetriever{
		contacts: jobContactsMap,
	}
	logger := logrus.New()

	// Run each scenario
	for _, sc := range scenarios {
		t.Run(fmt.Sprintf("Random_%s_Sev%d", sc.category, sc.severity), func(t *testing.T) {
			targets, err := GetNotificationTargets(
				context.Background(),
				clusterContacts,
				sc.category,
				sc.severity,
				sc.alertName,
				nil,
				mockRetriever,
				logger,
			)
			assert.NoError(t, err)

			// Verify that all returned targets meet the criteria
			for contactType, addresses := range targets {
				for _, addr := range addresses {
					found := false
					for _, contact := range clusterContacts {
						if contact.Type == contactType && contact.Address == addr {
							found = true
							// Verify the randomizedseverity threshold
							if contact.SeverityThreshold != nil {
								assert.LessOrEqual(t, sc.severity, *contact.SeverityThreshold)
							}
							// Verify the randomized category
							if contact.AlertType != "" {
								assert.True(t, contact.AlertType == sc.category || contact.AlertType == AlertTypeCluster, "Category mismatch for %s:[%s]vs[%s]", addr, contact.AlertType, sc.category)
							}
							// Verify the randomized alert name
							if contact.AlertName != "" {
								assert.Equal(t, sc.alertName, contact.AlertName, "Alert name mismatch for %s: %s", addr, contact.AlertName)
							}
						}
					}
					for _, contact := range jobContacts {
						if contact.Type == contactType && contact.Address == addr {
							found = true
							// Verify the randomized severity threshold
							if contact.SeverityThreshold != nil {
								assert.LessOrEqual(t, sc.severity, *contact.SeverityThreshold)
							}
						}
					}
					assert.True(t, found, "Target %s of type %s not found in contacts", addr, contactType)
				}
			}
		})
	}
}

func generateRandomJobContacts(count int) []ContactInfo {
	contacts := make([]ContactInfo, count)

	for i := 0; i < count; i++ {
		contact := generateRandomContact()
		contacts[i] = contact
	}

	return contacts
}

func generateRandomClusterContacts(count int) []ContactInfo {
	contacts := make([]ContactInfo, count)

	for i := 0; i < count; i++ {
		contact := generateRandomContact()

		// 50% chance of having an explicit category
		if rand.Float32() < 0.5 {
			contact.AlertType = getRandomCategory()
		}

		// 20% chance of having an alert name override
		if rand.Float32() < 0.2 {
			contact.AlertName = fmt.Sprintf("SpecificAlert%d", rand.Intn(5))
		}

		contacts[i] = contact
	}

	return contacts
}

func getRandomContactType() ContactType {
	contactTypes := []ContactType{ContactTypeEmail, ContactTypeSlack, ContactTypePagerDuty}
	return contactTypes[rand.Intn(len(contactTypes))]
}

func getRandomCategory() AlertType {
	categories := []AlertType{AlertTypeCluster, AlertTypeJob, ""}
	return categories[rand.Intn(len(categories))]
}

func generateRandomContact() ContactInfo {
	contactType := getRandomContactType()

	contact := ContactInfo{
		Address: generateRandomString(10),
		Type:    contactType,
	}

	// 20% chance of having an alert type override
	if rand.Float32() < 0.2 {
		contact.AlertName = fmt.Sprintf("SpecificAlert%d", rand.Intn(5))
	}

	// Randomly assign a severity threshold
	if rand.Float32() > 0.5 {
		threshold := rand.Intn(5) + 1
		contact.SeverityThreshold = IntPtr(threshold)
	}

	return contact
}

// Helper function to generate random strings for Slack webhook URLs
func generateRandomString(length int) string {
	const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}
	return string(result)
}

// Verify that when we fail to look up notification info for a wsjob, we still notify 'all' or 'job' contacts.
func TestNotificationTargetsFallback(t *testing.T) {
	contacts := []ContactInfo{
		{
			Type:      ContactTypeEmail,
			Address:   "all@example.com",
			AlertType: AlertTypeAll,
		},
		{
			Type:      ContactTypeEmail,
			Address:   "job@example.com",
			AlertType: AlertTypeJob,
		},
		{
			Type:      ContactTypeEmail,
			Address:   "cluster@example.com",
			AlertType: AlertTypeCluster,
		},
	}
	mockRetriever := &mockJobContactInfoRetriever{
		contacts: map[ContactType][]string{
			ContactTypeEmail: {"job-owner@example.com"},
		},
		jobID: "default/test-job",
		err:   assert.AnError,
	}
	targets, err := GetNotificationTargets(
		context.Background(),
		contacts,
		AlertTypeJob,
		1,
		"ClusterCritical",
		nil,
		mockRetriever,
		logrus.New(),
	)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"all@example.com", "job@example.com"}, targets[ContactTypeEmail])
}

// Verify that we look up job contacts with appropriate labels
func TestNotificationTargetsJobLabels(t *testing.T) {
	contacts := []ContactInfo{
		{
			Type:      ContactTypeEmail,
			Address:   "job@example.com",
			AlertType: AlertTypeJob,
		},
	}
	mockRetriever := &mockJobContactInfoRetriever{
		contacts: map[ContactType][]string{
			ContactTypeEmail: {"job-owner@example.com"},
		},
		jobID:     "default/test-job",
		sessionID: "default",
	}

	for _, jobLabel := range alertPayloadJobIDLabels {
		for _, sessionLabel := range alertPayloadSessionIDLabels {
			targets, err := GetNotificationTargets(
				context.Background(),
				contacts,
				AlertTypeJob,
				1,
				"ClusterCritical",
				map[string]string{
					jobLabel:     "default/test-job",
					sessionLabel: "default",
				},
				mockRetriever,
				logrus.New(),
			)
			assert.NoError(t, err)
			assert.ElementsMatch(t, []string{"job-owner@example.com", "job@example.com"}, targets[ContactTypeEmail])
		}
	}
}

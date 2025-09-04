package alertrouter

// This file defines "component" tests, verifying that a webhook triggered with a specific payload
// will send an email to specific recipients.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// mockJobContactInfoRetriever implements JobContactInfoRetriever for testing
type mockJobContactInfoRetriever struct {
	contacts  map[ContactType][]string
	err       error
	jobID     string
	sessionID string
}

func (m *mockJobContactInfoRetriever) GetJobContacts(ctx context.Context, logger logrus.FieldLogger, labels map[string]string, severity int32) (map[ContactType][]string, error) {
	if m.err != nil {
		return nil, m.err
	}
	if labels == nil {
		return make(map[ContactType][]string), nil
	}
	jobID, ok := findLabelValue(labels, alertPayloadJobIDLabels)
	if !ok {
		return nil, fmt.Errorf("jobID not found in labels: %+v", labels)
	}
	sessionID, ok := findLabelValue(labels, alertPayloadSessionIDLabels)
	if !ok {
		return nil, fmt.Errorf("sessionID not found in labels: %+v", labels)
	}
	if jobID == m.jobID && sessionID == m.sessionID {
		return m.contacts, nil
	}
	return nil, fmt.Errorf("jobID or sessionID not found in labels: %+v", labels)
}

// mockMailSender implements a test double for mail.Client
// Note: the atomic counter helps in the stress test below (observed off-by-one/two errors otherwise)
type mockMailSender struct {
	sentMails []struct {
		to      []string
		subject string
		body    string
	}
	sentCount   atomic.Int64
	shouldFail  bool
	failTargets map[string]bool // Map of specific targets that should fail
}

func (m *mockMailSender) SendMail(to []string, subject string, body string) error {
	if m.shouldFail {
		return fmt.Errorf("failed to send email")
	}

	// Check for partial failures
	if m.failTargets != nil {
		for _, recipient := range to {
			if m.failTargets[recipient] {
				return fmt.Errorf("failed to send email to %s", recipient)
			}
		}
	}

	m.sentMails = append(m.sentMails, struct {
		to      []string
		subject string
		body    string
	}{to, subject, body})
	m.sentCount.Add(1)
	return nil
}

func (m *mockMailSender) ClearSentMails() {
	m.sentMails = make([]struct {
		to      []string
		subject string
		body    string
	}, 0)
	m.sentCount.Store(0)
	m.failTargets = nil
}

var clusterEmailRecipient = "ops@example.com"
var clusterSlackRecipient = "https://hooks.slack.com/services/xxx/yyy"
var clusterPagerDutyRecipient = "routing-key-123"

var jobEmailRecipient = "job-owner@example.com"
var jobSlackRecipient = "https://hooks.slack.com/services/aaa/bbb"
var jobPagerDutyRecipient = "routing-key-456"

func TestAlertHandler(t *testing.T) {
	// Create mock contact info retriever that returns a test email
	mockContactInfo := &mockJobContactInfoRetriever{
		contacts: map[ContactType][]string{
			ContactTypeEmail:     {jobEmailRecipient},
			ContactTypeSlack:     {jobSlackRecipient},
			ContactTypePagerDuty: {jobPagerDutyRecipient},
		},
		jobID:     "default/test-job",
		sessionID: "default",
	}

	// Create alert mappings
	rules := &AlertRuleMappings{
		mappings: map[string]AlertRuleMapping{
			"ClusterAlert": {
				Category: AlertTypeCluster,
				Severity: 1, // Critical
			},
			"TestAlert": {
				Category: AlertTypeJob,
				Severity: 2, // Warning
			},
		},
	}

	// Create mock mail sender
	mockMailSender := &mockMailSender{}

	// Create cluster operator contacts
	contacts := []ContactInfo{
		{
			Type:              ContactTypeEmail,
			Address:           clusterEmailRecipient,
			SeverityThreshold: IntPtr(5),
		},
		{
			Type:              ContactTypeSlack,
			Address:           clusterSlackRecipient,
			SeverityThreshold: IntPtr(5),
		},
		{
			Type:              ContactTypePagerDuty,
			Address:           clusterPagerDutyRecipient,
			SeverityThreshold: IntPtr(5),
		},
	}

	startsAt := "2025-02-06T00:00:00Z"
	alertPayloadFmt := `{"alerts":[{"status":"firing","labels":{"alertname":"%s","wsjobID":"default/test-job","sessionID":"default"}, "startsAt": "%s", "annotations": %s}]}`
	MakePayload := func(alertName string) string {
		return fmt.Sprintf(alertPayloadFmt, alertName, startsAt, "{}")
	}
	MakeAnnPayload := func(alertName string, annotations map[string]string, startsAt string) string {
		jsonBytes, err := json.Marshal(annotations)
		if err != nil {
			t.Fatalf("Failed to serialize annotations for alert payload: %v", err)
		}
		return fmt.Sprintf(alertPayloadFmt, alertName, startsAt, string(jsonBytes))
	}
	clusterDomainName := "mycluster.local"

	dashboardAnnotation1 := map[string]string{"dashboard_url": "/d/uid/name?var-a=1&var-b=2"}
	expectedDashUrl1 := map[string]string{"dashboard base URL": "https://grafana.mycluster.local/d/uid/name",
		"dashboard params1": "var-a=1", "dashboard params2": "var-b=2", "dashboard time param": "from=2025-02-05T23%3A45%3A00.001Z"}

	dashboardAnnotation2 := map[string]string{"dashboard_url": "http://custom.com/d/uid/name?var-a=1&var-b=2&from=1234"}
	expectedDashUrl2 := map[string]string{"dashboard base URL": "http://custom.com/d/uid/name",
		"dashboard a param": "var-a=1", "dashboard b param": "var-b=2", "dashboard time param": "from=1234"}

	dashboardAnnotation3 := map[string]string{
		"dashboard_url":         "/d/uid1/name1?var-a=1",
		"another_dashboard_url": "http://custom.com/d/uid2/name2"}
	expectedDashUrl3 := map[string]string{"dashboard1 base URL": "https://grafana.mycluster.local/d/uid1/name1",
		"dashboard2 base URL":  "http://custom.com/d/uid2/name2",
		"dashboard time param": "from=2025-02-05T23%3A45%3A00.001Z"}

	dashboardAnnotation4 := map[string]string{"dashboard_url": "http://custom.com/dash"}
	expectedDashUrl4 := map[string]string{"URL without any params": "http://custom.com/dash\n"}

	// Create alert handler
	handler, err := NewAlertHandler(logrus.New(), clusterDomainName, mockMailSender, mockContactInfo, rules)
	if err != nil {
		t.Fatalf("failed to create handler: %v", err)
	}
	handler.SetClusterOperators(contacts)

	// Initialize notification senders
	mockHTTPClient := &mockHTTPClient{}
	handler.notificationSenders[ContactTypeSlack] = &SlackSender{httpClient: mockHTTPClient}
	handler.notificationSenders[ContactTypePagerDuty] = &PagerDutySender{
		httpClient:  mockHTTPClient,
		apiEndpoint: "https://events.pagerduty.com/v2/enqueue",
	}

	const (
		emailSuccess = iota
		emailSkipped
		emailFailure
		emailPartialFailure // New constant for partial failure testing
	)

	tests := []struct {
		testName                 string
		alertName                string
		payload                  string
		expectedStatus           int
		expectEmail              int
		wantEmailRecipients      []string
		wantNotificationContains string
		wantSlackRecipients      []string
		wantPagerDutyRecipients  []string
		wantAlerts               uint64            // number of alerts we expect to be received
		wantNotifications        uint64            // number of notifications we expect to be sent
		wantFailedNotifications  uint64            // number of notifications we expect to fail to send
		shouldJobLookupFail      bool              // whether we expect the job lookup to fail
		bodyRegex                map[string]string // Map of description=>regex to match the alert message
		// New fields for partial failure testing
		failSpecificTargets map[string]bool // Map of targets that should fail
	}{
		{
			testName:                "cluster alert",
			alertName:               "ClusterAlert",
			payload:                 MakePayload("ClusterAlert"),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailSuccess,
			wantEmailRecipients:     []string{clusterEmailRecipient},
			wantSlackRecipients:     []string{clusterSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient},
			wantAlerts:              1,
			wantNotifications:       3,
			wantFailedNotifications: 0,
		},
		{
			testName:                 "job alert with owner",
			alertName:                "TestAlert",
			payload:                  MakePayload("TestAlert"),
			expectedStatus:           http.StatusOK,
			expectEmail:              emailSuccess,
			wantEmailRecipients:      []string{clusterEmailRecipient, jobEmailRecipient},
			wantSlackRecipients:      []string{clusterSlackRecipient, jobSlackRecipient},
			wantPagerDutyRecipients:  []string{clusterPagerDutyRecipient, jobPagerDutyRecipient},
			wantNotificationContains: "wsjobID: default/test-job",
			wantAlerts:               2,
			wantNotifications:        9, // 6 new
			wantFailedNotifications:  0,
		},
		{
			testName:                "cluster alert with dashboard",
			alertName:               "ClusterAlert",
			payload:                 MakeAnnPayload("ClusterAlert", dashboardAnnotation1, startsAt),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailSuccess,
			wantEmailRecipients:     []string{clusterEmailRecipient},
			wantSlackRecipients:     []string{clusterSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient},
			wantAlerts:              3,
			wantNotifications:       12, // 3 new
			wantFailedNotifications: 0,
			bodyRegex:               expectedDashUrl1,
		},
		{
			testName:                "cluster alert with custom dash url",
			alertName:               "ClusterAlert",
			payload:                 MakeAnnPayload("ClusterAlert", dashboardAnnotation2, startsAt),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailSuccess,
			wantEmailRecipients:     []string{clusterEmailRecipient},
			wantSlackRecipients:     []string{clusterSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient},
			wantAlerts:              4,
			wantNotifications:       15, // 3 new
			wantFailedNotifications: 0,
			bodyRegex:               expectedDashUrl2,
		},
		{
			testName:                "cluster alert with two dash urls",
			alertName:               "ClusterAlert",
			payload:                 MakeAnnPayload("ClusterAlert", dashboardAnnotation3, startsAt),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailSuccess,
			wantEmailRecipients:     []string{clusterEmailRecipient},
			wantSlackRecipients:     []string{clusterSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient},
			wantAlerts:              5,
			wantNotifications:       18, // 3 new
			wantFailedNotifications: 0,
			bodyRegex:               expectedDashUrl3,
		},
		{
			testName:                "cluster alert with empty StartsAt",
			alertName:               "ClusterAlert",
			payload:                 MakeAnnPayload("ClusterAlert", dashboardAnnotation4, ""),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailSuccess,
			wantEmailRecipients:     []string{clusterEmailRecipient},
			wantSlackRecipients:     []string{clusterSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient},
			wantAlerts:              6,
			wantNotifications:       21, // 3 new
			wantFailedNotifications: 0,
			bodyRegex:               expectedDashUrl4,
		},
		{
			testName:                "cluster alert with invalid StartsAt",
			alertName:               "ClusterAlert",
			payload:                 MakeAnnPayload("ClusterAlert", dashboardAnnotation4, "malformated"),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailSuccess,
			wantEmailRecipients:     []string{clusterEmailRecipient},
			wantSlackRecipients:     []string{clusterSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient},
			wantAlerts:              7,
			wantNotifications:       24, // 3 new
			wantFailedNotifications: 0,
			bodyRegex:               expectedDashUrl4,
		},
		{
			testName:                "invalid json",
			payload:                 `{"invalid json`,
			expectedStatus:          http.StatusBadRequest,
			expectEmail:             emailSkipped,
			wantAlerts:              7,
			wantNotifications:       24, // no new
			wantFailedNotifications: 0,
		},
		{
			testName:                "missing job ID in job alert",
			alertName:               "TestAlert",
			payload:                 MakePayload("TestAlert"),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailPartialFailure, // failing only the job-specific part
			wantEmailRecipients:     []string{clusterEmailRecipient},
			wantSlackRecipients:     []string{clusterSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient},
			shouldJobLookupFail:     true,
			wantAlerts:              8,
			wantNotifications:       27, // 3 new
			wantFailedNotifications: 0,
		},
		{
			testName:                "Email Send Failure",
			alertName:               "TestAlert",
			payload:                 MakePayload("TestAlert"),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailFailure,
			wantEmailRecipients:     []string{},
			wantSlackRecipients:     []string{clusterSlackRecipient, jobSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient, jobPagerDutyRecipient},
			wantAlerts:              9,
			wantNotifications:       31,                   // 4 new
			wantFailedNotifications: 2 * (MaxRetries + 1), // Retriable failure, the failure repeats
		},
		{
			testName:                "Partial Email Failure",
			alertName:               "TestAlert",
			payload:                 MakePayload("TestAlert"),
			expectedStatus:          http.StatusOK,
			expectEmail:             emailPartialFailure,
			wantEmailRecipients:     []string{clusterEmailRecipient, jobEmailRecipient},
			wantSlackRecipients:     []string{clusterSlackRecipient, jobSlackRecipient},
			wantPagerDutyRecipients: []string{clusterPagerDutyRecipient, jobPagerDutyRecipient},
			wantAlerts:              10,
			wantNotifications:       36, // 5 new
			wantFailedNotifications: 3 * (MaxRetries + 1),
			failSpecificTargets:     map[string]bool{jobEmailRecipient: true}, // Only job owner email fails
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			mockMailSender.ClearSentMails()
			mockHTTPClient.ClearRequests()
			mockMailSender.shouldFail = tt.expectEmail == emailFailure
			if tt.expectEmail == emailPartialFailure {
				mockMailSender.failTargets = tt.failSpecificTargets
			}
			mockContactInfo.err = nil
			if tt.shouldJobLookupFail {
				mockContactInfo.err = fmt.Errorf("job lookup failed")
			}

			req := httptest.NewRequest(http.MethodPost, "/alerts", bytes.NewBufferString(tt.payload))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)

			if status := rr.Code; status != tt.expectedStatus {
				t.Errorf("handler returned wrong status code: got %v want %v",
					status, tt.expectedStatus)
			}

			// Fuzz delay for channel enqueue/pickup
			time.Sleep(200 * time.Millisecond)

			// Verify stats
			if got := handler.stats.AlertsReceived.Load(); got != tt.wantAlerts {
				t.Errorf("wrong number of alerts received: got %d, want %d", got, tt.wantAlerts)
			}
			if got := handler.stats.NotificationsSent.Load(); got != tt.wantNotifications {
				t.Errorf("wrong number of total notifications sent: got %d, want %d", got, tt.wantNotifications)
			}
			if got := handler.stats.NotificationsFailed.Load(); got != tt.wantFailedNotifications {
				t.Errorf("wrong number of notifications failed: got %d, want %d", got, tt.wantFailedNotifications)
			}
			if tt.wantAlerts > 0 {
				if got := handler.stats.LastAlertTime.Load(); got == 0 {
					t.Error("LastAlertTime not set")
				}
			}
			if tt.wantNotifications > 0 {
				if got := handler.stats.LastNotificationTime.Load(); got == 0 {
					t.Error("LastNotificationTime not set")
				}
			}

			if tt.expectEmail == emailSuccess {
				if len(mockMailSender.sentMails) == 0 {
					t.Error("expected email to be sent, but none was sent")
				} else {
					// Track which recipients we've seen
					seenRecipients := make(map[string]bool)

					// Check each sent email
					for _, sent := range mockMailSender.sentMails {
						if sent.subject != tt.alertName {
							t.Errorf("wrong subject: got %q, want %q", sent.subject, tt.alertName)
						}
						if !strings.Contains(sent.body, "Status: firing") {
							t.Error("email body missing status")
						}
						if tt.wantNotificationContains != "" && !strings.Contains(sent.body, tt.wantNotificationContains) {
							t.Errorf("email body missing expected content %q", tt.wantNotificationContains)
						}
						for description, regexPattern := range tt.bodyRegex {
							regex, err := regexp.Compile(regexPattern)
							if err != nil {
								t.Errorf("Invalid regex pattern from test: %s: %v", regexPattern, err)
							}
							if !regex.MatchString(sent.body) {
								t.Errorf("Alert message doesn't match regex: %s\n", description)
							}
						}

						// Each send should be to exactly one recipient
						if len(sent.to) != 1 {
							t.Errorf("expected exactly one recipient per send, got %d: %v", len(sent.to), sent.to)
						}

						// Mark this recipient as seen
						seenRecipients[sent.to[0]] = true
					}

					// Verify we saw all expected recipients exactly once
					for _, wantRecipient := range tt.wantEmailRecipients {
						if !seenRecipients[wantRecipient] {
							t.Errorf("expected recipient %q was not notified", wantRecipient)
						}
					}

					// Verify we didn't see any unexpected recipients
					for recipient := range seenRecipients {
						found := false
						for _, wantRecipient := range tt.wantEmailRecipients {
							if recipient == wantRecipient {
								found = true
								break
							}
						}
						if !found {
							t.Errorf("unexpected recipient was notified: %q", recipient)
						}
					}
				}
			} else {
				if len(mockMailSender.sentMails) > 0 && tt.expectEmail != emailPartialFailure {
					t.Error("expected no email to be sent, but one was sent")
				}
			}

			if tt.wantSlackRecipients != nil {
				// Verify Slack notifications
				if len(tt.wantSlackRecipients) > 0 {
					slackRequests := 0
					for _, req := range mockHTTPClient.requests {
						if strings.HasPrefix(req.url, "https://hooks.slack.com/") {
							// Verify the webhook URL is one of the expected ones
							found := false
							for _, want := range tt.wantSlackRecipients {
								if req.url == want {
									found = true
									break
								}
							}
							if !found {
								t.Errorf("unexpected Slack webhook URL: %q", req.url)
							}
							slackRequests++
						}
					}

					if slackRequests != len(tt.wantSlackRecipients) {
						t.Errorf("wrong number of Slack notifications: got %d, want %d", slackRequests, len(tt.wantSlackRecipients))
					}
				}
			}
			if tt.wantPagerDutyRecipients != nil {
				// Verify PagerDuty notifications
				if len(tt.wantPagerDutyRecipients) > 0 {
					pdRequests := 0
					expectedURL := "https://events.pagerduty.com/v2/enqueue"
					for _, req := range mockHTTPClient.requests {
						if req.url == expectedURL {
							// Extract and verify the PagerDuty payload
							payloadBytes, err := json.Marshal(req.payload)
							if err != nil {
								t.Fatalf("failed to marshal PagerDuty payload: %v", err)
							}

							var pdPayload pagerDutyPayload
							if err := json.Unmarshal(payloadBytes, &pdPayload); err != nil {
								t.Fatalf("failed to unmarshal PagerDuty payload: %v", err)
							}

							// Verify routing key is one of the expected ones
							found := false
							for _, want := range tt.wantPagerDutyRecipients {
								if pdPayload.RoutingKey == want {
									found = true
									break
								}
							}
							if !found {
								t.Errorf("unexpected PagerDuty routing key: %q", pdPayload.RoutingKey)
							}
							pdRequests++
						}
					}

					if pdRequests != len(tt.wantPagerDutyRecipients) {
						t.Errorf("wrong number of PagerDuty notifications: got %d, want %d", pdRequests, len(tt.wantPagerDutyRecipients))
					}
				}
			}

			// For partial failure tests, verify that only failed targets are retried
			if tt.expectEmail == emailPartialFailure {
				// Check that successful targets were notified exactly once
				successfulTargetCount := make(map[string]int)
				for _, mail := range mockMailSender.sentMails {
					for _, recipient := range mail.to {
						if !tt.failSpecificTargets[recipient] {
							successfulTargetCount[recipient]++
						}
					}
				}
				for target, count := range successfulTargetCount {
					if count > 1 {
						t.Errorf("successful target %s was notified %d times, expected 1", target, count)
					}
				}
			}
		})
	}
}

// TestAlertStress tests the alert router's event queue with a large number of alerts (a "spike")
// We create a large number of alerts and enqueue them in a loop.
// We then check that the mail sender was called the correct number of times.
func TestAlertStress(t *testing.T) {
	timeoutDuration := 10 * time.Second
	testCases := []struct {
		name      string
		numAlerts int
	}{
		{"Small batch", 100},
		{"Medium batch", 1000},
		{"Large batch", 5000},
		{"X-Large batch", 10000},
	}

	// Run test cases sequentially
	for _, tc := range testCases {
		// Sleep between test cases to allow resources to be cleaned up
		time.Sleep(100 * time.Millisecond)

		t.Run(fmt.Sprintf("Sequential_%s", tc.name), func(t *testing.T) {
			// Create a mock mail sender
			mockSender := &mockMailSender{}

			// WLOG, use only a single email address, a cluster-operator contact, and a single mapping rule.
			contacts := []ContactInfo{
				{
					Type:              ContactTypeEmail,
					Address:           clusterEmailRecipient,
					SeverityThreshold: IntPtr(5),
				},
			}
			rules := &AlertRuleMappings{
				mappings: map[string]AlertRuleMapping{
					"ClusterAlert": {
						Category: AlertTypeCluster,
						Severity: 1, // Critical
					},
				},
			}
			clusterDomainName := "mycluster.local"

			// Create an alert handler
			handler, err := NewAlertHandler(logrus.New(), clusterDomainName, mockSender, nil, rules)
			if err != nil {
				t.Fatalf("failed to create handler: %v", err)
			}
			handler.SetClusterOperators(contacts)
			defer handler.StopWorkers()
			startTime := time.Now()
			// Send the alerts through the webhook via HTTP request
			for i := 0; i < tc.numAlerts; i++ {
				payload := `{"alerts":[{"status":"firing","labels":{"alertname":"ClusterAlert"}}]}`
				req := httptest.NewRequest(http.MethodPost, "/alerts", bytes.NewBufferString(payload))
				req.Header.Set("Content-Type", "application/json")
				rr := httptest.NewRecorder()
				handler.ServeHTTP(rr, req)
				// Tiny delay every 100 alerts
				if i%100 == 0 {
					time.Sleep(1 * time.Millisecond)
				}
			}
			sendDuration := time.Since(startTime)
			t.Logf("Time taken to send %d alerts: %s", tc.numAlerts, sendDuration)

			// Wait for all notification emails to be sent, with a timeout
			for mockSender.sentCount.Load() < int64(tc.numAlerts) && time.Since(startTime) < timeoutDuration {
				time.Sleep(10 * time.Millisecond)
				t.Logf("Waiting for %d emails to be sent; queue size: %d", tc.numAlerts-int(mockSender.sentCount.Load()), handler.workQueue.queue.Len())
			}
			processDuration := time.Since(startTime)
			if mockSender.sentCount.Load() < int64(tc.numAlerts) {
				t.Errorf("timed out after %s waiting for %d emails to be sent", processDuration, tc.numAlerts)
			} else {
				t.Logf("Time taken to process %d alerts: %s", tc.numAlerts, processDuration)
			}
			// Note: from a run on my laptop:
			// Time taken to process 10000 alerts: 1.121590625s
		})
	}
}

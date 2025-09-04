package alertrouter

import (
	"encoding/json"
	"fmt"
	"testing"
)

// mockHTTPClient implements HTTPClient for testing
type mockHTTPClient struct {
	requests []struct {
		url     string
		payload interface{}
	}
	shouldFail bool
}

func (m *mockHTTPClient) PostJSON(url string, payload interface{}) error {
	if m.shouldFail {
		return fmt.Errorf("mock HTTP client failure")
	}
	m.requests = append(m.requests, struct {
		url     string
		payload interface{}
	}{url, payload})
	return nil
}

func (m *mockHTTPClient) ClearRequests() {
	m.requests = nil
}

func TestSlackSender(t *testing.T) {
	mockClient := &mockHTTPClient{}
	sender := &SlackSender{httpClient: mockClient}

	tests := []struct {
		name     string
		targets  []string
		subject  string
		message  string
		wantFail bool
		wantURL  string
		wantText string
	}{
		{
			name:     "basic slack message",
			targets:  []string{"https://hooks.slack.com/services/xxx/yyy"},
			subject:  "Test Alert",
			message:  "This is a test message",
			wantURL:  "https://hooks.slack.com/services/xxx/yyy",
			wantText: "Test Alert\n\nThis is a test message",
		},
		{
			name:     "multiple webhooks",
			targets:  []string{"https://hooks.slack.com/services/aaa/bbb", "https://hooks.slack.com/services/ccc/ddd"},
			subject:  "Multi Alert",
			message:  "Testing multiple webhooks",
			wantURL:  "https://hooks.slack.com/services/ccc/ddd", // We'll see the last one
			wantText: "Multi Alert\n\nTesting multiple webhooks",
		},
		{
			name:     "http failure",
			targets:  []string{"https://hooks.slack.com/services/xxx/yyy"},
			subject:  "Failed Alert",
			message:  "This should fail",
			wantFail: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient.shouldFail = tt.wantFail
			err := sender.SendNotification(tt.targets, tt.subject, tt.message)

			if tt.wantFail {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if mockClient.requests[len(mockClient.requests)-1].url != tt.wantURL {
				t.Errorf("wrong URL: got %q, want %q", mockClient.requests[len(mockClient.requests)-1].url, tt.wantURL)
			}

			payload, ok := mockClient.requests[len(mockClient.requests)-1].payload.(SlackPayload)
			if !ok {
				t.Fatalf("expected SlackPayload type but got %T", mockClient.requests[len(mockClient.requests)-1].payload)
			}

			expectedText := fmt.Sprintf("*%s*\n\n%s", tt.subject, tt.message)
			if len(payload.Blocks) != 1 || payload.Blocks[0].Text.Text != expectedText {
				t.Errorf("wrong text: got %q, want %q", payload.Blocks[0].Text.Text, expectedText)
			}
		})
	}
}

func TestPagerDutySender(t *testing.T) {
	mockClient := &mockHTTPClient{}
	sender := &PagerDutySender{
		httpClient:  mockClient,
		apiEndpoint: "https://events.pagerduty.com/v2/enqueue",
	}

	tests := []struct {
		name        string
		targets     []string
		subject     string
		message     string
		wantFail    bool
		wantKey     string
		wantSummary string
	}{
		{
			name:        "basic PagerDuty alert",
			targets:     []string{"routing-key-123"},
			subject:     "Test Alert",
			message:     "This is a test message",
			wantKey:     "routing-key-123",
			wantSummary: "Test Alert",
		},
		{
			name:        "multiple routing keys",
			targets:     []string{"key1", "key2"},
			subject:     "Multi Alert",
			message:     "Testing multiple keys",
			wantKey:     "key2", // We'll see the last one
			wantSummary: "Multi Alert",
		},
		{
			name:     "http failure",
			targets:  []string{"routing-key-123"},
			subject:  "Failed Alert",
			message:  "This should fail",
			wantFail: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockClient.shouldFail = tt.wantFail
			err := sender.SendNotification(tt.targets, tt.subject, tt.message)

			if tt.wantFail {
				if err == nil {
					t.Error("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if mockClient.requests[len(mockClient.requests)-1].url != sender.apiEndpoint {
				t.Errorf("wrong URL: got %q, want %q", mockClient.requests[len(mockClient.requests)-1].url, sender.apiEndpoint)
			}

			// Extract and verify the PagerDuty payload
			payloadBytes, err := json.Marshal(mockClient.requests[len(mockClient.requests)-1].payload)
			if err != nil {
				t.Fatalf("failed to marshal payload: %v", err)
			}

			var pdPayload struct {
				RoutingKey string `json:"routing_key"`
				Payload    struct {
					Summary string `json:"summary"`
				} `json:"payload"`
			}

			if err := json.Unmarshal(payloadBytes, &pdPayload); err != nil {
				t.Fatalf("failed to unmarshal payload: %v", err)
			}

			if pdPayload.RoutingKey != tt.wantKey {
				t.Errorf("wrong routing key: got %q, want %q", pdPayload.RoutingKey, tt.wantKey)
			}

			if pdPayload.Payload.Summary != tt.wantSummary {
				t.Errorf("wrong summary: got %q, want %q", pdPayload.Payload.Summary, tt.wantSummary)
			}
		})
	}
}

package alertrouter

import (
	"fmt"
	"strings"
)

const (
	pagerDutyEventsAPIEndpoint = "https://events.pagerduty.com/v2/enqueue"
)

// NotificationSender defines the interface for sending notifications
type NotificationSender interface {
	SendNotification(targets []string, subject string, message string) error
}

// EmailSender implements NotificationSender for email notifications
type EmailSender struct {
	mailSender MailSender
}

func NewEmailSender(mailSender MailSender) *EmailSender {
	return &EmailSender{mailSender: mailSender}
}

func (s *EmailSender) SendNotification(targets []string, subject string, message string) error {
	return s.mailSender.SendMail(targets, subject, message)
}

// SlackSender implements NotificationSender for Slack notifications
type SlackSender struct {
	httpClient HTTPClient
}

type TextType struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type BlockType struct {
	Type string    `json:"type"`
	Text *TextType `json:"text,omitempty"`
}

type SlackPayload struct {
	Text   string      `json:"text"`
	Blocks []BlockType `json:"blocks"`
}

func NewSlackSender(httpClient HTTPClient) *SlackSender {
	return &SlackSender{
		httpClient: httpClient,
	}
}

func (s *SlackSender) SendNotification(targets []string, subject string, message string) error {
	var errs []error

	payload := SlackPayload{
		Text: subject, // Fallback text for notifications
		Blocks: []BlockType{
			{
				Type: "section",
				Text: &TextType{
					Type: "mrkdwn",
					Text: fmt.Sprintf("*%s*\n\n%s", subject, message),
				},
			},
		},
	}

	for _, webhookURL := range targets {
		if err := s.httpClient.PostJSON(webhookURL, payload); err != nil {
			errs = append(errs, fmt.Errorf("failed to send to %s: %w", webhookURL, err))
			continue
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("notification errors: %v", errs)
	}
	return nil
}

// PagerDutySender implements NotificationSender for PagerDuty notifications
type PagerDutySender struct {
	httpClient  HTTPClient
	apiEndpoint string
}

func NewPagerDutySender(httpClient HTTPClient) *PagerDutySender {
	return &PagerDutySender{
		httpClient:  httpClient,
		apiEndpoint: pagerDutyEventsAPIEndpoint,
	}
}

type pagerDutyPayload struct {
	RoutingKey  string                `json:"routing_key"`
	EventAction string                `json:"event_action"`
	Payload     pagerDutyEventPayload `json:"payload"`
}

type pagerDutyEventPayload struct {
	Summary       string            `json:"summary"`
	Severity      string            `json:"severity"`
	Source        string            `json:"source"`
	CustomDetails map[string]string `json:"custom_details"`
}

func makePagerDutyPayload(routingKey, subject, message string) pagerDutyPayload {
	return pagerDutyPayload{
		RoutingKey:  routingKey,
		EventAction: "trigger",
		Payload: pagerDutyEventPayload{
			Summary:  subject,
			Severity: "critical", // TODO: Make configurable based on alert severity
			Source:   "Alert Router",
			CustomDetails: map[string]string{
				"message": message,
			},
		},
	}
}

func (s *PagerDutySender) SendNotification(targets []string, subject string, message string) error {
	var errs []error

	for _, routingKey := range targets {
		payload := makePagerDutyPayload(routingKey, subject, message)

		if err := s.httpClient.PostJSON(s.apiEndpoint, payload); err != nil {
			errs = append(errs, fmt.Errorf("failed to send notification to PagerDuty (routing_key: %s): %w", routingKey, err))
			continue
		}
	}

	if len(errs) > 0 {
		// Combine all errors into a single detailed error message
		var errMsgs []string
		for _, err := range errs {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("PagerDuty notification errors:\n%s", strings.Join(errMsgs, "\n"))
	}
	return nil
}

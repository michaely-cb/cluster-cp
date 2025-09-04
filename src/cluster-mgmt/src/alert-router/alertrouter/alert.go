package alertrouter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// AlertHandler handles incoming alert webhooks
type AlertHandler struct {
	logger               *logrus.Logger
	clusterDomainName    string
	clusterOperators     *ClusterOperatorContacts
	contactInfoRetriever JobContactInfoRetriever
	rules                *AlertRuleMappings
	stats                *Stats
	workQueue            *AlertWorkQueue
	notificationSenders  map[ContactType]NotificationSender
	httpClient           HTTPClient
}

// NewAlertHandler creates a new AlertHandler instance
// - mailSender is the mail client to use for sending emails
// - clusterOperators is the list of cluster operator notification [with targets, and optional severity threshold, and optional specific rules, and optional job/cluster restriction
// - contactInfoRetriever wraps a wsClient that will query etcd to get notification targets for a given wsjob
// - rules is the classification (severity, category) for each alert
func NewAlertHandler(logger *logrus.Logger, clusterDomainName string, mailSender MailSender, contactInfoRetriever JobContactInfoRetriever, rules *AlertRuleMappings) (*AlertHandler, error) {
	// Create shared HTTP client
	httpClient := NewHTTPClient()

	// Initialize notification senders
	notificationSenders := make(map[ContactType]NotificationSender)
	notificationSenders[ContactTypeEmail] = NewEmailSender(mailSender)
	notificationSenders[ContactTypeSlack] = NewSlackSender(httpClient)
	notificationSenders[ContactTypePagerDuty] = NewPagerDutySender(httpClient)

	// Create stats first since queue needs it
	stats := NewStats()

	handler := &AlertHandler{
		logger:               logger,
		clusterDomainName:    clusterDomainName,
		clusterOperators:     NewClusterOperatorContacts(),
		contactInfoRetriever: contactInfoRetriever,
		rules:                rules,
		stats:                stats,
		workQueue:            NewAlertWorkQueue(logger, stats),
		notificationSenders:  notificationSenders,
		httpClient:           httpClient,
	}
	handler.workQueue.Start()
	return handler, nil
}

// StopWorkers stops the worker goroutines
func (h *AlertHandler) StopWorkers() {
	h.workQueue.Stop()
}

// GetStats returns the stats instance
func (h *AlertHandler) GetStats() *Stats {
	return h.stats
}

// GetClusterOperators returns the current cluster operator contacts
func (h *AlertHandler) GetClusterOperators() []ContactInfo {
	return h.clusterOperators.GetContacts()
}

// SetClusterOperators sets the cluster operator contacts
func (h *AlertHandler) SetClusterOperators(contacts []ContactInfo) {
	h.clusterOperators = &ClusterOperatorContacts{contacts: contacts}
}

// UpdateClusterOperatorsFromConfig updates the cluster operator contacts from config data
func (h *AlertHandler) UpdateClusterOperatorsFromConfig(configData []byte) error {
	return h.clusterOperators.SetContactsFromConfig(h.logger, configData)
}

// AlertManagerPayload represents the webhook payload from Prometheus AlertManager
type AlertManagerPayload struct {
	Alerts []Alert `json:"alerts"`
	// Other fields available but not needed for basic implementation
}

// Alert represents a single alert from AlertManager
type Alert struct {
	Status      string            `json:"status"`
	Labels      map[string]string `json:"labels"`
	Annotations map[string]string `json:"annotations"`
	StartsAt    string            `json:"startsAt"`
	EndsAt      string            `json:"endsAt"`
}

// HandleWebhook processes incoming webhook requests
func (h *AlertHandler) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	var payload AlertManagerPayload

	// Log useful request metadata instead of the opaque request body
	h.logger.WithFields(logrus.Fields{
		"method":         r.Method,
		"remote_ip":      r.RemoteAddr,
		"user_agent":     r.UserAgent(),
		"path":           r.URL.Path,
		"content_length": r.ContentLength,
	}).Info("Received webhook request")

	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		h.logger.WithFields(logrus.Fields{
			"error": err.Error(),
		}).Error("Failed to decode webhook payload")
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	h.stats.IncrementAlertsReceived()

	for _, alert := range payload.Alerts {
		// Log useful alert information with structured fields
		logFields := logrus.Fields{
			"status":    alert.Status,
			"alertname": alert.Labels["alertname"],
			"starts_at": alert.StartsAt,
		}

		// Add a few optional fields if they exist
		if severity, ok := alert.Labels["severity"]; ok {
			logFields["severity"] = severity
		}
		if job, ok := alert.Labels["job"]; ok {
			logFields["job"] = job
		}
		if instance, ok := alert.Labels["instance"]; ok {
			logFields["instance"] = instance
		}
		if cluster, ok := alert.Labels["cluster"]; ok {
			logFields["cluster"] = cluster
		}
		if summary, ok := alert.Annotations["summary"]; ok {
			logFields["summary"] = summary
		}

		h.logger.WithFields(logFields).Info("Processing alert")

		// Get alert rule mapping
		alertName := alert.Labels["alertname"]
		mapping, ok := h.rules.GetMapping(alertName)
		if !ok {
			h.logger.WithFields(logrus.Fields{
				"alertname": alertName,
			}).Warn("No mapping found for alert, will use defaults")
			mapping = h.rules.GetDefaultMapping(alertName)
		}

		// Queue the alert for processing
		h.workQueue.Enqueue(&AlertWorkItem{
			alert:               alert,
			mapping:             &mapping,
			handler:             h,
			notificationTargets: nil, // Will be populated on first processing attempt
		})
	}

	w.WriteHeader(http.StatusOK)
}

func formatDashboardUrl(logger *logrus.Entry, alert Alert, dashUrl string, clusterDomainName string) string {
	u, err := url.Parse(dashUrl)
	if err != nil {
		return ""
	}
	// Set the domain name, if it's empty
	if u.Host == "" {
		u.Host = "grafana." + clusterDomainName
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	// Set dashboard time range, if not already present
	params := u.Query()
	if !(params.Has("from") || params.Has("to")) {
		if alert.StartsAt == "" {
			logger.Warning("Alert missing StartsAt field")
		} else {
			startTs, err := time.Parse(time.RFC3339, alert.StartsAt)
			if err == nil {
				// Show from 15 minutes earlier, to give more context
				// +1ms forces the TS to include decimals (00:00.001Z), which Grafana expects
				startTs := startTs.Add((-15 * time.Minute) + time.Millisecond)
				params.Add("from", startTs.Format(time.RFC3339Nano))
				u.RawQuery = params.Encode()
			} else {
				logger.WithError(err).Error("Failed to parse StartsAt timestamp")
			}
		}
	}
	return u.String()
}

func formatAlertMessage(logger *logrus.Entry, item *AlertWorkItem) string {
	var sb strings.Builder
	alert := item.alert

	sb.WriteString("Alert Details:\n\n")
	sb.WriteString(fmt.Sprintf("Status: %s\n", alert.Status))
	sb.WriteString(fmt.Sprintf("Started: %s\n", alert.StartsAt))

	if alert.EndsAt != "" {
		sb.WriteString(fmt.Sprintf("Ended: %s\n", alert.EndsAt))
	}

	sb.WriteString("\nLabels:\n")
	for k, v := range alert.Labels {
		sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}

	dashUrls := []string{}

	sb.WriteString("\nAnnotations:\n")
	for k, v := range alert.Annotations {
		if strings.Contains(k, "dashboard_url") {
			dashUrl := formatDashboardUrl(logger, alert, v, item.handler.clusterDomainName)
			if dashUrl != "" {
				dashUrls = append(dashUrls, dashUrl)
			}
			continue
		}
		sb.WriteString(fmt.Sprintf("%s: %s\n", k, v))
	}

	if len(dashUrls) > 0 {
		sb.WriteString("\nDashboards:\n")
	}
	for _, u := range dashUrls {
		sb.WriteString(fmt.Sprintf(" - %s\n", u))
	}

	return sb.String()
}

// ServeHTTP implements the http.Handler interface
func (h *AlertHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	h.HandleWebhook(w, r)
}

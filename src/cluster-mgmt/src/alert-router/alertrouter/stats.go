package alertrouter

import (
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	alertsReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alertrouter_alerts_received_total",
		Help: "Total number of alerts received by the alert router",
	})
	alertsDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alertrouter_alerts_dropped_total",
		Help: "Total number of alerts that were failed to be processed by the alert router",
	})
	notificationsSentTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alertrouter_notifications_sent_total",
		Help: "Total number of notifications successfully sent",
	})
	notificationsSkippedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alertrouter_notifications_skipped_total",
		Help: "Total number of notifications skipped",
	})
	notificationsFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alertrouter_notifications_failed_total",
		Help: "Total number of notifications that failed to send",
	})
	lastAlertTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "alertrouter_last_alert_timestamp_seconds",
		Help: "Unix timestamp of the last received alert",
	})
	lastNotificationTimestamp = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "alertrouter_last_notification_timestamp_seconds",
		Help: "Unix timestamp of the last sent notification",
	})
	uptimeSeconds = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "alertrouter_uptime_seconds",
		Help: "Time since the alert router started in seconds",
	})
	// Queue metrics
	queueLengthCurrent = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "alertrouter_queue_length_current",
		Help: "Current number of alerts waiting to be processed",
	})
	alertsQueuedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alertrouter_alerts_queued_total",
		Help: "Total number of alerts that have been queued for processing",
	})
	alertProcessingDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "alertrouter_alert_processing_duration_seconds",
		Help:    "Time taken to process alerts",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	})
	alertsInProcessingGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "alertrouter_alerts_in_processing",
		Help: "Current number of alerts being processed",
	})
	alertProcessingCompletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "alertrouter_alert_processing_completed_total",
		Help: "Total number of alerts that completed processing",
	})
)

// Stats tracks runtime statistics for the alert router
type Stats struct {
	StartTime            time.Time     `json:"start_time"`
	AlertsReceived       atomic.Uint64 `json:"alerts_received"`
	AlertsDropped        atomic.Uint64 `json:"alerts_dropped"`
	NotificationsSent    atomic.Uint64 `json:"notifications_sent"`
	NotificationsSkipped atomic.Uint64 `json:"notifications_skipped"`
	NotificationsFailed  atomic.Uint64 `json:"notifications_failed"`
	LastAlertTime        atomic.Int64  `json:"last_alert_time"`
	LastNotificationTime atomic.Int64  `json:"last_notification_time"`
	// Queue stats
	AlertsQueued             atomic.Uint64 `json:"alerts_queued"`
	AlertsInProcessing       atomic.Int32  `json:"alerts_in_processing"`
	AlertsProcessingComplete atomic.Uint64 `json:"alerts_processing_complete"`
	CurrentQueueLength       atomic.Int32  `json:"current_queue_length"`
}

// NewStats creates a new Stats instance
func NewStats() *Stats {
	return &Stats{
		StartTime: time.Now(),
	}
}

// IncrementAlertsDropped increments the alerts dropped counter
func (s *Stats) IncrementAlertsDropped() {
	s.AlertsDropped.Add(1)
	alertsDroppedTotal.Inc()
}

// IncrementAlertsReceived increments the alerts received counter
func (s *Stats) IncrementAlertsReceived() {
	s.AlertsReceived.Add(1)
	now := time.Now()
	s.LastAlertTime.Store(now.Unix())
	alertsReceivedTotal.Inc()
	lastAlertTimestamp.Set(float64(now.Unix()))
}

// IncrementNotificationsSent increments the notifications sent counter
func (s *Stats) IncrementNotificationsSent(n int) {
	s.NotificationsSent.Add(uint64(n))
	now := time.Now()
	s.LastNotificationTime.Store(now.Unix())
	notificationsSentTotal.Add(float64(n))
	lastNotificationTimestamp.Set(float64(now.Unix()))
}

// IncrementNotificationsSkipped increments the notifications skipped counter
func (s *Stats) IncrementNotificationsSkipped() {
	s.NotificationsSkipped.Add(1)
	notificationsSkippedTotal.Inc()
}

// IncrementNotificationsFailed increments the notifications failed counter
func (s *Stats) IncrementNotificationsFailed(n int) {
	s.NotificationsFailed.Add(uint64(n))
	notificationsFailedTotal.Add(float64(n))
}

// GetUptime returns the uptime duration
func (s *Stats) GetUptime() time.Duration {
	uptime := time.Since(s.StartTime)
	uptimeSeconds.Set(uptime.Seconds())
	return uptime
}

// StatusResponse represents the complete status response
type StatusResponse struct {
	Uptime               string                      `json:"uptime"`
	AlertsReceived       uint64                      `json:"alerts_received"`
	NotificationsSent    uint64                      `json:"notifications_sent"`
	NotificationsSkipped uint64                      `json:"notifications_skipped"`
	NotificationsFailed  uint64                      `json:"notifications_failed"`
	LastAlertTime        *time.Time                  `json:"last_alert_time,omitempty"`
	LastNotificationTime *time.Time                  `json:"last_notification_time,omitempty"`
	LoadedRules          map[string]AlertRuleMapping `json:"loaded_rules"`
	LoadedContacts       int                         `json:"loaded_contacts"`
	// Queue status
	CurrentQueueLength       int32  `json:"current_queue_length"`
	AlertsQueued             uint64 `json:"alerts_queued"`
	AlertsInProcessing       int32  `json:"alerts_in_processing"`
	AlertsProcessingComplete uint64 `json:"alerts_processing_complete"`
}

// GetStatusResponse returns a complete status response
func (s *Stats) GetStatusResponse(rules *AlertRuleMappings, contacts []ContactInfo) StatusResponse {
	response := StatusResponse{
		Uptime:               s.GetUptime().String(),
		AlertsReceived:       s.AlertsReceived.Load(),
		NotificationsSent:    s.NotificationsSent.Load(),
		NotificationsSkipped: s.NotificationsSkipped.Load(),
		NotificationsFailed:  s.NotificationsFailed.Load(),
		LoadedRules:          rules.mappings,
		LoadedContacts:       len(contacts),
		// Queue status
		CurrentQueueLength:       s.CurrentQueueLength.Load(),
		AlertsQueued:             s.AlertsQueued.Load(),
		AlertsInProcessing:       s.AlertsInProcessing.Load(),
		AlertsProcessingComplete: s.AlertsProcessingComplete.Load(),
	}

	lastAlertTime := s.LastAlertTime.Load()
	if lastAlertTime > 0 {
		t := time.Unix(lastAlertTime, 0)
		response.LastAlertTime = &t
	}

	lastNotificationTime := s.LastNotificationTime.Load()
	if lastNotificationTime > 0 {
		t := time.Unix(lastNotificationTime, 0)
		response.LastNotificationTime = &t
	}

	return response
}

// UpdateQueueLength updates the current queue length metric
func (s *Stats) UpdateQueueLength(length int32) {
	s.CurrentQueueLength.Store(length)
	queueLengthCurrent.Set(float64(length))
}

// IncrementAlertsQueued increments the alerts queued counter
func (s *Stats) IncrementAlertsQueued() {
	s.AlertsQueued.Add(1)
	alertsQueuedTotal.Inc()
}

// AlertProcessingStarted increments the in-processing counter
func (s *Stats) AlertProcessingStarted() {
	s.AlertsInProcessing.Add(1)
	alertsInProcessingGauge.Inc()
}

// AlertProcessingCompleted decrements the in-processing counter and records completion
func (s *Stats) AlertProcessingCompleted(duration time.Duration) {
	s.AlertsInProcessing.Add(-1)
	s.AlertsProcessingComplete.Add(1)
	alertsInProcessingGauge.Dec()
	alertProcessingCompletedTotal.Inc()
	alertProcessingDuration.Observe(duration.Seconds())
}

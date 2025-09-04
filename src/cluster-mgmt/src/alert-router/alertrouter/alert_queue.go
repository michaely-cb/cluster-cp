package alertrouter

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/client-go/util/workqueue"
)

// TODO: add env-varaiables to configure queue-buffer-size and number of workers [defaults: 1024, 5]
const (
	DefaultNumWorkers = 10
	MaxRetries        = 5 // Maximum number of retries for retriable errors
)

// AlertError represents an error that occurred while processing an alert
type AlertError struct {
	err error
	// retriable indicates whether this error should trigger a retry
	retriable bool
}

func (e *AlertError) Error() string {
	return e.err.Error()
}

func newRetriableError(err error) *AlertError {
	return &AlertError{err: err, retriable: true}
}

func newNonRetriableError(err error) *AlertError {
	return &AlertError{err: err, retriable: false}
}

// AlertWorkItem represents a single alert to be processed
type AlertWorkItem struct {
	alert   Alert
	mapping *AlertRuleMapping
	handler *AlertHandler // Reference to parent handler for accessing all other fields
	// Store notification targets to avoid recalculating them on retries
	notificationTargets map[ContactType][]string
}

// NotificationResult tracks the success/failure of notifications per target
type NotificationResult struct {
	successfulTargets []string
	failedTargets     []string
	err               error
}

// AlertWorkQueue handles asynchronous processing of alerts
type AlertWorkQueue struct {
	queue    workqueue.RateLimitingInterface
	workers  int
	wg       sync.WaitGroup
	shutdown chan struct{}
	logger   *logrus.Logger
	stats    *Stats
}

// NewAlertWorkQueue creates a new AlertWorkQueue with the specified number of workers
func NewAlertWorkQueue(logger *logrus.Logger, stats *Stats) *AlertWorkQueue {
	return &AlertWorkQueue{
		queue:    workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter()),
		workers:  DefaultNumWorkers,
		shutdown: make(chan struct{}),
		logger:   logger,
		stats:    stats,
	}
}

// Start initializes the worker goroutines
func (q *AlertWorkQueue) Start() {
	for i := 0; i < q.workers; i++ {
		q.wg.Add(1)
		go q.worker()
	}
}

// Stop gracefully shuts down the queue and waits for workers to finish
func (q *AlertWorkQueue) Stop() {
	q.queue.ShutDown()
	close(q.shutdown)
	q.wg.Wait()
}

// Enqueue adds an alert to the work queue
func (q *AlertWorkQueue) Enqueue(item *AlertWorkItem) {
	q.queue.Add(item)
	q.stats.IncrementAlertsQueued()
	q.stats.UpdateQueueLength(int32(q.queue.Len()))
}

// worker processes items from the queue
func (q *AlertWorkQueue) worker() {
	defer q.wg.Done()

	for {
		obj, shutdown := q.queue.Get()
		if shutdown {
			return
		}

		item, ok := obj.(*AlertWorkItem)
		if !ok {
			q.logger.Error("Failed to process non-AlertWorkItem")
			q.queue.Done(obj)
			continue
		}

		// Track processing start
		q.stats.AlertProcessingStarted()
		startTime := time.Now()

		err := q.processAlert(item)

		// Track processing completion
		q.stats.AlertProcessingCompleted(time.Since(startTime))

		if err != nil {
			// Check if error is retriable and hasn't exceeded max retries
			if alertErr, ok := err.(*AlertError); ok && alertErr.retriable {
				numRequeues := q.queue.NumRequeues(obj)
				if numRequeues < MaxRetries {
					q.logger.WithError(err).WithFields(logrus.Fields{
						"alert":      item.alert.Labels["alertname"],
						"retry":      numRequeues + 1,
						"maxRetries": MaxRetries,
					}).Warn("Failed to process alert, requeueing")
					q.queue.AddRateLimited(obj) // Requeue the original object to maintain identity
				} else {
					q.logger.WithError(err).WithFields(logrus.Fields{
						"alert":      item.alert.Labels["alertname"],
						"maxRetries": MaxRetries,
					}).Error("Failed to process alert after max retries, dropping")
					item.handler.stats.IncrementAlertsDropped()
					q.queue.Forget(obj)
				}
			} else {
				// Non-retriable error or unknown error type, don't requeue
				q.logger.WithError(err).WithFields(logrus.Fields{
					"alert": item.alert.Labels["alertname"],
				}).Error("Failed to process alert with non-retriable error, dropping")
				item.handler.stats.IncrementAlertsDropped()
				q.queue.Forget(obj)
			}
		} else {
			// If successful, forget the item
			q.queue.Forget(obj)
		}
		q.queue.Done(obj)

		// Update queue length after processing
		q.stats.UpdateQueueLength(int32(q.queue.Len()))
	}
}

// processAlert handles the actual alert processing
func (q *AlertWorkQueue) processAlert(item *AlertWorkItem) error {
	ctx := context.Background()
	logger := item.handler.logger.WithFields(logrus.Fields{
		"alert":                       item.alert,
		"existingNotificationTargets": item.notificationTargets,
	})

	// Create message from alert
	subject := item.alert.Labels["alertname"]
	message := formatAlertMessage(logger, item)

	// Use category from mapping
	category := item.mapping.Category

	// Only fetch notification targets if they haven't been fetched yet
	if item.notificationTargets == nil {
		var err error
		item.notificationTargets, err = GetNotificationTargets(
			ctx,
			item.handler.clusterOperators.GetContacts(),
			category,
			item.mapping.Severity,
			item.alert.Labels["alertname"],
			item.alert.Labels,
			item.handler.contactInfoRetriever,
			logger,
		)
		if err != nil {
			logger.WithError(err).Error("Failed to get notification targets")
			// Configuration/lookup errors are not retriable
			return newNonRetriableError(fmt.Errorf("failed to get notification targets: %w", err))
		}
	}

	var anyErrors error
	hasFailures := false

	// Send notifications for each type
	for contactType, targets := range item.notificationTargets {
		if len(targets) == 0 {
			continue
		}

		sender, ok := item.handler.notificationSenders[contactType]
		if !ok {
			logger.WithFields(logrus.Fields{
				"type": contactType,
			}).Error("No notification sender configured for type")
			item.handler.stats.IncrementNotificationsSkipped()
			// Configuration errors are not retriable
			return newNonRetriableError(fmt.Errorf("no notification sender configured for type %s", contactType))
		}

		logger.WithFields(logrus.Fields{
			"type":     contactType,
			"targets":  targets,
			"category": category,
			"severity": item.mapping.Severity,
		}).Info("Sending notification")

		result := sendNotificationsToTargets(logger, sender, targets, subject, message)
		if result.err != nil {
			logger.WithFields(logrus.Fields{
				"error":          result.err,
				"type":           contactType,
				"failed_targets": result.failedTargets,
			}).Error("Failed to send notification to some targets")
			item.handler.stats.IncrementNotificationsFailed(len(result.failedTargets))
			anyErrors = result.err
			hasFailures = true
		}

		// Track successful notifications for stats
		if len(result.successfulTargets) > 0 {
			item.handler.stats.IncrementNotificationsSent(len(result.successfulTargets))
		}

		// Update the remaining targets for this contact type
		if len(result.failedTargets) > 0 {
			item.notificationTargets[contactType] = result.failedTargets
		} else {
			// All targets succeeded, remove this contact type from the map
			delete(item.notificationTargets, contactType)
		}
	}

	if hasFailures {
		return newRetriableError(anyErrors)
	}
	return nil
}

// Wrapper for sending notification and handling panics as errors
func sendNotificationWrapper(logger *logrus.Entry, sender NotificationSender, targets []string, subject, message string) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic recovered in sendNotificationWrapper: %v", r)
			logger.WithError(err).WithFields(logrus.Fields{
				"targets": targets,
				"subject": subject,
				"message": message,
			}).Error("Failed to send notification")
		}
	}()

	return sender.SendNotification(targets, subject, message)
}

// sendNotificationsToTargets sends notifications to a list of targets and returns which ones succeeded/failed
func sendNotificationsToTargets(logger *logrus.Entry, sender NotificationSender, targets []string, subject, message string) NotificationResult {
	result := NotificationResult{
		successfulTargets: make([]string, 0, len(targets)),
		failedTargets:     make([]string, 0),
	}

	// Try each target individually to track which ones succeed/fail
	for _, target := range targets {
		if err := sendNotificationWrapper(logger, sender, []string{target}, subject, message); err != nil {
			result.failedTargets = append(result.failedTargets, target)
			result.err = err // Store the last error
		} else {
			result.successfulTargets = append(result.successfulTargets, target)
		}
	}

	return result
}

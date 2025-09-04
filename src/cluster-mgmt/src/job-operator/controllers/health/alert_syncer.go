package health

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/alertmanager/api/v2/client/alert"
	"github.com/prometheus/alertmanager/api/v2/models"
	"github.com/prometheus/alertmanager/cli"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

const scheduleAlertFilter = "schedule_critical=true"

// AlertSyncer is an interface designed to be run by a manager.Manager
// that also provides a AlertCache interface
type AlertSyncer interface {
	manager.LeaderElectionRunnable
	manager.Runnable
	AlertCache
	// SyncOnce enables the systemVersionCache to be initialized before use by the Manager
	SyncOnce()
}

type alertSyncer struct {
	sync.RWMutex
	alertClient alertGetter
	log         logr.Logger

	cache       map[string]models.GettableAlerts
	retrievedAt v1.Time
	lastErr     error
}

type alertGetter interface {
	GetAlerts(params *alert.GetAlertsParams, opts ...alert.ClientOption) (*alert.GetAlertsOK, error)
}

// NewAlertSyncer provides an implementation of AlertSyncer that gets alerts at syncInterval
func NewAlertSyncer(log logr.Logger) (AlertSyncer, error) {
	parsedURL, err := url.Parse(fmt.Sprintf("http://%s.%s:9093", alertManagerServiceName, alertManagerNamespace))
	if err != nil {
		log.Error(err, "unable to parse alertmanager svc url")
		return nil, err
	}
	alertClient := cli.NewAlertmanagerClient(parsedURL).Alert
	return &alertSyncer{
		log:         log,
		alertClient: alertClient,
		cache:       map[string]models.GettableAlerts{},
	}, nil
}

func (s *alertSyncer) NeedLeaderElection() bool {
	return true
}

func (s *alertSyncer) Start(ctx context.Context) error {
	// Jitter returns a time.Duration between duration and duration + maxFactor * duration.
	// This is to avoid node/system reconcile always starts at the same time of alert sync
	// which could lead to reconcile waiting for an extra cycle.
	wait.JitterUntil(s.SyncOnce, syncInterval/2, 1, false, ctx.Done())
	return nil
}

func (s *alertSyncer) SyncOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), syncInterval)
	defer cancel()

	// get cluster mgmt alerts only
	params := &alert.GetAlertsParams{
		Context: ctx,
		Filter:  []string{scheduleAlertFilter},
	}

	s.Lock()
	defer s.Unlock()
	var resp *alert.GetAlertsOK
	s.cache = map[string]models.GettableAlerts{}
	resp, s.lastErr = s.alertClient.GetAlerts(params)
	if s.lastErr != nil {
		log.Log.Error(s.lastErr, "failed to pull alerts")
	} else {
		if len(resp.Payload) > 0 {
			log.Log.V(1).Info("got alerts", "len", len(resp.Payload))
			for _, al := range resp.Payload {
				log.Log.Info("alert detected", "alert", al)
				instance := al.Labels["instance"]
				if instance == "" {
					continue
				}
				if vals, ok := s.cache[instance]; ok {
					s.cache[instance] = append(vals, al)
				} else {
					s.cache[instance] = models.GettableAlerts{al}
				}
			}
		}
	}
	s.retrievedAt = v1.NewTime(time.Now())
}

// AlertCache outlines an interface to interact with cached alerts
type AlertCache interface {
	// Get will return the currently cached alerts for a given node/system.
	// An error will be returned if the systemVersionCache is not populated, node specific filters
	// cannot be run, or if the last retrieval resulted in an error. The time
	// returned is the time of the last retrieval attempt.
	Get(names ...string) (models.GettableAlerts, v1.Time, error)
}

func (s *alertSyncer) Get(names ...string) (models.GettableAlerts, v1.Time, error) {
	s.RLock()
	defer s.RUnlock()
	if s.retrievedAt.IsZero() {
		return nil, s.retrievedAt, errors.New("alert cache is not yet initialized yet")
	}
	if s.lastErr != nil {
		return nil, s.retrievedAt, s.lastErr
	}
	var res models.GettableAlerts
	for _, n := range names {
		if n != "" {
			res = append(res, s.cache[n]...)
		}
	}
	return res, s.retrievedAt, s.lastErr
}

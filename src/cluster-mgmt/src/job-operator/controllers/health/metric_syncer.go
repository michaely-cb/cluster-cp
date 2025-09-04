package health

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	wscommon "cerebras.com/job-operator/common"
)

// MetricSyncer is an interface designed to be run by a manager.Manager
// that also provides a MetricCache interface
type MetricSyncer interface {
	manager.LeaderElectionRunnable
	manager.Runnable
	MetricCache
	// SyncOnce enables the systemVersionCache to be initialized before use by the Manager
	SyncOnce()
}

type metricSyncer struct {
	sync.RWMutex
	metricsQuerier wscommon.MetricsQuerier
	log            logr.Logger

	systemVersionCache map[string]string
	retrievedAt        v1.Time
}

// NewMetricSyncer provides an implementation of MetricSyncer that gets metrics at syncInterval
func NewMetricSyncer(log logr.Logger, metricsQuerier wscommon.MetricsQuerier) MetricSyncer {
	return &metricSyncer{
		log:                log,
		systemVersionCache: map[string]string{},
		metricsQuerier:     metricsQuerier,
	}
}

func (s *metricSyncer) NeedLeaderElection() bool {
	return true
}

func (s *metricSyncer) Start(ctx context.Context) error {
	// sync every metricSyncInterval
	wait.JitterUntil(s.SyncOnce, metricSyncInterval, 0, false, ctx.Done())
	return nil
}

func (s *metricSyncer) SyncOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), syncInterval)
	defer cancel()

	versions, _ := wscommon.GetSystemVersions(ctx, s.metricsQuerier, nil, true, false)
	if len(versions) > 0 {
		s.Lock()
		defer s.Unlock()
		s.systemVersionCache = versions
		s.log.V(1).Info("system versions fetched", "len", len(versions))
	}
	s.retrievedAt = v1.NewTime(time.Now())
}

// MetricCache outlines an interface to interact with cached metrics
type MetricCache interface {
	// Return last fetched system version metric
	GetSystemVersion(name string) string
}

func (s *metricSyncer) GetSystemVersion(name string) string {
	s.RLock()
	defer s.RUnlock()
	return s.systemVersionCache[name]
}

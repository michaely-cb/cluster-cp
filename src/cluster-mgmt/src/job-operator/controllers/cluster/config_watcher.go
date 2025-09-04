package cluster

import (
	"context"
	"sync"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-logr/logr"
	"golang.org/x/exp/slices"

	"cerebras.com/job-operator/common"
)

type ApplyCfgChange func(ctx context.Context, cfg *common.ClusterConfig) error

type CfgChangeWatcher struct {
	logger logr.Logger

	cfgCh         chan common.ConfigUpdateEvent
	reasons       []common.ConfigUpdateReason
	mu            sync.Mutex
	lastAppliedRV string // track last-applied for test

	applyChangeFn ApplyCfgChange
}

func newCfgChangeWatcher(logger logr.Logger, fn ApplyCfgChange, reasons []common.ConfigUpdateReason) CfgChangeWatcher {
	return CfgChangeWatcher{
		logger:        logger,
		cfgCh:         make(chan common.ConfigUpdateEvent, 10),
		applyChangeFn: fn,
		reasons:       reasons,
	}
}

func (c *CfgChangeWatcher) GetCfgCh() chan common.ConfigUpdateEvent { return c.cfgCh }

func (c *CfgChangeWatcher) Start(ctx context.Context) error {
	var changeHandlerCtx context.Context
	var cancelLastChange context.CancelFunc
	cancelLastChange = func() {} // initial no-op to avoid if logic in select {}

	for {
		select {
		case event := <-c.cfgCh:
			if !slices.Contains(c.reasons, event.Reason) {
				continue
			}

			cancelLastChange() // stop in-progress update if any

			// start a cfg apply go routine with retries
			changeHandlerCtx, cancelLastChange = context.WithCancel(ctx)
			expBackoffCtx := backoff.WithContext(backoff.NewExponentialBackOff(), changeHandlerCtx)
			applyCfgOp := func() error { return c.applyChange(changeHandlerCtx, event.Cfg) }
			applyCfgOpWithRetries := func() { _ = backoff.Retry(applyCfgOp, expBackoffCtx) }

			go applyCfgOpWithRetries()

		case <-ctx.Done():
			cancelLastChange()
			return nil
		}
	}
}

// applyChange prevents concurrent applies due to repeated config updates during an existing apply
func (c *CfgChangeWatcher) applyChange(ctx context.Context, cfg *common.ClusterConfig) error {
	if common.SystemNamespace != common.Namespace {
		return nil
	}
	log := c.logger.WithValues("rv", cfg.ResourceVersion)

	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-ctx.Done():
		// our context may have been canceled while we wait for the mu
		log.Info("applyChange canceled")
		return nil
	default:
		err := c.applyChangeFn(ctx, cfg)
		if err != nil {
			log.WithValues("error", err.Error()).Info("failed to apply config change")
		} else {
			c.lastAppliedRV = cfg.ResourceVersion
		}
		return err
	}
}

package common

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	// This backoff will wait a max of ~5.5 seconds before giving up based on params below
	// Time waiting after each failure = .5, 0.7, 0.98, 1.372, 1.9208
	// Total time waiting for backoff = .5 + 0.7 + 0.98 + 1.372 + 1.9208 = 5.4728 seconds
	defaultBackoff = wait.Backoff{
		Duration: 500 * time.Millisecond,
		Factor:   1.4,
		Jitter:   0.1,
		Steps:    5,
	}

	defaultTimeout = 6 * time.Second
)

type K8sOperation func(ctx context.Context) error

func CreateWithRetryAndTimeout(ctx context.Context, c client.Client, obj client.Object, options ...client.CreateOption) error {
	createOperation := func(ctx context.Context) error {
		return c.Create(ctx, obj, options...)
	}
	return withRetryAndTimeout(ctx, createOperation)
}

func PatchWithRetryAndTimeout(ctx context.Context, c client.Client, obj client.Object, patch client.Patch, options ...client.PatchOption) error {
	patchOperation := func(ctx context.Context) error {
		return c.Patch(ctx, obj, patch, options...)
	}

	return withRetryAndTimeout(ctx, patchOperation)
}

func withRetryAndTimeout(ctx context.Context, operation K8sOperation) error {
	timedCtx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	var lastErr error
	err := wait.ExponentialBackoffWithContext(timedCtx, defaultBackoff, func(innerCtx context.Context) (bool, error) {
		lastErr = operation(innerCtx)
		return lastErr == nil, nil
	})

	if wait.Interrupted(err) && lastErr != nil {
		return lastErr
	}

	return err
}

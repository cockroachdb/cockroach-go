// Copyright 2025 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package crdb

import (
	"time"
)

// RetryFunc owns the state for a transaction retry operation. Usually, this is
// just the retry count. RetryFunc is not assumed to be safe for concurrent use.
//
// The function is called after each retryable error to determine whether to
// retry and how long to wait. It receives the retryable error that triggered
// the retry attempt.
//
// Return values:
//   - duration: The delay to wait before the next retry attempt. If 0, retry
//     immediately without delay.
//   - error: If non-nil, stops retrying and returns this error to the caller
//     (typically a MaxRetriesExceededError). If nil, the retry will proceed
//     after the specified duration.
//
// Example behavior:
//   - (100ms, nil): Wait 100ms, then retry
//   - (0, nil): Retry immediately
//   - (0, err): Stop retrying, return err to caller
type RetryFunc func(err error) (time.Duration, error)

// RetryPolicy constructs a new instance of a RetryFunc for each transaction
// it is used with. Instances of RetryPolicy can likely be immutable and
// should be safe for concurrent calls to NewRetry.
type RetryPolicy interface {
	NewRetry() RetryFunc
}

const (
	// NoRetries is a sentinel value for LimitBackoffRetryPolicy.RetryLimit
	// indicating that no retries should be attempted. When a policy has
	// RetryLimit set to NoRetries, the transaction will be attempted only
	// once, and any retryable error will immediately return a
	// MaxRetriesExceededError.
	//
	// Use WithNoRetries(ctx) to create a context with this behavior.
	NoRetries = -1

	// UnlimitedRetries indicates that retries should continue indefinitely
	// until the transaction succeeds or a non-retryable error occurs. This
	// is represented by setting RetryLimit to 0.
	//
	// Use WithMaxRetries(ctx, 0) to create a context with unlimited retries,
	// though this is generally not recommended in production as it can lead
	// to infinite retry loops.
	UnlimitedRetries = 0
)

// LimitBackoffRetryPolicy implements RetryPolicy with a configurable retry limit
// and optional constant delay between retries.
//
// The RetryLimit field controls retry behavior:
//   - Positive value (e.g., 10): Retry up to that many times before failing
//   - UnlimitedRetries (0): Retry indefinitely until success or non-retryable error
//   - NoRetries (-1) or any negative value: Do not retry; fail immediately on first retryable error
//
// If Delay is greater than zero, the policy will wait for the specified duration
// between retry attempts.
//
// Example usage with limited retries and no delay:
//
//	policy := &LimitBackoffRetryPolicy{
//	    RetryLimit: 10,
//	    Delay:      0,
//	}
//	ctx := crdb.WithRetryPolicy(context.Background(), policy)
//	err := crdb.ExecuteTx(ctx, db, nil, func(tx *sql.Tx) error {
//	    // transaction logic
//	})
//
// Example usage with fixed delay between retries:
//
//	policy := &LimitBackoffRetryPolicy{
//	    RetryLimit: 5,
//	    Delay:      100 * time.Millisecond,
//	}
//	ctx := crdb.WithRetryPolicy(context.Background(), policy)
//
// Example usage with unlimited retries:
//
//	policy := &LimitBackoffRetryPolicy{
//	    RetryLimit: UnlimitedRetries,  // or 0
//	    Delay:      50 * time.Millisecond,
//	}
//
// Note: Convenience functions are available:
//   - WithMaxRetries(ctx, n) creates a LimitBackoffRetryPolicy with RetryLimit=n and Delay=0
//   - WithNoRetries(ctx) creates a LimitBackoffRetryPolicy with RetryLimit=NoRetries
type LimitBackoffRetryPolicy struct {
	// RetryLimit controls the retry behavior:
	//   - Positive value: Maximum number of retries before returning MaxRetriesExceededError
	//   - UnlimitedRetries (0): Retry indefinitely
	//   - NoRetries (-1) or any negative value: Do not retry, fail immediately
	RetryLimit int

	// Delay is the fixed duration to wait between retry attempts. If 0,
	// retries happen immediately without delay.
	Delay time.Duration
}

// NewRetry implements RetryPolicy.
func (l *LimitBackoffRetryPolicy) NewRetry() RetryFunc {
	tryCount := 0
	return func(err error) (time.Duration, error) {
		tryCount++
		// Any negative value (including NoRetries) means fail immediately
		if l.RetryLimit < UnlimitedRetries {
			return 0, newMaxRetriesExceededError(err, 0)
		}
		// UnlimitedRetries (0) means retry indefinitely, so skip the limit check
		// Any positive value enforces the retry limit
		if l.RetryLimit > UnlimitedRetries && tryCount > l.RetryLimit {
			return 0, newMaxRetriesExceededError(err, l.RetryLimit)
		}
		return l.Delay, nil
	}
}

// ExpBackoffRetryPolicy implements RetryPolicy using an exponential backoff strategy
// where delays double with each retry attempt, with an optional maximum delay cap.
//
// The delay between retries doubles with each attempt, starting from BaseDelay:
//   - Retry 1: BaseDelay
//   - Retry 2: BaseDelay * 2
//   - Retry 3: BaseDelay * 4
//   - Retry N: BaseDelay * 2^(N-1)
//
// If MaxDelay is set (> 0), the delay is capped at that value once reached.
// This prevents excessive wait times during high retry counts and provides a
// predictable upper bound for backoff duration.
//
// The policy will retry up to RetryLimit times. When the limit is exceeded or
// if the delay calculation overflows without a MaxDelay set, it returns a
// MaxRetriesExceededError.
//
// Example usage with capped exponential backoff:
//
//	policy := &ExpBackoffRetryPolicy{
//	    RetryLimit: 10,
//	    BaseDelay:  100 * time.Millisecond,
//	    MaxDelay:   5 * time.Second,
//	}
//	ctx := crdb.WithRetryPolicy(context.Background(), policy)
//	err := crdb.ExecuteTx(ctx, db, nil, func(tx *sql.Tx) error {
//	    // transaction logic that may encounter retryable errors
//	    return tx.ExecContext(ctx, "UPDATE ...")
//	})
//
// This configuration produces delays: 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s,
// then stays at 5s for all subsequent retries.
//
// Example usage with unbounded exponential backoff:
//
//	policy := &ExpBackoffRetryPolicy{
//	    RetryLimit: 5,
//	    BaseDelay:  1 * time.Second,
//	    MaxDelay:   0,  // no cap
//	}
//
// This configuration produces delays: 1s, 2s, 4s, 8s, 16s.
// Note: Setting MaxDelay to 0 means no cap, but be aware that delay overflow
// will cause the policy to fail early.
type ExpBackoffRetryPolicy struct {
	// RetryLimit is the maximum number of retries allowed. After this many
	// retries, a MaxRetriesExceededError is returned.
	RetryLimit int

	// BaseDelay is the initial delay before the first retry. Each subsequent
	// retry doubles this value: delay = BaseDelay * 2^(attempt-1).
	BaseDelay time.Duration

	// MaxDelay is the maximum delay cap. If > 0, delays are capped at this
	// value once reached. If 0, delays grow unbounded (until overflow, which
	// causes early termination).
	MaxDelay time.Duration
}

// NewRetry implements RetryPolicy.
func (l *ExpBackoffRetryPolicy) NewRetry() RetryFunc {
	tryCount := 0
	return func(err error) (time.Duration, error) {
		tryCount++
		if tryCount > l.RetryLimit {
			return 0, newMaxRetriesExceededError(err, l.RetryLimit)
		}
		delay := l.BaseDelay << (tryCount - 1)
		if l.MaxDelay > 0 && delay > l.MaxDelay {
			return l.MaxDelay, nil
		}
		if delay < l.BaseDelay {
			// We've overflowed.
			if l.MaxDelay > 0 {
				return l.MaxDelay, nil
			}
			// There's no max delay. Giving up is probably better in
			// practice than using a 290-year MAX_INT delay.
			return 0, newMaxRetriesExceededError(err, tryCount)
		}
		return delay, nil
	}
}

// Vargo adapts third-party backoff strategies (like those from github.com/sethvargo/go-retry)
// into a RetryPolicy without creating a direct dependency on those libraries.
//
// This function allows you to use any backoff implementation that conforms to the
// VargoBackoff interface, providing flexibility to integrate external retry strategies
// with CockroachDB transaction retries.
//
// Example usage with a hypothetical external backoff library:
//
//	import "github.com/sethvargo/go-retry"
//
//	// Create a retry policy using an external backoff strategy
//	policy := crdb.Vargo(func() crdb.VargoBackoff {
//	    // Fibonacci backoff: 1s, 1s, 2s, 3s, 5s, 8s...
//	    return retry.NewFibonacci(1 * time.Second)
//	})
//	ctx := crdb.WithRetryPolicy(context.Background(), policy)
//	err := crdb.ExecuteTx(ctx, db, nil, func(tx *sql.Tx) error {
//	    // transaction logic
//	})
//
// The function parameter should return a fresh VargoBackoff instance for each
// transaction, as backoff state is not safe for concurrent use.
func Vargo(fn func() VargoBackoff) RetryPolicy {
	return &vargoAdapter{
		DelegateFactory: fn,
	}
}

// VargoBackoff is an interface for external backoff strategies that provide
// delays through a Next() method. This allows adaptation of backoff policies
// from libraries like github.com/sethvargo/go-retry without creating a direct
// dependency.
//
// Next returns the next backoff duration and a boolean indicating whether to
// stop retrying. When stop is true, the retry loop terminates with a
// MaxRetriesExceededError.
type VargoBackoff interface {
	// Next returns the next delay duration and whether to stop retrying.
	// When stop is true, no more retries will be attempted.
	Next() (next time.Duration, stop bool)
}

// vargoAdapter adapts backoff policies in the style of github.com/sethvargo/go-retry.
type vargoAdapter struct {
	DelegateFactory func() VargoBackoff
}

// NewRetry implements RetryPolicy by delegating to the external backoff strategy.
// It creates a fresh backoff instance using DelegateFactory and wraps its Next()
// method to conform to the RetryFunc signature.
func (b *vargoAdapter) NewRetry() RetryFunc {
	delegate := b.DelegateFactory()
	count := 0
	return func(err error) (time.Duration, error) {
		count++
		d, stop := delegate.Next()
		if stop {
			return 0, newMaxRetriesExceededError(err, count)
		}
		return d, nil
	}
}

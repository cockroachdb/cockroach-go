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
type RetryFunc func(err error) (time.Duration, error)

// RetryPolicy constructs a new instance of a RetryFunc for each transaction
// it is used with. Instances of RetryPolicy can likely be immutable and
// should be safe for concurrent calls to NewRetry.
type RetryPolicy interface {
	NewRetry() RetryFunc
}

type LimitBackoffRetryPolicy struct {
	RetryLimit int
	Delay      time.Duration
}

func (l *LimitBackoffRetryPolicy) NewRetry() RetryFunc {
	tryCount := 0
	return func(err error) (time.Duration, error) {
		tryCount++
		if tryCount > l.RetryLimit {
			return 0, newMaxRetriesExceededError(err, l.RetryLimit)
		}
		return l.Delay, nil
	}
}

// ExpBackoffRetryPolicy implements RetryPolicy using an exponential backoff with optional
// saturation.
type ExpBackoffRetryPolicy struct {
	RetryLimit int
	BaseDelay  time.Duration
	MaxDelay   time.Duration
}

// NewRetry implements RetryPolicy
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

// Vargo converts a go-retry style Delay provider into a RetryPolicy
func Vargo(fn func() VargoBackoff) RetryPolicy {
	return &vargoAdapter{
		DelegateFactory: fn,
	}
}

// VargoBackoff allow us to adapt sethvargo/go-retry Backoff policies
// without also creating a transitive dependency on that library.
type VargoBackoff interface {
	Next() (next time.Duration, stop bool)
}

// vargoAdapter adapts backoff policies in the style of sethvargo/go-retry
type vargoAdapter struct {
	DelegateFactory func() VargoBackoff
}

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

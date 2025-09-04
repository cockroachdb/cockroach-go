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
	"testing"
	"time"
)

func assertDelays(t *testing.T, policy RetryPolicy, expectedDelays []time.Duration) {
	actualDelays := make([]time.Duration, 0, len(expectedDelays))
	rf := policy.NewRetry()
	for {
		delay, err := rf(nil)
		if err != nil {
			break
		}

		actualDelays = append(actualDelays, delay)
		if len(actualDelays) > len(expectedDelays) {
			t.Fatalf("too many retries: expected %d", len(expectedDelays))
		}
	}
	if len(actualDelays) != len(expectedDelays) {
		t.Errorf("wrong number of retries: expected %d, got %d", len(expectedDelays), len(actualDelays))
	}
	for i, delay := range actualDelays {
		expected := expectedDelays[i]
		if delay != expected {
			t.Errorf("wrong delay at index %d: expected %d, got %d", i, expected, delay)
		}
	}
}

func TestLimitBackoffRetryPolicy(t *testing.T) {
	policy := &LimitBackoffRetryPolicy{
		RetryLimit: 3,
		Delay:      1 * time.Second,
	}
	assertDelays(t, policy, []time.Duration{
		1 * time.Second,
		1 * time.Second,
		1 * time.Second,
	})
}

func TestExpBackoffRetryPolicy(t *testing.T) {
	policy := &ExpBackoffRetryPolicy{
		RetryLimit: 5,
		BaseDelay:  1 * time.Second,
		MaxDelay:   5 * time.Second,
	}
	assertDelays(t, policy, []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		5 * time.Second,
		5 * time.Second,
	})
}

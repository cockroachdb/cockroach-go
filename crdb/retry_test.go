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
	"errors"
	"testing"
	"time"
)

func assertDelays(t *testing.T, policy RetryPolicy, expectedDelays []time.Duration) {
	t.Helper()
	actualDelays := make([]time.Duration, 0, len(expectedDelays))
	rf := policy.NewRetry()

	// Test with nil error (normal retry case)
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

	// Test that RetryFunc also works when passed a non-nil error
	// The error passed to RetryFunc should not affect the retry logic
	rf2 := policy.NewRetry()
	testErr := errors.New("test retryable error")
	actualDelays2 := make([]time.Duration, 0, len(expectedDelays))
	for {
		delay, err := rf2(testErr)
		if err != nil {
			break
		}
		actualDelays2 = append(actualDelays2, delay)
		if len(actualDelays2) > len(expectedDelays) {
			t.Fatalf("too many retries with non-nil err: expected %d", len(expectedDelays))
		}
	}
	if len(actualDelays2) != len(expectedDelays) {
		t.Errorf("wrong number of retries with non-nil err: expected %d, got %d", len(expectedDelays), len(actualDelays2))
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

func TestNoRetries(t *testing.T) {
	policy := &LimitBackoffRetryPolicy{
		RetryLimit: NoRetries,
		Delay:      0,
	}
	// NoRetries should fail immediately without any retries
	assertDelays(t, policy, []time.Duration{})

	// Verify the error is returned on first call
	rf := policy.NewRetry()
	testErr := errors.New("test error")
	delay, err := rf(testErr)
	if err == nil {
		t.Error("expected error on first call with NoRetries, got nil")
	}
	if delay != 0 {
		t.Errorf("expected delay 0, got %v", delay)
	}
}

func TestUnlimitedRetries(t *testing.T) {
	policy := &LimitBackoffRetryPolicy{
		RetryLimit: UnlimitedRetries,
		Delay:      10 * time.Millisecond,
	}

	// Test that UnlimitedRetries continues beyond any reasonable limit
	rf := policy.NewRetry()
	testErr := errors.New("test error")

	// Try 1000 retries - should all succeed with no error
	for i := 0; i < 1000; i++ {
		delay, err := rf(testErr)
		if err != nil {
			t.Fatalf("unexpected error at retry %d: %v", i, err)
		}
		if delay != 10*time.Millisecond {
			t.Errorf("wrong delay at retry %d: expected 10ms, got %v", i, delay)
		}
	}
}

func TestLimitBackoffRetryPolicyEdgeCases(t *testing.T) {
	t.Run("zero BaseDelay with LimitBackoffRetryPolicy", func(t *testing.T) {
		policy := &LimitBackoffRetryPolicy{
			RetryLimit: 3,
			Delay:      0, // zero delay = immediate retries
		}
		assertDelays(t, policy, []time.Duration{0, 0, 0})
	})

	t.Run("negative RetryLimit less than NoRetries", func(t *testing.T) {
		// Negative values other than NoRetries (-1) should be treated as invalid
		// but the implementation currently treats any negative as "no retries"
		policy := &LimitBackoffRetryPolicy{
			RetryLimit: -5,
			Delay:      0,
		}
		rf := policy.NewRetry()
		_, err := rf(errors.New("test"))
		// Should fail immediately like NoRetries
		if err == nil {
			t.Error("expected error for negative RetryLimit < NoRetries, got nil")
		}
	})

	t.Run("very large RetryLimit", func(t *testing.T) {
		policy := &LimitBackoffRetryPolicy{
			RetryLimit: 1000000,
			Delay:      0,
		}
		rf := policy.NewRetry()
		// Should be able to retry many times
		for i := 0; i < 100; i++ {
			_, err := rf(nil)
			if err != nil {
				t.Fatalf("unexpected error at retry %d with large limit: %v", i, err)
			}
		}
	})
}

func TestExpBackoffRetryPolicyEdgeCases(t *testing.T) {
	t.Run("zero BaseDelay", func(t *testing.T) {
		policy := &ExpBackoffRetryPolicy{
			RetryLimit: 3,
			BaseDelay:  0,
			MaxDelay:   1 * time.Second,
		}
		// With zero base delay, all delays should be 0
		assertDelays(t, policy, []time.Duration{0, 0, 0})
	})

	t.Run("MaxDelay less than BaseDelay", func(t *testing.T) {
		policy := &ExpBackoffRetryPolicy{
			RetryLimit: 3,
			BaseDelay:  1 * time.Second,
			MaxDelay:   100 * time.Millisecond, // smaller than base
		}
		// All delays should be capped at MaxDelay
		assertDelays(t, policy, []time.Duration{
			100 * time.Millisecond,
			100 * time.Millisecond,
			100 * time.Millisecond,
		})
	})

	t.Run("MaxDelay equals BaseDelay", func(t *testing.T) {
		policy := &ExpBackoffRetryPolicy{
			RetryLimit: 3,
			BaseDelay:  1 * time.Second,
			MaxDelay:   1 * time.Second, // same as base
		}
		// All delays should be capped at MaxDelay (no exponential growth)
		assertDelays(t, policy, []time.Duration{
			1 * time.Second,
			1 * time.Second,
			1 * time.Second,
		})
	})

	t.Run("zero MaxDelay with potential overflow", func(t *testing.T) {
		policy := &ExpBackoffRetryPolicy{
			RetryLimit: 100,
			BaseDelay:  1 * time.Hour,
			MaxDelay:   0, // no cap
		}
		rf := policy.NewRetry()

		// First few should work fine
		for i := 0; i < 5; i++ {
			delay, err := rf(nil)
			if err != nil {
				t.Fatalf("unexpected error at retry %d: %v", i, err)
			}
			expected := (1 * time.Hour) << i
			if delay != expected {
				t.Errorf("retry %d: expected delay %v, got %v", i, expected, delay)
			}
		}

		// Eventually should overflow and fail
		var overflowed bool
		for i := 5; i < 100; i++ {
			_, err := rf(nil)
			if err != nil {
				overflowed = true
				break
			}
		}
		if !overflowed {
			t.Error("expected overflow error with large base delay and no MaxDelay")
		}
	})

	t.Run("single retry with exponential backoff", func(t *testing.T) {
		policy := &ExpBackoffRetryPolicy{
			RetryLimit: 1,
			BaseDelay:  100 * time.Millisecond,
			MaxDelay:   0,
		}
		assertDelays(t, policy, []time.Duration{100 * time.Millisecond})
	})

	t.Run("NoRetries with ExpBackoffRetryPolicy", func(t *testing.T) {
		// ExpBackoffRetryPolicy doesn't have NoRetries logic, but testing
		// with RetryLimit=0 to see behavior
		policy := &ExpBackoffRetryPolicy{
			RetryLimit: 0,
			BaseDelay:  1 * time.Second,
			MaxDelay:   5 * time.Second,
		}
		rf := policy.NewRetry()
		// With RetryLimit=0, first call should fail
		_, err := rf(nil)
		if err == nil {
			t.Error("expected error with RetryLimit=0 on ExpBackoffRetryPolicy, got nil")
		}
	})
}

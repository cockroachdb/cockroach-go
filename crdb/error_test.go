// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package crdb

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestMaxRetriesExceededError(t *testing.T) {
	origError := fmt.Errorf("root error")
	err := newMaxRetriesExceededError(origError, 10)
	if !strings.HasPrefix(err.Error(), "retrying txn failed after 10 attempts.") {
		t.Fatalf("expected txn retry error message")
	}
	if !errors.Is(err, origError) {
		t.Fatal("expected to find root error cause")
	}
}

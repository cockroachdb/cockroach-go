// Copyright 2016 The Cockroach Authors.
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
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/testserver"
)

// TestExecuteCtx verifies that ExecuteCtx correctly handles different retry limits
// and context cancellation when executing database operations.
//
// TODO(seanc@): Add test cases that force retryable errors by simulating
// transaction conflicts or network failures. Consider using the same write skew
// pattern from TestExecuteTx.
func TestExecuteCtx(t *testing.T) {
	db, stop := testserver.NewDBForTest(t)
	defer stop()
	ctx := context.Background()

	// Setup test table
	if _, err := db.ExecContext(ctx, `CREATE TABLE test_retry (id INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name       string
		maxRetries int
		id         int
		withCancel bool
		wantErr    error
	}{
		{"no retries", 0, 0, false, nil},
		{"single retry", 1, 1, false, nil},
		{"cancelled context", 1, 2, true, context.Canceled},
		{"no args", 1, 3, false, nil},
	}

	fn := func(ctx context.Context, args ...interface{}) error {
		if len(args) == 0 {
			_, err := db.ExecContext(ctx, `INSERT INTO test_retry VALUES (3)`)
			return err
		}
		id := args[0].(int)
		_, err := db.ExecContext(ctx, `INSERT INTO test_retry VALUES ($1)`, id)
		return err
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			limitedCtx := WithMaxRetries(ctx, tc.maxRetries)
			if tc.withCancel {
				var cancel context.CancelFunc
				limitedCtx, cancel = context.WithCancel(limitedCtx)
				cancel()
			}

			var err error
			if tc.name == "no args" {
				err = ExecuteCtx(limitedCtx, fn)
			} else {
				err = ExecuteCtx(limitedCtx, fn, tc.id)
			}

			if !errors.Is(err, tc.wantErr) {
				t.Errorf("got error %v, want %v", err, tc.wantErr)
			}
		})
	}
}

// TestExecuteTx verifies transaction retry using the classic
// example of write skew in bank account balance transfers.
func TestExecuteTx(t *testing.T) {
	db, stop := testserver.NewDBForTest(t)
	defer stop()
	ctx := context.Background()

	if err := ExecuteTxGenericTest(ctx, stdlibWriteSkewTest{db: db}); err != nil {
		t.Fatal(err)
	}
}

// TestConfigureRetries verifies that the number of retries can be specified
// via context.
func TestConfigureRetries(t *testing.T) {
	ctx := context.Background()
	if numRetriesFromContext(ctx) != defaultRetries {
		t.Fatal("expect default number of retries")
	}
	ctx = WithMaxRetries(context.Background(), 123+defaultRetries)
	if numRetriesFromContext(ctx) != defaultRetries+123 {
		t.Fatal("expected default+123 retires")
	}
}

type stdlibWriteSkewTest struct {
	db *sql.DB
}

var _ WriteSkewTest = stdlibWriteSkewTest{}

func (t stdlibWriteSkewTest) Init(ctx context.Context) error {
	initStmt := `
CREATE DATABASE d;
CREATE TABLE d.t (acct INT PRIMARY KEY, balance INT);
INSERT INTO d.t (acct, balance) VALUES (1, 100), (2, 100);
`
	_, err := t.db.ExecContext(ctx, initStmt)
	return err
}

func (t stdlibWriteSkewTest) ExecuteTx(ctx context.Context, fn func(tx interface{}) error) error {
	return ExecuteTx(ctx, t.db, nil /* opts */, func(tx *sql.Tx) error {
		return fn(tx)
	})
}

func (t stdlibWriteSkewTest) GetBalances(ctx context.Context, txi interface{}) (int, int, error) {
	tx := txi.(*sql.Tx)
	var rows *sql.Rows
	rows, err := tx.QueryContext(ctx, `SELECT balance FROM d.t WHERE acct IN (1, 2);`)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	var bal1, bal2 int
	balances := []*int{&bal1, &bal2}
	i := 0
	for ; rows.Next(); i++ {
		if err = rows.Scan(balances[i]); err != nil {
			return 0, 0, err
		}
	}
	if i != 2 {
		return 0, 0, fmt.Errorf("expected two balances; got %d", i)
	}
	return bal1, bal2, nil
}

func (t stdlibWriteSkewTest) UpdateBalance(
	ctx context.Context, txi interface{}, acct, delta int,
) error {
	tx := txi.(*sql.Tx)
	_, err := tx.ExecContext(ctx, `UPDATE d.t SET balance=balance+$1 WHERE acct=$2;`, delta, acct)
	return err
}

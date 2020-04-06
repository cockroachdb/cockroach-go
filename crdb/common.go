// Copyright 2020 The Cockroach Authors.
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
	"context"
)

// Tx abstracts the operations needed by ExecuteInTx so that different
// frameworks (e.g. go's sql package, pgx, gorm) can be used with ExecuteInTx.
type Tx interface {
	Exec(context.Context, string, ...interface{}) error
	Commit(context.Context) error
	Rollback(context.Context) error
}

// ExecuteInTx runs fn inside tx. This method is primarily intended for internal
// use. See other packages for higher-level, framework-specific ExecuteTx()
// functions.
//
// *WARNING*: It is assumed that no statements have been executed on the
// supplied Tx. ExecuteInTx will only retry statements that are performed within
// the supplied closure (fn). Any statements performed on the tx before
// ExecuteInTx is invoked will *not* be re-run if the transaction needs to be
// retried.
//
// fn is subject to the same restrictions as the fn passed to ExecuteTx.
func ExecuteInTx(ctx context.Context, tx Tx, fn func() error) (err error) {
	defer func() {
		if err == nil {
			// Ignore commit errors. The tx has already been committed by RELEASE.
			_ = tx.Commit(ctx)
		} else {
			// We always need to execute a Rollback() so sql.DB releases the
			// connection.
			_ = tx.Rollback(ctx)
		}
	}()
	// Specify that we intend to retry this txn in case of CockroachDB retryable
	// errors.
	if err = tx.Exec(ctx, "SAVEPOINT cockroach_restart"); err != nil {
		return err
	}

	for {
		released := false
		err = fn()
		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			released = true
			if err = tx.Exec(ctx, "RELEASE SAVEPOINT cockroach_restart"); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart.
		if !errIsRetryable(err) {
			if released {
				err = newAmbiguousCommitError(err)
			}
			return err
		}

		if retryErr := tx.Exec(ctx, "ROLLBACK TO SAVEPOINT cockroach_restart"); retryErr != nil {
			return newTxnRestartError(retryErr, err)
		}
	}
}

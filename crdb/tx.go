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
//
// Author: Andrei Matei (andrei@cockroachlabs.com)

// Package crdb provides helpers for using CockroachDB in client
// applications.
package crdb

import (
	"database/sql"

	"github.com/cockroachdb/pq"
)

// ExecuteTx runs fn inside a transaction and retries it as needed.
// On non-retryable failures, the transaction is aborted and rolled
// back; on success, the transaction is committed.
//
// For more information about CockroachDB's transaction model see
// https://cockroachlabs.com/docs/transactions.html.
//
// NOTE: the supplied exec closure should not have external side
// effects beyond changes to the database.
func ExecuteTx(db *sql.DB, fn func(*sql.Tx) error) (err error) {
	// Start a transaction.
	var tx *sql.Tx
	tx, err = db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()
	// Specify that we intend to retry this txn in case of CockroachDB retryable
	// errors.
	if _, err = tx.Exec("SAVEPOINT cockroach_restart"); err != nil {
		return err
	}

	for {
		err = fn(tx)
		if err == nil {
			// RELEASE acts like COMMIT in CockroachDB. We use it since it gives us an
			// opportunity to react to retryable errors, whereas tx.Commit() doesn't.
			if _, err = tx.Exec("RELEASE SAVEPOINT cockroach_restart"); err == nil {
				return nil
			}
		}
		// We got an error; let's see if it's a retryable one and, if so, restart.
		pqErr, ok := err.(*pq.Error)
		if retryable := ok && pqErr.Code == "CR000"; !retryable {
			return err
		}
		if _, err = tx.Exec("ROLLBACK TO SAVEPOINT cockroach_restart"); err != nil {
			return err
		}
	}
}

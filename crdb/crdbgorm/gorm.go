// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package crdbgorm

import (
	"context"
	"database/sql"
	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/jinzhu/gorm"
)

// ExecuteTx runs fn inside a transaction and retries it as needed. On
// non-retryable failures, the transaction is aborted and rolled back; on
// success, the transaction is committed.
//
// See crdb.ExecuteTx() for more information.
func ExecuteTx(
	ctx context.Context, db *gorm.DB, opts *sql.TxOptions, fn func(tx *gorm.DB) error,
) error {
	tx := db.BeginTx(ctx, opts)
	if db.Error != nil {
		return db.Error
	}
	return crdb.ExecuteInTx(ctx, gormTxAdapter{tx}, func() error { return fn(tx) })
}

// gormTxAdapter adapts a *gorm.DB to a crdb.Tx.
type gormTxAdapter struct {
	*gorm.DB
}

var _ crdb.Tx = gormTxAdapter{}

// Exec is part of the crdb.Tx interface.
func (tx gormTxAdapter) Exec(_ context.Context, q string, args ...interface{}) error {
	return tx.DB.Exec(q, args...).Error
}

// Commit is part of the crdb.Tx interface.
func (tx gormTxAdapter) Commit(_ context.Context) error {
	return tx.DB.Commit().Error
}

// Rollback is part of the crdb.Tx interface.
func (tx gormTxAdapter) Rollback(_ context.Context) error {
	return tx.DB.Rollback().Error
}

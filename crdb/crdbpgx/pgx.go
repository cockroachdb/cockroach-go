// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package crdbpgx

import (
	"context"
	"github.com/cockroachdb/cockroach-go/crdb"

	"github.com/jackc/pgx"
)

// ExecuteTx runs fn inside a transaction and retries it as needed. On
// non-retryable failures, the transaction is aborted and rolled back; on
// success, the transaction is committed.
//
// See crdb.ExecuteTx() for more information.
//
// conn can be a pgx.Conn or a pgxpool.Conn.
func ExecuteTx(
	ctx context.Context, conn Conn, txOptions pgx.TxOptions, fn func(pgx.Tx) error,
) error {
	tx, err := conn.BeginTx(ctx, txOptions)
	if err != nil {
		return err
	}
	return crdb.ExecuteInTx(ctx, pgxTxAdapter{tx}, func() error { return fn(tx) })
}

// Conn abstracts pgx transactions creators: pgx.Conn and pgxpool.Connect.
type Conn interface {
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type pgxTxAdapter struct {
	pgx.Tx
}

var _ crdb.Tx = pgxTxAdapter{}

// Exec is part of the crdb.Tx interface.
func (tx pgxTxAdapter) Exec(ctx context.Context, q string, args ...interface{}) error {
	_, err := tx.Tx.Exec(ctx, q, args...)
	return err
}

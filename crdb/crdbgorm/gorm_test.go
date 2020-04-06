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
	"fmt"
	"github.com/cockroachdb/cockroach-go/crdb"
	"github.com/cockroachdb/cockroach-go/testserver"
	"github.com/jinzhu/gorm"
	"testing"
)

// TestExecuteTx verifies transaction retry using the classic example of write
// skew in bank account balance transfers.
func TestExecuteTx(t *testing.T) {
	ts, err := testserver.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.Start(); err != nil {
		t.Fatal(err)
	}
	url := ts.PGURL()
	db, err := sql.Open("postgres", url.String())
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.WaitForInit(db); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	gormDB, err := gorm.Open("postgres", ts.PGURL().String())
	if err != nil {
		t.Fatal(err)
	}
	// Set to true and gorm logs all the queries.
	gormDB.LogMode(false)

	if err := crdb.ExecuteTxGenericTest(ctx, gormWriteSkewTest{db: gormDB}); err != nil {
		t.Fatal(err)
	}
}

// Account is our model, which corresponds to the "accounts" database
// table.
type Account struct {
	ID      int `gorm:"primary_key"`
	Balance int
}

type gormWriteSkewTest struct {
	db *gorm.DB
}

var _ crdb.WriteSkewTest = gormWriteSkewTest{}

func (t gormWriteSkewTest) Init(context.Context) error {
	t.db.AutoMigrate(&Account{})
	t.db.Create(&Account{ID: 1, Balance: 100})
	t.db.Create(&Account{ID: 2, Balance: 100})
	return t.db.Error
}

// ExecuteTx is part of the crdb.WriteSkewTest interface.
func (t gormWriteSkewTest) ExecuteTx(ctx context.Context, fn func(tx interface{}) error) error {
	return ExecuteTx(ctx, t.db, nil /* opts */, func(tx *gorm.DB) error {
		return fn(tx)
	})
}

// GetBalances is part of the crdb.WriteSkewTest interface.
func (t gormWriteSkewTest) GetBalances(_ context.Context, txi interface{}) (int, int, error) {
	tx := txi.(*gorm.DB)

	var accounts []Account
	tx.Find(&accounts)
	if len(accounts) != 2 {
		return 0, 0, fmt.Errorf("expected two balances; got %d", len(accounts))
	}
	return accounts[0].Balance, accounts[1].Balance, nil
}

// UpdateBalance is part of the crdb.WriteSkewInterface.
func (t gormWriteSkewTest) UpdateBalance(
	_ context.Context, txi interface{}, accountID, delta int,
) error {
	tx := txi.(*gorm.DB)
	var acc Account
	tx.First(&acc, accountID)
	acc.Balance += delta
	return tx.Save(acc).Error
}

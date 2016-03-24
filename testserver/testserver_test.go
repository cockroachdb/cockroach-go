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
// Author: Marc Berhault (marc@cockroachlabs.com)

package testserver_test

import (
	"database/sql"
	"testing"

	// Needed for postgres driver test.
	"github.com/cockroachdb/cockroach-go/testserver"
	_ "github.com/lib/pq"
)

func TestRunServer(t *testing.T) {
	ts, err := testserver.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}

	err = ts.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	url := ts.PGURL()
	if url == nil {
		t.Fatalf("url not found")
	}
	t.Logf("URL: %s", url.String())

	db, err := sql.Open("postgres", url.String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
}

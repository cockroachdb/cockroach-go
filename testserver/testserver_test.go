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

package testserver_test

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/testserver"
)

const noPW = ""

const defStoreMemSize = 0.2

func TestRunServer(t *testing.T) {
	const testPW = "foobar"
	for _, tc := range []struct {
		name          string
		instantiation func(*testing.T) (*sql.DB, func())
	}{
		{
			name:          "Insecure",
			instantiation: func(t *testing.T) (*sql.DB, func()) { return testserver.NewDBForTest(t) },
		},
		{
			name: "InsecureWithCustomizedMemSize",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.SetStoreMemSizeOpt(0.3))
			},
		},
		{
			name:          "SecureClientCert",
			instantiation: func(t *testing.T) (*sql.DB, func()) { return testserver.NewDBForTest(t, testserver.SecureOpt()) },
		},
		{
			name: "SecurePassword",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.SecureOpt(), testserver.RootPasswordOpt(testPW))
			},
		},
		{
			name: "InsecureTenantStoreOnDisk",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.StoreOnDiskOpt())
			},
		},
		{
			name: "SecureTenantStoreOnDisk",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.SecureOpt(), testserver.StoreOnDiskOpt())
			},
		},
		{
			name: "InsecureTenant",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return newTenantDBForTest(t, false /* secure */, false /* proxy */, noPW, false /* diskStore */, defStoreMemSize /* storeMemSize */)
			},
		},
		{
			name: "SecureTenant",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return newTenantDBForTest(t, true /* secure */, false /* proxy */, noPW, false /* diskStore */, defStoreMemSize /* storeMemSize */)
			},
		},
		{
			name: "SecureTenantCustomPassword",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return newTenantDBForTest(t, true /* secure */, false /* proxy */, testPW, false /* diskStore */, defStoreMemSize /* storeMemSize */)
			},
		},
		{
			name: "SecureTenantThroughProxy",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return newTenantDBForTest(t, true /* secure */, true /* proxy */, noPW, false /* diskStore */, defStoreMemSize /* storeMemSize */)
			},
		},
		{
			name: "SecureTenantThroughProxyCustomPassword",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return newTenantDBForTest(t, true /* secure */, true /* proxy */, testPW, false /* diskStore */, defStoreMemSize /* storeMemSize */)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, stop := tc.instantiation(t)
			defer stop()
			if _, err := db.Exec("SELECT 1"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPGURLWhitespace(t *testing.T) {
	ts, err := testserver.NewTestServer()
	if err != nil {
		t.Fatal(err)
	}
	url := ts.PGURL().String()
	if trimmed := strings.TrimSpace(url); url != trimmed {
		t.Errorf("unexpected whitespace in server URL: %q", url)
	}
}

// tenantInterface is defined in order to use tenant-related methods on the
// TestServer.
type tenantInterface interface {
	NewTenantServer(proxy bool) (testserver.TestServer, error)
}

// newTenantDBForTest is a testing helper function that starts a TestServer
// process and a SQL tenant process pointed at this TestServer. A sql connection
// to the tenant and a cleanup function are returned.
func newTenantDBForTest(
	t *testing.T, secure bool, proxy bool, pw string, diskStore bool, storeMemSize float64,
) (*sql.DB, func()) {
	t.Helper()
	var opts []testserver.TestServerOpt
	if secure {
		opts = append(opts, testserver.SecureOpt())
	}
	if diskStore {
		opts = append(opts, testserver.StoreOnDiskOpt())
	}
	if pw != "" {
		opts = append(opts, testserver.RootPasswordOpt(pw))
	}
	if storeMemSize >= 0 {
		opts = append(opts, testserver.SetStoreMemSizeOpt(storeMemSize))
	} else {
		t.Fatal("Percentage memory size for data storage cannot be nagative")
	}
	ts, err := testserver.NewTestServer(opts...)
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := ts.(tenantInterface).NewTenantServer(proxy)
	if err != nil {
		t.Fatal(err)
	}
	url := tenant.PGURL()
	if url == nil {
		t.Fatal("postgres url not found")
	}
	db, err := sql.Open("postgres", url.String())
	if err != nil {
		t.Fatal(err)
	}
	return db, func() {
		_ = db.Close()
		tenant.Stop()
		ts.Stop()
	}
}

func TestTenant(t *testing.T) {
	db, stop := newTenantDBForTest(t, false /* secure */, false /* proxy */, noPW, false /* diskStore */, defStoreMemSize /* storeMemSize */)
	defer stop()
	if _, err := db.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("SELECT crdb_internal.create_tenant(123)"); err == nil {
		t.Fatal("expected an error when creating a tenant since secondary tenants should not be able to do so")
	}
}

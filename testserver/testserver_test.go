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
        "fmt"
        "io"
        "log"
        http "net/http"
        "os"
        "os/exec"
        "path/filepath"
        "runtime"
        "strings"
        "sync"
        "testing"
        "time"

        "github.com/cockroachdb/cockroach-go/v2/testserver"
        "github.com/stretchr/testify/require"
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
                                return newTenantDBForTest(
                                        t,
                                        false,           /* secure */
                                        false,           /* proxy */
                                        noPW,            /* pw */
                                        false,           /* diskStore */
                                        defStoreMemSize, /* storeMemSize */
                                        false,           /* nonStableDB */
                                )
                        },
                },
                {
                        name: "SecureTenant",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return newTenantDBForTest(
                                        t,
                                        true,            /* secure */
                                        false,           /* proxy */
                                        noPW,            /* pw */
                                        false,           /* diskStore */
                                        defStoreMemSize, /* storeMemSize */
                                        false,           /* nonStableDB */
                                )
                        },
                },
                {
                        name: "SecureTenantCustomPassword",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return newTenantDBForTest(t,
                                        true,            /* secure */
                                        false,           /* proxy */
                                        testPW,          /* pw */
                                        false,           /* diskStore */
                                        defStoreMemSize, /* storeMemSize */
                                        false,           /* nonStableDB */
                                )
                        },
                },
                {
                        name:          "InsecureNonStable",
                        instantiation: func(t *testing.T) (*sql.DB, func()) { return testserver.NewDBForTest(t, testserver.NonStableDbOpt()) },
                },
                {
                        name: "InsecureWithCustomizedMemSizeNonStable",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testserver.NewDBForTest(t, testserver.SetStoreMemSizeOpt(0.3), testserver.NonStableDbOpt())
                        },
                },
                {
                        name: "SecureClientCertNonStable",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testserver.NewDBForTest(t, testserver.SecureOpt(), testserver.NonStableDbOpt())
                        },
                },
                {
                        name: "SecurePasswordNonStable",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testserver.NewDBForTest(t, testserver.SecureOpt(),
                                        testserver.RootPasswordOpt(testPW), testserver.NonStableDbOpt())
                        },
                },
                {
                        name: "InsecureTenantStoreOnDiskNonStable",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testserver.NewDBForTest(t, testserver.StoreOnDiskOpt(), testserver.NonStableDbOpt())
                        },
                },
                {
                        name: "SecureTenantStoreOnDiskNonStable",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testserver.NewDBForTest(t,
                                        testserver.SecureOpt(),
                                        testserver.StoreOnDiskOpt(),
                                        testserver.NonStableDbOpt(),
                                )
                        },
                },
                {
                        name: "SecureTenantThroughProxyNonStable",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return newTenantDBForTest(
                                        t,
                                        true,            /* secure */
                                        true,            /* proxy */
                                        noPW,            /* pw */
                                        false,           /* diskStore */
                                        defStoreMemSize, /* storeMemSize */
                                        true,            /* nonStableDB */
                                )
                        },
                },
                {
                        name: "SecureTenantThroughProxyCustomPasswordNonStable",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return newTenantDBForTest(
                                        t,
                                        true,            /* secure */
                                        true,            /* proxy */
                                        testPW,          /* pw */
                                        false,           /* diskStore */
                                        defStoreMemSize, /* storeMemSize */
                                        true,            /* nonStableDB */
                                )
                        },
                },
                {
                        name: "Insecure 3 Node",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testserver.NewDBForTest(t, testserver.ThreeNodeOpt())
                        },
                },
                {
                        name: "Insecure 3 Node On Disk",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testserver.NewDBForTest(t, testserver.ThreeNodeOpt(), testserver.StoreOnDiskOpt())
                        },
                },
        } {
                t.Run(tc.name, func(t *testing.T) {
                        db, stop := tc.instantiation(t)
                        defer stop()
                        var out int
                        row := db.QueryRow("SELECT 1")
                        require.NoError(t, row.Scan(&out))
                        require.Equal(t, out, 1)
                        _, err := db.Exec("SELECT 1")
                        require.NoError(t, err)
                })
        }
}

func TestCockroachBinaryPathOpt(t *testing.T) {
        crdbBinary := "doesnotexist"
        _, err := testserver.NewTestServer(testserver.CockroachBinaryPathOpt(crdbBinary))
        if err == nil {
                t.Fatal("expected err, got nil")
        }
        // Confirm that the command is updated to reference the absolute path
        // of the custom cockroachdb binary.
        cmdPath, fPathErr := filepath.Abs(crdbBinary)
        if fPathErr != nil {
                cmdPath = crdbBinary
        }
        wantSubstring := fmt.Sprintf("command %s version failed", cmdPath)
        if msg := err.Error(); !strings.Contains(msg, wantSubstring) {
                t.Fatalf("error message %q does not contain %q", msg, wantSubstring)
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
  t *testing.T,
  secure bool,
  proxy bool,
  pw string,
  diskStore bool,
  storeMemSize float64,
  nonStableDB bool,
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
        if nonStableDB {
                opts = append(opts, testserver.NonStableDbOpt())
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
        db, stop := newTenantDBForTest(
                t,
                false,           /* secure */
                false,           /* proxy */
                noPW,            /* pw */
                false,           /* diskStore */
                defStoreMemSize, /* storeMemSize */
                false,           /* nonStableDB */
        )

        defer stop()
        if _, err := db.Exec("SELECT 1"); err != nil {
                t.Fatal(err)
        }

        if _, err := db.Exec("SELECT crdb_internal.create_tenant(123)"); err == nil {
                t.Fatal("expected an error when creating a tenant since secondary tenants should not be able to do so")
        }
}

func TestFlockOnDownloadedCRDB(t *testing.T) {
        for _, tc := range []struct {
                name          string
                instantiation func(*testing.T) (*sql.DB, func())
        }{
                {
                        // It will print "waiting for download of ..." in log for about 30 seconds.
                        name: "DownloadPassed",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testFlockWithDownloadPassing(t)
                        },
                }, {
                        name: "DownloadKilled",
                        instantiation: func(t *testing.T) (*sql.DB, func()) {
                                return testFlockWithDownloadKilled(t)
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

func TestRestartNode(t *testing.T) {
        ts, err := testserver.NewTestServer(testserver.ThreeNodeOpt(), testserver.StoreOnDiskOpt())
        require.NoError(t, err)
        defer ts.Stop()
        for i := 0; i < 3; i++ {
                require.NoError(t, ts.WaitForInitFinishForNode(i))
        }

        log.Printf("Stopping Node 2")
        require.NoError(t, ts.StopNode(0))
        for i := 1; i < 3; i++ {
                url := ts.PGURLForNode(i)

                db, err := sql.Open("postgres", url.String())
                require.NoError(t, err)
                var out int
                row := db.QueryRow("SELECT 1")
                err = row.Scan(&out)
                require.NoError(t, err)
                require.NoError(t, db.Close())
        }

        require.NoError(t, ts.StartNode(0))
        require.NoError(t, ts.WaitForInitFinishForNode(0))

        for i := 0; i < 3; i++ {
                url := ts.PGURLForNode(i)
                db, err := sql.Open("postgres", url.String())
                require.NoError(t, err)

                var out int
                row := db.QueryRow("SELECT 1")
                err = row.Scan(&out)
                require.NoError(t, err)
                require.NoError(t, db.Close())
        }

        url := ts.PGURLForNode(0)
        db, err := sql.Open("postgres", url.String())
        defer db.Close()
        require.NoError(t, err)
        var out int
        row := db.QueryRow("SELECT 1")
        err = row.Scan(&out)
        require.NoError(t, err)
}

func downloadBinaryTest(filepath string, url string) error {
        // Get the data
        resp, err := http.Get(url)
        if err != nil {
                return err
        }
        defer resp.Body.Close()

        // Create the file
        out, err := os.Create(filepath)
        if err != nil {
                return err
        }
        defer out.Close()

        // Write the body to file
        _, err = io.Copy(out, resp.Body)
        return err
}

func TestUpgradeNode(t *testing.T) {
        oldVersion := "v21.2.12"
        newVersion := "v22.1.6"

        var oldVersionBinary, newVersionBinary string
        switch runtime.GOOS {
        case "darwin":
                oldVersionBinary = fmt.Sprintf("cockroach-%s.darwin-10.9-amd64", oldVersion)
                newVersionBinary = fmt.Sprintf("cockroach-%s.darwin-10.9-amd64", newVersion)
        case "linux":
                oldVersionBinary = fmt.Sprintf("cockroach-%s.linux-amd64", oldVersion)
                newVersionBinary = fmt.Sprintf("cockroach-%s.linux-amd64", newVersion)
        default:
                t.Fatalf("unsupported os for test: %s", runtime.GOOS)
        }

        getBinary := func(fileName string) {
                require.NoError(t, exec.Command("mkdir", "./temp_binaries").Start())
                require.NoError(t, downloadBinaryTest(fmt.Sprintf("./temp_binaries/%s.tgz", fileName),
                        fmt.Sprintf("https://binaries.cockroachdb.com/%s.tgz", fileName)))
                tarCmd := exec.Command("tar", "-zxvf", fmt.Sprintf("./temp_binaries/%s.tgz", fileName), "-C", "./temp_binaries")
                require.NoError(t, tarCmd.Start())
                require.NoError(t, tarCmd.Wait())
        }

        defer func() {
                require.NoError(t, exec.Command("rm", "-rf", "./temp_binaries").Start())
        }()

        getBinary(oldVersionBinary)
        getBinary(newVersionBinary)

        absPathOldBinary, err := filepath.Abs(fmt.Sprintf("./temp_binaries/%s/cockroach", oldVersionBinary))
        require.NoError(t, err)
        absPathNewBinary, err := filepath.Abs(fmt.Sprintf("./temp_binaries/%s/cockroach", newVersionBinary))
        require.NoError(t, err)

        ts, err := testserver.NewTestServer(
                testserver.ThreeNodeOpt(),
                testserver.CockroachBinaryPathOpt(absPathOldBinary),
                testserver.UpgradeCockroachBinaryPathOpt(absPathNewBinary),
                testserver.StoreOnDiskOpt(),
        )
        require.NoError(t, err)
        defer ts.Stop()

        for i := 0; i < 3; i++ {
                require.NoError(t, ts.WaitForInitFinishForNode(i))
        }

        url := ts.PGURL()
        db, err := sql.Open("postgres", url.String())
        require.NoError(t, err)
        defer func() {
                require.NoError(t, db.Close())
        }()

        var version string
        row := db.QueryRow("SHOW CLUSTER SETTING version")
        err = row.Scan(&version)
        require.NoError(t, err)

        _, err = db.Exec("SET CLUSTER SETTING cluster.preserve_downgrade_option = '21.2';")
        require.NoError(t, err)
        require.NoError(t, db.Close())

        for i := 0; i < 3; i++ {
                require.NoError(t, ts.UpgradeNode(i))
                require.NoError(t, ts.WaitForInitFinishForNode(i))
        }

        for i := 0; i < 3; i++ {
                url := ts.PGURLForNode(i)

                db, err = sql.Open("postgres", url.String())
                require.NoError(t, err)

                var out int
                row = db.QueryRow("SELECT 1")
                err = row.Scan(&out)
                require.NoError(t, err)
                require.NoError(t, db.Close())
        }

        db, err = sql.Open("postgres", ts.PGURL().String())
        require.NoError(t, err)
        defer db.Close()
        _, err = db.Exec("RESET CLUSTER SETTING cluster.preserve_downgrade_option;")
        require.NoError(t, err)

        updated := false
        for start := time.Now(); time.Since(start) < 300*time.Second; {
                row = db.QueryRow("SHOW CLUSTER SETTING version")
                err = row.Scan(&version)
                if err != nil {
                        t.Fatal(err)
                }
                if version == "22.1" {
                        updated = true
                        break
                }
                time.Sleep(time.Second)
        }

        if !updated {
                t.Fatal("update to 22.1 did not complete")
        }
}

var wg = sync.WaitGroup{}

// testFlockWithDownloadPassing is to test the flock over downloaded CRDB binary with
// two goroutines, the second goroutine waits for the first goroutine to
// finish downloading the CRDB binary into a local file.
func testFlockWithDownloadPassing(
  t *testing.T, opts ...testserver.TestServerOpt,
) (*sql.DB, func()) {

        localFile, err := getLocalFile(false)
        if err != nil {
                t.Fatal(err)
        }

        // Remove existing local file, to ensure that the first goroutine will download the CRDB binary.
        if err := removeExistingLocalFile(localFile); err != nil {
                t.Fatal(err)
        }

        wg.Add(2)
        var db *sql.DB
        var stop func()

        // First NewDBForTest goroutine to download the CRDB binary to the local file.
        go func() {
                db, stop = testserver.NewDBForTest(t, opts...)
                wg.Done()
        }()
        // Wait for 2 seconds, and then start the second NewDBForTest process.
        time.Sleep(2000 * time.Millisecond)
        // The second goroutine needs to wait until the first goroutine finishes downloading.
        // It will print "waiting for download of ..." in log for about 30 seconds.
        go func() {
                db, stop = testserver.NewDBForTest(t, opts...)
                wg.Done()
        }()
        wg.Wait()
        return db, stop
}

// testFlockWithDownloadKilled is to simulate the case that a NewDBForTest process is
// killed before finishing downloading CRDB binary, and another NewDBForTest process has
// to remove the existing local file and redownload.
func testFlockWithDownloadKilled(t *testing.T) (*sql.DB, func()) {
        localFile, err := getLocalFile(false)
        if err != nil {
                t.Fatal(err)
        }

        // Remove existing local file, to ensure that the first goroutine will download the CRDB binary.
        if err := removeExistingLocalFile(localFile); err != nil {
                t.Fatal(err)
        }

        // First NewDBForTest process to download the CRDB binary to the local file,
        // but killed in the middle.
        _, stop := testserver.NewDBForTest(t, testserver.StopDownloadInMiddleOpt())
        stop()
        // Start the second NewDBForTest process.
        // It will remove the local file and redownload.
        return testserver.NewDBForTest(t)

}

// getLocalFile returns the to-be-downloaded CRDB binary's local path.
func getLocalFile(nonStable bool) (string, error) {
        response, latestStableVersion, err := testserver.GetDownloadResponse(nonStable)
        if err != nil {
                return "", err
        }
        filename, err := testserver.GetDownloadFilename(response, nonStable, latestStableVersion)
        if err != nil {
                return "", err
        }
        localFile := filepath.Join(os.TempDir(), filename)
        return localFile, err
}

// removeExistingLocalFile removes the existing local file for downloaded CRDB binary.
func removeExistingLocalFile(localFile string) error {
        _, err := os.Stat(localFile)
        if err == nil {
                if err := os.Remove(localFile); err != nil {
                        return fmt.Errorf("cannot remove local file: %s", err)
                }
        }
        return nil
}

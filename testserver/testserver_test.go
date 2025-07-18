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
	"net"
	http "net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
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
			name: "InsecureWithHostOverride",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.ListenAddrHostOpt("0.0.0.0"))
			},
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
			name: "InsecureCustomVersion",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.CustomVersionOpt("v21.2.15"))
			},
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
				return testserver.NewDBForTest(t, testserver.ThreeNodeOpt(),
					testserver.AddListenAddrPortOpt(26257),
					testserver.AddListenAddrPortOpt(26258),
					testserver.AddListenAddrPortOpt(26259))
			},
		},
		{
			name: "Insecure 3 Node On Disk",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.ThreeNodeOpt(),
					testserver.StoreOnDiskOpt(),
					testserver.AddListenAddrPortOpt(26257),
					testserver.AddListenAddrPortOpt(26258),
					testserver.AddListenAddrPortOpt(26259))
			},
		},
		{
			name: "Insecure 3 Node On Disk No Ports Specified",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.ThreeNodeOpt(),
					testserver.StoreOnDiskOpt())
			},
		},
		{
			name: "Insecure 3 Node With Http Ports",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.ThreeNodeOpt(),
					testserver.StoreOnDiskOpt(),
					testserver.AddHttpPortOpt(8080),
					testserver.AddHttpPortOpt(8081),
					testserver.AddHttpPortOpt(8082),
				)
			},
		},
		{
			name: "Demo mode",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t,
					testserver.DemoModeOpt(),
				)
			},
		},
		{
			name: "Demo mode 3-node",
			instantiation: func(t *testing.T) (*sql.DB, func()) {
				return testserver.NewDBForTest(t, testserver.ThreeNodeOpt(),
					testserver.DemoModeOpt(),
				)
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

func TestCockroachExternalIODirOpt(t *testing.T) {
	externalDir, err := os.MkdirTemp("/tmp", "cockroach-testserver")
	require.NoError(t, err)
	defer func() {
		err := os.RemoveAll(externalDir)
		require.NoError(t, err)
	}()

	db, cleanup := testserver.NewDBForTest(t, testserver.ExternalIODirOpt(externalDir))
	defer cleanup()

	// test that we can use external dir
	_, err = db.Exec("BACKUP INTO 'nodelocal://self/backup'")
	require.NoError(t, err)

	// test that external dir has files
	f, err := os.Open(externalDir)
	require.NoError(t, err)
	defer f.Close()
	_, err = f.Readdirnames(1)
	require.NoError(t, err)
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

func TestSingleNodePort(t *testing.T) {
	port, err := getFreePort()
	require.NoError(t, err)

	ts, err := testserver.NewTestServer(testserver.AddListenAddrPortOpt(port))
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	// check that port overriding worked
	url := ts.PGURL()
	require.Equal(t, strconv.Itoa(port), url.Port())

	db, err := sql.Open("postgres", url.String())
	require.NoError(t, err)

	// check that connection was successful
	var out int
	row := db.QueryRow("SELECT 1")
	require.NoError(t, row.Scan(&out))
	require.Equal(t, 1, out)
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

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	if err != nil {
		return 0, err
	}

	err = l.Close()
	if err != nil {
		return 0, err
	}

	return port, err
}

func TestRestartNodeParallel(t *testing.T) {
	require.NoError(t, os.Mkdir("./temp_binaries", 0755))
	defer func() {
		require.NoError(t, os.RemoveAll("./temp_binaries"))
	}()
	var fileName string
	switch runtime.GOOS {
	case "darwin":
		fileName = fmt.Sprintf("cockroach-%s.darwin-10.9-amd64", "v22.1.6")
		require.NoError(t, downloadBinaryTest(
			fmt.Sprintf("./temp_binaries/%s.tgz", fileName),
			fmt.Sprintf("https://binaries.cockroachdb.com/%s.tgz", fileName)))
	case "linux":
		fileName = fmt.Sprintf("cockroach-%s.linux-amd64", "v22.1.6")
		require.NoError(t, downloadBinaryTest(
			fmt.Sprintf("./temp_binaries/%s.tgz", fileName),
			fmt.Sprintf("https://binaries.cockroachdb.com/%s.tgz", fileName)))
	default:
		t.Fatalf("unsupported os for test: %s", runtime.GOOS)
	}

	tarCmd := exec.Command("tar", "-zxvf", fmt.Sprintf("./temp_binaries/%s.tgz", fileName), "-C", "./temp_binaries")
	require.NoError(t, tarCmd.Start())
	require.NoError(t, tarCmd.Wait())

	mu := sync.Mutex{}
	usedPorts := make(map[int]struct{})
	const ParallelExecs = 5
	var wg sync.WaitGroup
	wg.Add(ParallelExecs)
	for i := 0; i < ParallelExecs; i++ {
		go func() {
			ports := make([]int, 3)
			for j := 0; j < 3; j++ {
				for {
					port, err := getFreePort()
					require.NoError(t, err)
					mu.Lock()
					_, found := usedPorts[port]
					if !found {
						usedPorts[port] = struct{}{}
					}
					ports[j] = port
					mu.Unlock()
					if !found {
						break
					}
				}
			}
			testRestartNode(t, ports, "temp_binaries/"+fileName+"/cockroach")
			wg.Done()
		}()
	}
	wg.Wait()
}

func testRestartNode(t *testing.T, ports []int, binaryPath string) {
	const pollListenURLTimeout = 150
	ts, err := testserver.NewTestServer(
		testserver.ThreeNodeOpt(),
		testserver.StoreOnDiskOpt(),
		testserver.AddListenAddrPortOpt(ports[0]),
		testserver.AddListenAddrPortOpt(ports[1]),
		testserver.AddListenAddrPortOpt(ports[2]),
		testserver.CockroachBinaryPathOpt(binaryPath),
		testserver.PollListenURLTimeoutOpt(pollListenURLTimeout))
	require.NoError(t, err)
	defer ts.Stop()
	for i := 0; i < 3; i++ {
		require.NoError(t, ts.WaitForInitFinishForNode(i))
	}

	log.Printf("Stopping Node 0")
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
	require.NoError(t, err)
	var out int
	row := db.QueryRow("SELECT 1")
	err = row.Scan(&out)
	require.NoError(t, err)
	require.NoError(t, db.Close())
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
		require.NoError(t, exec.Command("rm", "-rf", "./temp_binaries").Run())
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

// testFlockWithDownloadPassing is to test the flock over downloaded CRDB binary with
// two goroutines, the second goroutine waits for the first goroutine to
// finish downloading the CRDB binary into a local file.
func testFlockWithDownloadPassing(
	t *testing.T, opts ...testserver.TestServerOpt,
) (*sql.DB, func()) {
	var wg = sync.WaitGroup{}

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
	_, latestStableVersion, err := testserver.GetDownloadURL("", nonStable)
	if err != nil {
		return "", err
	}
	filename, err := testserver.GetDownloadFilename(latestStableVersion)
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

func TestLocalityFlagsOpt(t *testing.T) {
	ts, err := testserver.NewTestServer(
		testserver.ThreeNodeOpt(),
		testserver.LocalityFlagsOpt("region=us-east1", "region=us-central1", "region=us-west1"))
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		ts.WaitForInitFinishForNode(i)
	}

	db, err := sql.Open("postgres", ts.PGURL().String())
	require.NoError(t, err)

	found := map[string]bool{}

	rows, err := db.Query("SELECT region FROM [SHOW REGIONS]")
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var region string
		require.NoError(t, rows.Scan(&region))
		found[region] = true
	}

	require.Equal(t, map[string]bool{
		"us-east1":    true,
		"us-central1": true,
		"us-west1":    true,
	}, found)
}

func TestCockroachLogsDirOpt(t *testing.T) {
	logsDir, err := os.MkdirTemp("", "logs-dir-opt")
	require.NoError(t, err)
	defer require.NoError(t, os.RemoveAll(logsDir))

	ts, err := testserver.NewTestServer(
		testserver.ThreeNodeOpt(),
		testserver.CockroachLogsDirOpt(logsDir))
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		if err := ts.WaitForInitFinishForNode(i); err != nil {
			// Make sure we stop the testserver in this case as well.
			ts.Stop()
			require.NoError(t, err)
		}
	}

	// This should delete all resources, but log files should
	// continue to exist under `logsDir`.
	ts.Stop()

	for _, nodeID := range []string{"0", "1", "2"} {
		for _, logFile := range []string{"cockroach.stdout", "cockroach.stderr"} {
			_, err := os.Stat(filepath.Join(logsDir, nodeID, logFile))
			require.NoError(t, err)
		}
	}
}

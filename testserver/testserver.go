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

// Package testserver provides helpers to run a cockroach binary within tests.
// It automatically downloads the latest cockroach binary for your platform,
// or attempts to run "cockroach" from your PATH.
//
// To use, run as follows:
//
//	import "github.com/cockroachdb/cockroach-go/v2/testserver"
//	import "testing"
//	import "time"
//
//	func TestRunServer(t *testing.T) {
//	   ts, err := testserver.NewTestServer()
//	   if err != nil {
//	     t.Fatal(err)
//	   }
//	   defer ts.Stop()
//
//	   db, err := sql.Open("postgres", ts.PGURL().String())
//	   if err != nil {
//	     t.Fatal(err)
//	   }
//	 }
package testserver

import (
	"bufio"
	"bytes"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/testserver/version"
	// Import postgres driver.
	_ "github.com/lib/pq"
)

var customBinaryFlag = flag.String("cockroach-binary", "", "Use specified cockroach binary")

const (
	stateNew = 1 + iota
	stateRunning
	stateStopped
	stateFailed
)

const (
	// First tenant ID to use is 2 since 1 belongs to the system tenant. Refer
	// to NewTenantServer for more information.
	firstTenantID = 2
)

// By default, we allocate 20% of available memory to the test server.
const defaultStoreMemSize = 0.2

const defaultInitTimeout = 60
const defaultPollListenURLTimeout = 60
const defaultListenAddrHost = "localhost"

const testserverMessagePrefix = "cockroach-go testserver"
const tenantserverMessagePrefix = "cockroach-go tenantserver"

// TestServer is a helper to run a real cockroach node.
type TestServer interface {
	// Start starts the server.
	Start() error
	// Stop stops the server and cleans up any associated resources.
	Stop()

	// Stdout returns the entire contents of the first node's stdout.
	Stdout() string
	// Stdout returns the entire contents of the first node's stderr.
	Stderr() string
	// StdoutForNode returns the entire contents of the node's stdout.
	StdoutForNode(i int) string
	// StderrForNode returns the entire contents of the node's stderr.
	StderrForNode(i int) string
	// PGURL returns the postgres connection URL to this server.
	PGURL() *url.URL
	// WaitForInit retries until a SQL connection is successfully established to
	// this server.
	WaitForInit() error
	// BaseDir returns directory StoreOnDiskOpt writes to if used.
	BaseDir() string

	// WaitForInitFinishForNode waits until a node has completed
	// initialization and is available to connect to and query on.
	WaitForInitFinishForNode(numNode int) error
	// StartNode runs the "cockroach start" command for the node.
	StartNode(i int) error
	// StopNode kills the node's process.
	StopNode(i int) error
	// UpgradeNode stops the node, then starts the node on the with the
	// binary specified at "upgradeBinaryPath".
	UpgradeNode(i int) error
	// PGURLForNode returns the PGUrl for the node.
	PGURLForNode(nodeNum int) *url.URL
}

type pgURLChan struct {
	// started will be closed after the start command is executed.
	started chan struct{}
	// set will be closed once the URL is available after startup.
	set chan struct{}
	u   *url.URL
	// The original URL is preserved here if we are using a custom password.
	// In that case, the one below uses client certificates, if secure (and
	// no password otherwise).
	orig url.URL
}

// nodeInfo contains the info to start a node and the state of the node.
type nodeInfo struct {
	startCmd         *exec.Cmd
	startCmdArgs     []string
	listeningURLFile string
	state            int
	stdout           string
	stderr           string
	stdoutBuf        logWriter
	stderrBuf        logWriter
}

// testServerImpl is a TestServer implementation.
type testServerImpl struct {
	mu          sync.RWMutex
	version     *version.Version
	serverArgs  testServerArgs
	serverState int
	baseDir     string
	pgURL       []pgURLChan
	initCmd     *exec.Cmd
	initCmdArgs []string
	nodes       []nodeInfo

	// curTenantID is used to allocate tenant IDs. Refer to NewTenantServer for
	// more information.
	curTenantID  int
	proxyAddr    string      // empty if no sql proxy running yet
	proxyProcess *os.Process // empty if no sql proxy running yet
}

// NewDBForTest creates a new CockroachDB TestServer instance and
// opens a SQL database connection to it. Returns a sql *DB instance and a
// shutdown function. The caller is responsible for executing the
// returned shutdown function on exit.
func NewDBForTest(t *testing.T, opts ...TestServerOpt) (*sql.DB, func()) {
	t.Helper()
	return NewDBForTestWithDatabase(t, "", opts...)
}

// NewDBForTestWithDatabase creates a new CockroachDB TestServer
// instance and opens a SQL database connection to it. If database is
// specified, the returned connection will explicitly connect to
// it. Returns a sql *DB instance a shutdown function. The caller is
// responsible for executing the returned shutdown function on exit.
func NewDBForTestWithDatabase(
	t *testing.T, database string, opts ...TestServerOpt,
) (*sql.DB, func()) {
	t.Helper()
	ts, err := NewTestServer(opts...)
	if err != nil {
		if errors.Is(err, errStoppedInMiddle) {
			// If the testserver is intentionally killed in the middle,
			// make sure it is stopped.
			return nil, func() {
				if ts != nil {
					ts.Stop()
				}
			}
		}
		t.Fatal(err)
	}
	url := ts.PGURL()
	if len(database) > 0 {
		url.Path = database
	}

	db, err := sql.Open("postgres", url.String())
	if err != nil {
		t.Fatalf("%s: %v", testserverMessagePrefix, err)
	}

	return db, func() {
		_ = db.Close()
		ts.Stop()
	}
}

// TestServerOpt is passed to NewTestServer.
type TestServerOpt func(args *testServerArgs)

type TestConfig struct {
	IsTest               bool
	StopDownloadInMiddle bool
}

type testServerArgs struct {
	secure                      bool
	rootPW                      string  // if nonempty, set as pw for root
	storeOnDisk                 bool    // to save database in disk
	storeMemSize                float64 // the proportion of available memory allocated to test server
	httpPorts                   []int
	listenAddrPorts             []int
	listenAddrHost              string
	testConfig                  TestConfig
	nonStableDB                 bool
	customVersion               string // custom cockroach version to use
	cockroachBinary             string // path to cockroach executable file
	upgradeCockroachBinary      string // path to cockroach binary for upgrade
	numNodes                    int
	externalIODir               string
	initTimeoutSeconds          int
	pollListenURLTimeoutSeconds int
	envVars                     []string // to be passed to cmd.Env
}

// CockroachBinaryPathOpt is a TestServer option that can be passed to
// NewTestServer to specify the path of the cockroach binary. This can be used
// to avoid downloading cockroach if running tests in an environment with no
// internet connection, for instance.
func CockroachBinaryPathOpt(executablePath string) TestServerOpt {
	return func(args *testServerArgs) {
		args.cockroachBinary = executablePath
	}
}

func UpgradeCockroachBinaryPathOpt(executablePath string) TestServerOpt {
	return func(args *testServerArgs) {
		args.upgradeCockroachBinary = executablePath
	}
}

// SecureOpt is a TestServer option that can be passed to NewTestServer to
// enable secure mode.
func SecureOpt() TestServerOpt {
	return func(args *testServerArgs) {
		args.secure = true
	}
}

// StoreOnDiskOpt is a TestServer option that can be passed to NewTestServer
// to enable storing database in memory.
func StoreOnDiskOpt() TestServerOpt {
	return func(args *testServerArgs) {
		args.storeOnDisk = true
	}
}

// SetStoreMemSizeOpt is a TestServer option that can be passed to NewTestServer
// to set the proportion of available memory that is allocated
// to the test server.
func SetStoreMemSizeOpt(memSize float64) TestServerOpt {
	return func(args *testServerArgs) {
		if memSize > 0 {
			args.storeMemSize = memSize
		} else {
			args.storeMemSize = defaultStoreMemSize
		}
	}
}

// RootPasswordOpt is a TestServer option that, when passed to NewTestServer,
// sets the given password for the root user (and returns a URL using it from
// PGURL(). This avoids having to use client certs.
func RootPasswordOpt(pw string) TestServerOpt {
	return func(args *testServerArgs) {
		args.rootPW = pw
	}
}

// NonStableDbOpt is a TestServer option that can be passed to NewTestServer to
// download the latest beta version of CRDB, but not necessary a stable one.
func NonStableDbOpt() TestServerOpt {
	return func(args *testServerArgs) {
		args.nonStableDB = true
	}
}

// CustomVersionOpt is a TestServer option that can be passed to NewTestServer to
// download the a specific version of CRDB.
func CustomVersionOpt(version string) TestServerOpt {
	return func(args *testServerArgs) {
		args.customVersion = version
	}
}

// ExposeConsoleOpt is a TestServer option that can be passed to NewTestServer to
// expose the console of the server on the given port.
// Warning: This is kept around for backwards compatibility, use AddHttpPortOpt
// instead.
func ExposeConsoleOpt(port int) TestServerOpt {
	return func(args *testServerArgs) {
		args.httpPorts = []int{port}
	}
}

// AddHttpPortOpt is a TestServer option that can be passed to NewTestServer to
// specify the http ports for the Cockroach nodes.
func AddHttpPortOpt(port int) TestServerOpt {
	return func(args *testServerArgs) {
		args.httpPorts = append(args.httpPorts, port)
	}
}

// AddListenAddrPortOpt is a TestServer option that can be passed to NewTestServer to
// specify the ports for the Cockroach nodes.
// In the case of restarting nodes, it is up to the user of TestServer to make
// sure the port used here cannot be re-used.
func AddListenAddrPortOpt(port int) TestServerOpt {
	return func(args *testServerArgs) {
		args.listenAddrPorts = append(args.listenAddrPorts, port)
	}
}

// ListenAddrHostOpt is a TestServer option that can be passed to
// NewTestServer to specify the host for Cockroach to listen on. By default,
// this is `localhost`, and the most common override is 0.0.0.0.
func ListenAddrHostOpt(host string) TestServerOpt {
	return func(args *testServerArgs) {
		args.listenAddrHost = host
	}
}

// StopDownloadInMiddleOpt is a TestServer option used only in testing.
// It is used to test the flock over downloaded CRDB binary.
// It should not be used in production.
func StopDownloadInMiddleOpt() TestServerOpt {
	return func(args *testServerArgs) {
		tc := TestConfig{IsTest: true, StopDownloadInMiddle: true}
		args.testConfig = tc
	}
}

func ThreeNodeOpt() TestServerOpt {
	return func(args *testServerArgs) {
		args.numNodes = 3
	}
}

// ExternalIODirOpt is a TestServer option that can be passed to NewTestServer to
// specify the external IO directory to be used for the cluster.
func ExternalIODirOpt(ioDir string) TestServerOpt {
	return func(args *testServerArgs) {
		args.externalIODir = ioDir
	}
}

func InitTimeoutOpt(timeout int) TestServerOpt {
	return func(args *testServerArgs) {
		args.initTimeoutSeconds = timeout
	}
}

func PollListenURLTimeoutOpt(timeout int) TestServerOpt {
	return func(args *testServerArgs) {
		args.pollListenURLTimeoutSeconds = timeout
	}
}

// EnvVarOpt is a list of environment variables to be passed to the start
// command. Each entry in the slice should be in `key=value` format.
func EnvVarOpt(vars []string) TestServerOpt {
	return func(args *testServerArgs) {
		args.envVars = vars
	}
}

const (
	logsDirName  = "logs"
	certsDirName = "certs"
)

var errStoppedInMiddle = errors.New("download stopped in middle")

// NewTestServer creates a new TestServer and starts it.
// It also waits until the server is ready to accept clients,
// so it safe to connect to the server returned by this function right away.
// The cockroach binary for your OS and ARCH is downloaded automatically.
// If the download fails, we attempt just call "cockroach", hoping it is
// found in your path.
func NewTestServer(opts ...TestServerOpt) (TestServer, error) {
	serverArgs := &testServerArgs{numNodes: 1}
	serverArgs.storeMemSize = defaultStoreMemSize
	serverArgs.initTimeoutSeconds = defaultInitTimeout
	serverArgs.pollListenURLTimeoutSeconds = defaultPollListenURLTimeout
	serverArgs.listenAddrHost = defaultListenAddrHost
	for _, applyOptToArgs := range opts {
		applyOptToArgs(serverArgs)
	}

	if serverArgs.cockroachBinary != "" {
		// CockroachBinaryPathOpt() overrides the flag or env variable.
	} else if len(*customBinaryFlag) > 0 {
		serverArgs.cockroachBinary = *customBinaryFlag
	} else if customBinaryEnv := os.Getenv("COCKROACH_BINARY"); customBinaryEnv != "" {
		serverArgs.cockroachBinary = customBinaryEnv
	}

	if len(serverArgs.listenAddrPorts) == 0 {
		serverArgs.listenAddrPorts = make([]int, serverArgs.numNodes)
	}
	if serverArgs.numNodes != 1 && len(serverArgs.listenAddrPorts) != serverArgs.numNodes {
		panic(fmt.Sprintf("need to specify a port for each node using AddListenAddrPortOpt, got %d nodes, need %d ports",
			serverArgs.numNodes, len(serverArgs.listenAddrPorts)))
	}

	var err error
	if serverArgs.cockroachBinary != "" {
		log.Printf("Using custom cockroach binary: %s", serverArgs.cockroachBinary)
		cockroachBinary, err := filepath.Abs(serverArgs.cockroachBinary)
		if err == nil {
			// Update path to absolute.
			serverArgs.cockroachBinary = cockroachBinary
		}
	} else {
		serverArgs.cockroachBinary, err = DownloadBinary(&serverArgs.testConfig, serverArgs.customVersion, serverArgs.nonStableDB)
		if err != nil {
			if errors.Is(err, errStoppedInMiddle) {
				// If the testserver is intentionally killed in the middle of downloading,
				// return error.
				return nil, err
			}
			log.Printf("%s: Failed to fetch latest binary: %v attempting to use cockroach binary from your PATH", testserverMessagePrefix, err)
			serverArgs.cockroachBinary = "cockroach"
		} else {
			log.Printf("Using automatically-downloaded binary: %s", serverArgs.cockroachBinary)
		}
	}

	baseDir, err := os.MkdirTemp("", "cockroach-testserver")
	if err != nil {
		return nil, fmt.Errorf("%s: could not create temp directory: %w", testserverMessagePrefix, err)
	}

	mkDir := func(name string) (string, error) {
		path := filepath.Join(baseDir, name)
		if err := os.MkdirAll(path, 0755); err != nil {
			return "", fmt.Errorf("%s: could not create %s directory: %s: %w",
				testserverMessagePrefix, name, path, err)
		}
		return path, nil
	}
	certsDir, err := mkDir(certsDirName)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", testserverMessagePrefix, err)
	}

	secureOpt := "--insecure"
	if serverArgs.secure {
		// Create certificates.
		certArgs := []string{
			"--certs-dir=" + certsDir,
			"--ca-key=" + filepath.Join(certsDir, "ca.key"),
		}
		for _, args := range [][]string{
			// Create the CA cert and key pair.
			{"cert", "create-ca"},
			// Create cert and key pair for the cockroach node.
			{"cert", "create-node", "localhost"},
			// Create cert and key pair for the root user (SQL client).
			{"cert", "create-client", "root", "--also-generate-pkcs8-key"},
		} {
			createCertCmd := exec.Command(serverArgs.cockroachBinary, append(args, certArgs...)...)
			log.Printf("%s executing: %s", testserverMessagePrefix, createCertCmd)
			if err := createCertCmd.Run(); err != nil {
				return nil, fmt.Errorf("%s command %s failed: %w", testserverMessagePrefix, createCertCmd, err)
			}
		}
		secureOpt = "--certs-dir=" + certsDir
	}

	// v19.1 and earlier do not have the `start-single-node` subcommand,
	// so use `start` for those versions.
	// TODO(rafi): Remove the version check and `start` once we stop testing 19.1.
	versionCmd := exec.Command(serverArgs.cockroachBinary, "version")
	versionOutput, err := versionCmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s command %s failed: %w", testserverMessagePrefix, versionCmd, err)
	}
	reader := bufio.NewReader(bytes.NewReader(versionOutput))
	versionLine, err := reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("%s failed to read version: %w", testserverMessagePrefix, err)
	}
	versionLineTokens := strings.Fields(versionLine)
	v, err := version.Parse(versionLineTokens[2])
	if err != nil {
		return nil, fmt.Errorf("%s failed to parse version: %w", testserverMessagePrefix, err)
	}

	startCmd := "start-single-node"
	if !v.AtLeast(version.MustParse("v19.2.0-alpha")) || serverArgs.numNodes > 1 {
		startCmd = "start"
	}

	nodes := make([]nodeInfo, serverArgs.numNodes)
	if len(serverArgs.httpPorts) == 0 {
		serverArgs.httpPorts = make([]int, serverArgs.numNodes)
	}

	if serverArgs.externalIODir == "" {
		serverArgs.externalIODir = "disabled"
	}

	for i := 0; i < serverArgs.numNodes; i++ {
		storeArg := fmt.Sprintf("--store=type=mem,size=%.2f", serverArgs.storeMemSize)
		nodeBaseDir := filepath.Join(baseDir, strconv.Itoa(i))
		if serverArgs.storeOnDisk {
			storeArg = fmt.Sprintf("--store=path=%s", nodeBaseDir)
		}
		// TODO(janexing): Make sure the log is written to logDir instead of shown in console.
		// Should be done once issue #109 is solved:
		// https://github.com/cockroachdb/cockroach-go/issues/109
		nodes[i].stdout = filepath.Join(nodeBaseDir, "cockroach.stdout")
		nodes[i].stderr = filepath.Join(nodeBaseDir, "cockroach.stderr")
		nodes[i].listeningURLFile = filepath.Join(nodeBaseDir, "listen-url")
		nodes[i].state = stateNew
		if serverArgs.numNodes > 1 {
			nodes[i].startCmdArgs = []string{
				serverArgs.cockroachBinary,
				startCmd,
				secureOpt,
				storeArg,
				fmt.Sprintf(
					"--listen-addr=%s:%d",
					serverArgs.listenAddrHost,
					serverArgs.listenAddrPorts[i],
				),
				fmt.Sprintf(
					"--http-addr=%s:%d",
					serverArgs.listenAddrHost,
					serverArgs.httpPorts[i],
				),
				"--listening-url-file=" + nodes[i].listeningURLFile,
				"--external-io-dir=" + serverArgs.externalIODir,
			}
		} else {
			nodes[0].startCmdArgs = []string{
				serverArgs.cockroachBinary,
				startCmd,
				"--logtostderr",
				secureOpt,
				fmt.Sprintf("--host=%s", serverArgs.listenAddrHost),
				"--port=" + strconv.Itoa(serverArgs.listenAddrPorts[0]),
				"--http-port=" + strconv.Itoa(serverArgs.httpPorts[0]),
				storeArg,
				"--listening-url-file=" + nodes[i].listeningURLFile,
				"--external-io-dir=" + serverArgs.externalIODir,
			}
		}
	}

	// We only need initArgs if we're creating a testserver
	// with multiple nodes.
	initArgs := []string{
		serverArgs.cockroachBinary,
		"init",
		secureOpt,
	}

	states := make([]int, serverArgs.numNodes)
	for i := 0; i < serverArgs.numNodes; i++ {
		states[i] = stateNew
	}

	ts := &testServerImpl{
		serverArgs:  *serverArgs,
		version:     v,
		serverState: stateNew,
		baseDir:     baseDir,
		initCmdArgs: initArgs,
		curTenantID: firstTenantID,
		nodes:       nodes,
	}
	ts.pgURL = make([]pgURLChan, serverArgs.numNodes)
	for i := range ts.pgURL {
		ts.pgURL[i].started = make(chan struct{})
		ts.pgURL[i].set = make(chan struct{})
	}

	if err := ts.Start(); err != nil {
		return nil, fmt.Errorf("%s Start failed: %w", testserverMessagePrefix, err)
	}

	if ts.PGURL() == nil {
		return nil, fmt.Errorf("%s: url not found", testserverMessagePrefix)
	}

	if err := ts.WaitForInit(); err != nil {
		return nil, fmt.Errorf("%s WaitForInit failed: %w", testserverMessagePrefix, err)
	}

	return ts, nil
}

// Stdout returns the entire contents of the first node stdout.
func (ts *testServerImpl) Stdout() string {
	return ts.StdoutForNode(0)
}

// Stderr returns the entire contents of the first node stderr.
func (ts *testServerImpl) Stderr() string {
	return ts.StderrForNode(0)
}

// StdoutForNode returns the entire contents of the node's stdout.
func (ts *testServerImpl) StdoutForNode(i int) string {
	return ts.nodes[i].stdoutBuf.String()
}

// StderrForNode returns the entire contents of the node's stderr.
func (ts *testServerImpl) StderrForNode(i int) string {
	return ts.nodes[i].stderrBuf.String()
}

// BaseDir returns directory StoreOnDiskOpt writes to if used.
func (ts *testServerImpl) BaseDir() string {
	return ts.baseDir
}

// PGURL returns the postgres connection URL to reach the started
// cockroach node.
//
// It blocks until the network URL is determined and does not timeout,
// relying instead on test timeouts.
func (ts *testServerImpl) PGURL() *url.URL {
	return ts.PGURLForNode(0)
}

func (ts *testServerImpl) setPGURL(u *url.URL) {
	ts.setPGURLForNode(0, u)
}

func (ts *testServerImpl) PGURLForNode(nodeNum int) *url.URL {
	<-ts.pgURL[nodeNum].set
	return ts.pgURL[nodeNum].u
}

func (ts *testServerImpl) setPGURLForNode(nodeNum int, u *url.URL) {
	ts.pgURL[nodeNum].u = u
	close(ts.pgURL[nodeNum].set)
}

func (ts *testServerImpl) WaitForInitFinishForNode(nodeIdx int) error {
	pgURL := ts.PGURLForNode(nodeIdx).String()
	for i := 0; i < ts.serverArgs.initTimeoutSeconds*10; i++ {
		err := func() error {
			db, err := sql.Open("postgres", pgURL)
			if err != nil {
				return err
			}
			defer func() { _ = db.Close() }()
			var s string
			if err := db.QueryRow("SELECT 'started'").Scan(&s); err != nil {
				return err
			}
			if s != "started" {
				return fmt.Errorf("healthcheck query had incorrect result")
			}
			return nil
		}()
		if err == nil {
			return nil
		}
		log.Printf("%s: WaitForInitFinishForNode %d (%s): Trying again after error: %v", testserverMessagePrefix, nodeIdx, pgURL, err)
		time.Sleep(time.Millisecond * 100)
	}
	log.Printf(
		"init did not finish for node %d\n\nstdout:\n%s\n\nstderr:\n:%s",
		nodeIdx,
		ts.StdoutForNode(nodeIdx),
		ts.StderrForNode(nodeIdx),
	)
	return fmt.Errorf("init did not finish for node %d", nodeIdx)
}

// WaitForInit retries until a connection is successfully established.
func (ts *testServerImpl) WaitForInit() error {
	return ts.WaitForInitFinishForNode(0)
}

func (ts *testServerImpl) pollListeningURLFile(nodeNum int) error {
	var data []byte
	for i := 0; i < ts.serverArgs.pollListenURLTimeoutSeconds*10; i++ {
		ts.mu.RLock()
		state := ts.nodes[nodeNum].state
		ts.mu.RUnlock()
		if state != stateRunning {
			return fmt.Errorf("server stopped or crashed before listening URL file was available")
		}
		var err error
		data, err = os.ReadFile(ts.nodes[nodeNum].listeningURLFile)
		if len(data) == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err == nil {
			break
		} else if !os.IsNotExist(err) {
			return fmt.Errorf("unexpected error while reading listening URL file: %w", err)
		}
	}

	if len(data) == 0 {
		panic("empty connection string")
	}

	u, err := url.Parse(string(bytes.TrimSpace(data)))
	if err != nil {
		return fmt.Errorf("failed to parse SQL URL: %w", err)
	}
	ts.pgURL[nodeNum].orig = *u
	if pw := ts.serverArgs.rootPW; pw != "" {
		db, err := sql.Open("postgres", u.String())
		if err != nil {
			return err
		}
		defer db.Close()
		if _, err := db.Exec(`ALTER USER root WITH PASSWORD $1`, pw); err != nil {
			return err
		}

		v := u.Query()
		v.Del("sslkey")
		v.Del("sslcert")
		u.RawQuery = v.Encode()
		u.User = url.UserPassword("root", pw)
	}

	ts.setPGURLForNode(nodeNum, u)

	return nil
}

// Start runs the process, returning an error on any problems,
// including being unable to start, but not unexpected failure.
// It should only be called once in the lifetime of a TestServer object.
// If the server is already running, this function is a no-op.
// If the server stopped or failed, please don't use ts.Start()
// to restart a testserver, but use NewTestServer().
func (ts *testServerImpl) Start() error {
	ts.mu.Lock()
	if ts.serverState != stateNew {
		ts.mu.Unlock()
		switch ts.serverState {
		case stateRunning:
			return nil // No-op if server is already running.
		case stateStopped, stateFailed:
			// Start() can only be called once.
			return errors.New(
				"`Start()` cannot be used to restart a stopped or failed server. " +
					"Please use NewTestServer()")
		}
	}
	ts.serverState = stateRunning
	ts.mu.Unlock()

	for i := 0; i < ts.serverArgs.numNodes; i++ {
		if err := ts.StartNode(i); err != nil {
			return err
		}
	}

	if ts.serverArgs.numNodes > 1 {
		err := ts.CockroachInit()
		if err != nil {
			return err
		}
	}

	return nil
}

// Stop kills the process if it is still running and cleans its directory.
// It should only be called once in the lifetime of a TestServer object.
// Logs fatal if the process has already failed.
func (ts *testServerImpl) Stop() {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	if ts.serverState == stateNew {
		log.Fatalf("%s: Stop() called, but Start() was never called", testserverMessagePrefix)
	}
	if ts.serverState == stateFailed {
		log.Fatalf("%s: Stop() called, but process exited unexpectedly. Stdout:\n%s\nStderr:\n%s\n",
			testserverMessagePrefix,
			ts.Stdout(),
			ts.Stderr())
		return
	}

	if ts.serverState != stateStopped {
		if p := ts.proxyProcess; p != nil {
			_ = p.Kill()
		}
	}

	ts.serverState = stateStopped
	for i, node := range ts.nodes {
		cmd := node.startCmd
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}

		if node.state != stateFailed {
			node.state = stateStopped
		}

		if node.state != stateStopped {
			ts.serverState = stateFailed
		}

		// RUnlock such that StopNode can Lock and Unlock.
		ts.mu.RUnlock()
		err := ts.StopNode(i)
		if err != nil {
			log.Printf("error stopping node %d: %s", i, err)
		}
		ts.mu.RLock()

		nodeDir := filepath.Join(ts.baseDir, strconv.Itoa(i))
		if err := os.RemoveAll(nodeDir); err != nil {
			log.Printf("error deleting tmp directory %s for node: %s", nodeDir, err)
		}
		if closeErr := ts.nodes[i].stdoutBuf.Close(); closeErr != nil {
			log.Printf("%s: failed to close stdout: %v", testserverMessagePrefix, closeErr)
		}
		if closeErr := ts.nodes[i].stderrBuf.Close(); closeErr != nil {
			log.Printf("%s: failed to close stderr: %v", testserverMessagePrefix, closeErr)
		}
	}

	// Only cleanup on intentional stops.
	_ = os.RemoveAll(ts.baseDir)
}

func (ts *testServerImpl) CockroachInit() error {
	// The port must be computed here, since it may not be known until after
	// a node is started (if the listen port is 0).
	args := append(ts.initCmdArgs, fmt.Sprintf("--host=localhost:%s", ts.PGURL().Port()))
	ts.initCmd = exec.Command(args[0], args[1:]...)
	ts.initCmd.Env = []string{
		"COCKROACH_MAX_OFFSET=1ns",
		"COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR=true",
	}

	// Set the working directory of the cockroach process to our temp folder.
	// This stops cockroach from polluting the project directory with _dump
	// folders.
	ts.initCmd.Dir = ts.baseDir

	err := ts.initCmd.Start()
	if ts.initCmd.Process != nil {
		log.Printf("process %d started: %s", ts.initCmd.Process.Pid, strings.Join(args, " "))
	}
	if err != nil {
		return err
	}
	return nil
}

type logWriter interface {
	Write(p []byte) (n int, err error)
	String() string
	Len() int64
	Close() error
}

type fileLogWriter struct {
	filename string
	file     *os.File
}

func newFileLogWriter(file string) (*fileLogWriter, error) {
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		return nil, err
	}
	// If the file doesn't exist, create it, else append to the file.
	f, err := os.OpenFile(file, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &fileLogWriter{
		filename: file,
		file:     f,
	}, nil
}

func (w fileLogWriter) Close() error {
	return w.file.Close()
}

func (w fileLogWriter) Write(p []byte) (n int, err error) {
	return w.file.Write(p)
}

func (w fileLogWriter) String() string {
	b, err := os.ReadFile(w.filename)
	if err == nil {
		return string(b)
	}
	return ""
}

func (w fileLogWriter) Len() int64 {
	s, err := os.Stat(w.filename)
	if err == nil {
		return s.Size()
	}
	return 0
}

func defaultEnv() map[string]string {
	vars := map[string]string{}
	u, err := user.Current()
	if err == nil {
		if _, ok := vars["USER"]; !ok {
			vars["USER"] = u.Username
		}
		if _, ok := vars["UID"]; !ok {
			vars["UID"] = u.Uid
		}
		if _, ok := vars["GID"]; !ok {
			vars["GID"] = u.Gid
		}
		if _, ok := vars["HOME"]; !ok {
			vars["HOME"] = u.HomeDir
		}
	}
	if _, ok := vars["PATH"]; !ok {
		vars["PATH"] = os.Getenv("PATH")
	}
	return vars
}

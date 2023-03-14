// Copyright 2022 The Cockroach Authors.
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

package testserver

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

func (ts *testServerImpl) StopNode(nodeNum int) error {
	ts.mu.Lock()
	ts.nodes[nodeNum].state = stateStopped
	ts.mu.Unlock()
	cmd := ts.nodes[nodeNum].startCmd

	// Kill the process.
	if cmd.Process != nil {
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			return err
		}
		if _, err := cmd.Process.Wait(); err != nil {
			return err
		}
	}
	// Reset the pgURL, since it could change if the node is started later;
	// specifically, if the listen port is 0 then the port will change.
	ts.pgURL[nodeNum] = pgURLChan{}
	ts.pgURL[nodeNum].started = make(chan struct{})
	ts.pgURL[nodeNum].set = make(chan struct{})

	if err := os.Remove(ts.nodes[nodeNum].listeningURLFile); err != nil {
		return err
	}

	return nil
}

func (ts *testServerImpl) StartNode(i int) error {
	ts.mu.RLock()
	if ts.nodes[i].state == stateRunning {
		return fmt.Errorf("node %d already running", i)
	}
	ts.mu.RUnlock()

	// We need to compute the join addresses here. since if the listen port is
	// 0, then the actual port will not be known until a node is started.
	var joinAddrs []string
	for otherNodeID := range ts.nodes {
		if i == otherNodeID {
			continue
		}
		if ts.serverArgs.listenAddrPorts[otherNodeID] != 0 {
			joinAddrs = append(joinAddrs, fmt.Sprintf("localhost:%d", ts.serverArgs.listenAddrPorts[otherNodeID]))
			continue
		}
		select {
		case <-ts.pgURL[otherNodeID].started:
			// PGURLForNode will block until the URL is ready. If something
			// goes wrong, the goroutine waiting on pollListeningURLFile
			// will time out.
			joinAddrs = append(joinAddrs, fmt.Sprintf("localhost:%s", ts.PGURLForNode(otherNodeID).Port()))
		default:
			// If the other node hasn't started yet, don't add the join arg.
		}
	}
	joinArg := fmt.Sprintf("--join=%s", strings.Join(joinAddrs, ","))

	args := ts.nodes[i].startCmdArgs
	if len(ts.nodes) > 1 {
		if len(joinAddrs) == 0 {
			// The start command always requires a --join arg, so we fake one
			// if we don't have any yet.
			joinArg = "--join=localhost:0"
		}
		args = append(args, joinArg)
	}
	ts.nodes[i].startCmd = exec.Command(args[0], args[1:]...)

	currCmd := ts.nodes[i].startCmd
	currCmd.Env = []string{
		"COCKROACH_MAX_OFFSET=1ns",
		"COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR=true",
	}
	currCmd.Env = append(currCmd.Env, ts.serverArgs.envVars...)

	// Set the working directory of the cockroach process to our temp folder.
	// This stops cockroach from polluting the project directory with _dump
	// folders.
	currCmd.Dir = ts.baseDir

	if len(ts.nodes[i].stdout) > 0 {
		wr, err := newFileLogWriter(ts.nodes[i].stdout)
		if err != nil {
			return fmt.Errorf("unable to open file %s: %w", ts.nodes[i].stdout, err)
		}
		ts.nodes[i].stdoutBuf = wr
	}
	currCmd.Stdout = ts.nodes[i].stdoutBuf

	if len(ts.nodes[i].stderr) > 0 {
		wr, err := newFileLogWriter(ts.nodes[i].stderr)
		if err != nil {
			return fmt.Errorf("unable to open file %s: %w", ts.nodes[1].stderr, err)
		}
		ts.nodes[i].stderrBuf = wr
	}
	currCmd.Stderr = ts.nodes[i].stderrBuf

	for k, v := range defaultEnv() {
		currCmd.Env = append(currCmd.Env, k+"="+v)
	}

	log.Printf("executing: %s", currCmd)
	err := currCmd.Start()
	close(ts.pgURL[i].started)
	if currCmd.Process != nil {
		log.Printf("process %d started. env=%s; cmd: %s", currCmd.Process.Pid, currCmd.Env, strings.Join(args, " "))
	}
	if err != nil {
		log.Print(err.Error())
		ts.mu.Lock()
		ts.nodes[i].state = stateFailed
		ts.mu.Unlock()

		return fmt.Errorf("command %s failed: %w", currCmd, err)
	}

	ts.mu.Lock()
	ts.nodes[i].state = stateRunning
	ts.mu.Unlock()

	capturedI := i

	if ts.pgURL[capturedI].u == nil {
		go func() {
			if err := ts.pollListeningURLFile(capturedI); err != nil {
				log.Printf("%s failed to poll listening URL file: %v", testserverMessagePrefix, err)
				close(ts.pgURL[capturedI].set)
				ts.Stop()
			}
		}()
	}

	return nil
}

func (ts *testServerImpl) UpgradeNode(nodeNum int) error {
	err := ts.StopNode(nodeNum)
	if err != nil {
		return err
	}
	ts.nodes[nodeNum].startCmdArgs[0] = ts.serverArgs.upgradeCockroachBinary
	return ts.StartNode(nodeNum)
}

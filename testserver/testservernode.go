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
	"os/exec"
	"strings"
)

func (ts *testServerImpl) StopNode(nodeNum int) error {
	ts.mu.Lock()
	ts.nodeStates[nodeNum] = stateStopped
	ts.mu.Unlock()
	ts.pgURL[nodeNum].u = nil
	cmd := ts.cmd[nodeNum]

	// Kill the process.
	if cmd.Process != nil {
		return cmd.Process.Kill()
	}

	return nil
}

func (ts *testServerImpl) StartNode(i int) error {
	ts.mu.RLock()
	if ts.nodeStates[i] == stateRunning {
		return fmt.Errorf("node %d already running", i)
	}
	ts.mu.RUnlock()
	ts.cmd[i] = exec.Command(ts.cmdArgs[i][0], ts.cmdArgs[i][1:]...)

	currCmd := ts.cmd[i]
	currCmd.Env = []string{
		"COCKROACH_MAX_OFFSET=1ns",
		"COCKROACH_TRUST_CLIENT_PROVIDED_SQL_REMOTE_ADDR=true",
	}

	// Set the working directory of the cockroach process to our temp folder.
	// This stops cockroach from polluting the project directory with _dump
	// folders.
	currCmd.Dir = ts.baseDir

	if len(ts.stdout) > 0 {
		wr, err := newFileLogWriter(ts.stdout)
		if err != nil {
			return fmt.Errorf("unable to open file %s: %w", ts.stdout, err)
		}
		ts.stdoutBuf = wr
	}
	currCmd.Stdout = ts.stdoutBuf

	if len(ts.stderr) > 0 {
		wr, err := newFileLogWriter(ts.stderr)
		if err != nil {
			return fmt.Errorf("unable to open file %s: %w", ts.stderr, err)
		}
		ts.stderrBuf = wr
	}
	currCmd.Stderr = ts.stderrBuf

	for k, v := range defaultEnv() {
		currCmd.Env = append(currCmd.Env, k+"="+v)
	}

	log.Printf("executing: %s", currCmd)
	err := currCmd.Start()
	if currCmd.Process != nil {
		log.Printf("process %d started: %s", currCmd.Process.Pid, strings.Join(ts.cmdArgs[i], " "))
	}
	if err != nil {
		log.Print(err.Error())
		ts.mu.Lock()
		ts.nodeStates[i] = stateFailed
		ts.mu.Unlock()

		return fmt.Errorf("command %s failed: %w", currCmd, err)
	}

	ts.mu.Lock()
	ts.nodeStates[i] = stateRunning
	ts.mu.Unlock()

	capturedI := i

	if ts.pgURL[capturedI].u == nil {
		ts.pgURL[i].set = make(chan struct{})
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
	ts.cmdArgs[nodeNum][0] = ts.serverArgs.upgradeCockroachBinary
	return ts.StartNode(nodeNum)
}

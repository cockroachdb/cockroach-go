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

package testserver

import (
	"testing"
	"time"
)

func TestRunServer(t *testing.T) {
	ts, err := NewTestServer()
	if err != nil {
		t.Fatal(err)
	}

	err = ts.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer ts.Stop()

	url := ts.WaitForPGURL(time.Second)
	if url == nil {
		t.Fatalf("url not found")
	}
	t.Logf("URL: %s", url.String())
}

func TestBinaryPaths(t *testing.T) {
	t.Logf("binaryName: %s", binaryName())
	t.Logf("latestMarkerURL: %s", latestMarkerURL())
	sha, err := findLatestSha()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("latestSha: %s", sha)
	filename, err := downloadLatestBinary()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("downloaded: %s", filename)
}

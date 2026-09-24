// Copyright 2026 The Cockroach Authors.
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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

const sampleReleaseCatalog = `schema_version: 1
releases:
  - version: v23.1.0-beta.1
    series: v23.1
    date: "2023-01-01"
    kind: testing
    withdrawn: false
    commit: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    platforms: [linux-amd64]
  - version: v23.1.0
    series: v23.1
    date: "2023-05-01"
    kind: production
    withdrawn: false
    commit: bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    platforms: [linux-amd64]
  - version: v23.1.1
    series: v23.1
    date: "2023-06-01"
    kind: production
    withdrawn: true
    commit: cccccccccccccccccccccccccccccccccccccccc
    platforms: [linux-amd64]
  - version: v23.1.2
    series: v23.1
    date: "2023-07-01"
    kind: production
    withdrawn: false
    commit: dddddddddddddddddddddddddddddddddddddddd
    platforms: [linux-amd64]
  - version: beta-20160407
    series: v1.0
    date: "2016-04-07"
    kind: testing
    withdrawn: false
    commit: eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
    platforms: [linux-amd64]
`

func TestLatestStableVersionFromCatalog(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(sampleReleaseCatalog))
	}))
	defer srv.Close()

	v, err := latestStableVersionFromCatalog(srv.URL)
	require.NoError(t, err)
	require.Equal(t, "v23.1.2", v.String())
}

func TestLatestStableVersionFromCatalogRejectsUnsupportedSchemaVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("schema_version: 2\nreleases: []\n"))
	}))
	defer srv.Close()

	_, err := latestStableVersionFromCatalog(srv.URL)
	require.Error(t, err)
}

func TestLatestStableVersionFromCatalogRejectsHTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	_, err := latestStableVersionFromCatalog(srv.URL)
	require.Error(t, err)
}

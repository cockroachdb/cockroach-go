// Copyright 2025 The Cockroach Authors.
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

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"

	"github.com/cockroachdb/cockroach-go/v2/testserver"
)

func main() {
	var (
		platform = flag.String("platform", runtime.GOOS, "Target platform (linux, darwin, windows)")
		arch     = flag.String("arch", runtime.GOARCH, "Target architecture (amd64, arm64)")
		version  = flag.String("version", "unstable", "CockroachDB version to download (use 'unstable' for latest bleeding edge, or specify version like 'v23.1.0')")
		output   = flag.String("output", "", "Output directory (defaults to temp directory)")
		help     = flag.Bool("help", false, "Show help")
	)

	flag.Parse()

	if *help {
		fmt.Fprintf(os.Stderr, "Usage: %s [options]\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Download and extract CockroachDB binaries for specified platform and architecture.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nSupported platforms: linux, darwin, windows\n")
		fmt.Fprintf(os.Stderr, "Supported architectures: amd64, arm64\n")
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s -platform=linux -arch=amd64 -version=v23.1.0\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -platform=darwin -arch=arm64\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -platform=linux -arch=amd64 -version=unstable\n", os.Args[0])
		os.Exit(0)
	}

	// Validate platform
	switch *platform {
	case "linux", "darwin", "windows":
		// Valid platforms
	default:
		log.Fatalf("Unsupported platform: %s. Supported platforms: linux, darwin, windows", *platform)
	}

	// Validate architecture
	switch *arch {
	case "amd64", "arm64":
		// Valid architectures
	default:
		log.Fatalf("Unsupported architecture: %s. Supported architectures: amd64, arm64", *arch)
	}

	// Special case: Windows only supports amd64
	if *platform == "windows" && *arch != "amd64" {
		log.Fatalf("Windows platform only supports amd64 architecture")
	}

	fmt.Printf("Downloading CockroachDB binary for %s-%s", *platform, *arch)

	var actualVersion string
	var nonStable bool

	if *version == "unstable" {
		fmt.Printf(" (latest unstable)")
		actualVersion = ""
		nonStable = true
	} else {
		fmt.Printf(" version %s", *version)
		actualVersion = *version
		nonStable = false
	}
	fmt.Printf("\n")

	// Download the binary
	binaryPath, err := testserver.DownloadBinaryWithPlatform(
		&testserver.TestConfig{},
		actualVersion,
		nonStable,
		*platform,
		*arch,
		*output,
	)
	if err != nil {
		log.Fatalf("Failed to download binary: %v", err)
	}

	fmt.Printf("Successfully downloaded CockroachDB binary to: %s\n", binaryPath)
}

// Copyright 2017 The Cockroach Authors.
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
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach-go/v2/testserver/version"
	"github.com/gofrs/flock"
	"gopkg.in/yaml.v3"
)

const (
	latestSuffix     = "LATEST"
	finishedFileMode = 0o555
	writingFileMode  = 0o600 // Allow reads so that another process can check if there's a flock.
)

const (
	linuxUrlpat = "https://binaries.cockroachdb.com/cockroach-%s.linux-%s.tgz"
	macUrlpat   = "https://binaries.cockroachdb.com/cockroach-%s.darwin-%s-%s.tgz"
	winUrlpat   = "https://binaries.cockroachdb.com/cockroach-%s.windows-6.2-amd64.zip"
)

// releaseDataURL is the location of the YAML file maintained by the
// docs team where release information is encoded. This data is used
// to render the public CockroachDB releases page. We leverage the
// data in structured format to generate release information used
// for testing purposes.
const releaseDataURL = "https://raw.githubusercontent.com/cockroachdb/docs/main/src/current/_data/releases.yml"

// GetDownloadURL returns the URL of a CRDB download. It creates the URL for
// downloading a CRDB binary for current runtime OS. If desiredVersion is
// specified, it will return the URL of the specified version. Otherwise, it
// will return the URL of the latest stable cockroach binary. If nonStable is
// true, the latest cockroach binary will be used.
func GetDownloadURL(desiredVersion string, nonStable bool) (string, string, error) {
	return GetDownloadURLWithPlatform(desiredVersion, nonStable, runtime.GOOS, runtime.GOARCH)
}

// GetDownloadURLWithPlatform returns the URL of a CRDB download for the specified
// platform and architecture. If desiredVersion is specified, it will return the URL
// of the specified version. Otherwise, it will return the URL of the latest stable
// cockroach binary. If nonStable is true, the latest cockroach binary will be used.
func GetDownloadURLWithPlatform(
	desiredVersion string, nonStable bool, goos, goarch string,
) (string, string, error) {
	targetGoos := goos
	if targetGoos == "linux" {
		targetGoos += "-gnu"
	}
	// For unstable builds, macOS ARM64 binaries have ".unsigned" at the end
	var binaryName string
	if nonStable && goos == "darwin" && goarch == "arm64" {
		binaryName = fmt.Sprintf("cockroach.%s-%s.unsigned", targetGoos, goarch)
	} else {
		binaryName = fmt.Sprintf("cockroach.%s-%s", targetGoos, goarch)
	}
	if goos == "windows" {
		binaryName += ".exe"
	}

	var dbUrl string
	var err error

	if desiredVersion != "" {
		dbUrl = getDownloadUrlForVersionWithPlatform(desiredVersion, goos, goarch)
	} else if nonStable {
		// For the latest (beta) CRDB, we use the `edge-binaries.cockroachdb.com` host.
		u := &url.URL{
			Scheme: "https",
			Host:   "edge-binaries.cockroachdb.com",
			Path:   path.Join("cockroach", fmt.Sprintf("%s.%s", binaryName, latestSuffix)),
		}
		dbUrl = u.String()
	} else {
		// For the latest stable CRDB, we use the url provided in the CRDB release page.
		dbUrl, desiredVersion, err = getLatestStableVersionInfo()
		if err != nil {
			return dbUrl, "", err
		}
	}

	return dbUrl, desiredVersion, nil
}

// DownloadFromURL starts a download of the cockroach binary from the given URL.
func DownloadFromURL(downloadURL string) (*http.Response, error) {
	log.Printf("GET %s", downloadURL)
	response, err := http.Get(downloadURL)
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		return nil, fmt.Errorf(
			"error downloading %s: %d (%s)",
			downloadURL,
			response.StatusCode,
			response.Status,
		)
	}

	return response, nil
}

// DownloadBinary saves the latest version of CRDB into a local binary file,
// and returns the path for this local binary.
// To download a specific cockroach version, specify desiredVersion. Otherwise,
// the latest stable or non-stable version will be chosen.
// To download the latest STABLE version of CRDB, set `nonStable` to false.
// To download the bleeding edge version of CRDB, set `nonStable` to true.
func DownloadBinary(tc *TestConfig, desiredVersion string, nonStable bool) (string, error) {
	return DownloadBinaryWithPlatform(tc, desiredVersion, nonStable, runtime.GOOS, runtime.GOARCH, "")
}

// DownloadBinaryWithPlatform saves the specified version of CRDB for the given
// platform and architecture into a local binary file, and returns the path for
// this local binary.
// To download a specific cockroach version, specify desiredVersion. Otherwise,
// the latest stable or non-stable version will be chosen.
// To download the latest STABLE version of CRDB, set `nonStable` to false.
// To download the bleeding edge version of CRDB, set `nonStable` to true.
// If outputDir is specified, the binary will be saved there, otherwise to temp directory.
func DownloadBinaryWithPlatform(
	tc *TestConfig, desiredVersion string, nonStable bool, goos, goarch, outputDir string,
) (string, error) {
	dbUrl, desiredVersion, err := GetDownloadURLWithPlatform(desiredVersion, nonStable, goos, goarch)
	if err != nil {
		return "", err
	}

	// For unstable builds, use "latest" as the version for filename generation
	filenameVersion := desiredVersion
	if nonStable && desiredVersion == "" {
		filenameVersion = "latest"
	}

	filename, err := GetDownloadFilenameWithPlatform(filenameVersion, goos)
	if err != nil {
		return "", err
	}

	var localFile string
	if outputDir != "" {
		// Create output directory if it doesn't exist
		if err := os.MkdirAll(outputDir, 0o755); err != nil {
			return "", fmt.Errorf("failed to create output directory %s: %w", outputDir, err)
		}
		localFile = filepath.Join(outputDir, filename)
	} else {
		localFile = filepath.Join(os.TempDir(), filename)
	}

	// Short circuit if the file already exists and is in the finished state.
	info, err := os.Stat(localFile)
	if err == nil && info.Mode().Perm() == finishedFileMode {
		return localFile, nil
	}

	response, err := DownloadFromURL(dbUrl)
	if err != nil {
		return "", err
	}

	defer func() { _ = response.Body.Close() }()

	for {
		info, err := os.Stat(localFile)
		if os.IsNotExist(err) {
			// File does not exist: download it.
			break
		}
		if err != nil {
			return "", err
		}
		// File already present: check mode.
		if info.Mode().Perm() == finishedFileMode {
			return localFile, nil
		}

		localFileLock := flock.New(localFile)
		// If there's a process downloading the binary, local file cannot be flocked.
		locked, err := localFileLock.TryLock()
		if err != nil {
			return "", err
		}

		if locked {
			// If local file can be locked, it means the previous download was
			// killed in the middle. Delete local file and re-download.
			log.Printf("previous download failed in the middle, deleting and re-downloading")
			if err := os.Remove(localFile); err != nil {
				log.Printf("failed to remove partial download %s: %v", localFile, err)
				return "", err
			}
			break
		}

		log.Printf("waiting for download of %s", localFile)
		time.Sleep(time.Millisecond * 10)
	}

	output, err := os.OpenFile(localFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, writingFileMode)
	if err != nil {
		return "", fmt.Errorf("error creating %s: %w", localFile, err)
	}

	// Assign a flock to the local file.
	// If the downloading process is killed in the middle,
	// the lock will be automatically dropped.
	localFileLock := flock.New(localFile)

	if _, err := localFileLock.TryLock(); err != nil {
		return "", err
	}

	defer func() { _ = localFileLock.Unlock() }()

	if tc.IsTest && tc.StopDownloadInMiddle {
		log.Printf("download process killed")
		output.Close()
		return "", errStoppedInMiddle
	}

	var downloadMethod func(*http.Response, *os.File, string) error

	if nonStable {
		downloadMethod = downloadBinaryFromResponse
	} else {
		if goos == "windows" {
			downloadMethod = downloadBinaryFromZip
		} else {
			downloadMethod = downloadBinaryFromTar
		}
	}
	log.Printf("saving %s to %s, this may take some time", response.Request.URL, localFile)
	if err := downloadMethod(response, output, localFile); err != nil {
		if !errors.Is(err, errStoppedInMiddle) {
			if err := os.Remove(localFile); err != nil {
				log.Printf("failed to remove %s: %s", localFile, err)
			}
		}
		return "", err
	}

	if err := localFileLock.Unlock(); err != nil {
		return "", err
	}

	if err := output.Close(); err != nil {
		return "", err
	}

	return localFile, nil
}

// GetDownloadFilename returns the local filename of the downloaded CRDB binary file.
func GetDownloadFilename(desiredVersion string) (string, error) {
	return GetDownloadFilenameWithPlatform(desiredVersion, runtime.GOOS)
}

// GetDownloadFilenameWithPlatform returns the local filename of the downloaded CRDB binary file
// for the specified platform.
func GetDownloadFilenameWithPlatform(desiredVersion, goos string) (string, error) {
	filename := fmt.Sprintf("cockroach-%s", desiredVersion)
	if goos == "windows" {
		filename += ".exe"
	}
	return filename, nil
}

// Release contains the information we extract from the YAML file in
// `releaseDataURL`.
type Release struct {
	Name      string `yaml:"release_name"`
	Withdrawn bool   `yaml:"withdrawn"`
	CloudOnly bool   `yaml:"cloud_only"`
}

// getLatestStableVersionInfo returns the latest stable CRDB's download URL,
// and the formatted corresponding version number. The download URL is based
// on the runtime OS.
// Note that it may return a withdrawn version, but the risk is low for local tests here.
func getLatestStableVersionInfo() (string, string, error) {
	resp, err := http.Get(releaseDataURL)
	if err != nil {
		return "", "", fmt.Errorf("could not download release data: %w", err)
	}
	defer resp.Body.Close()

	var blob bytes.Buffer
	if _, err := io.Copy(&blob, resp.Body); err != nil {
		return "", "", fmt.Errorf("error reading response body: %w", err)
	}

	var data []Release
	if err := yaml.Unmarshal(blob.Bytes(), &data); err != nil { //nolint:yaml
		return "", "", fmt.Errorf("failed to YAML parse release data: %w", err)
	}

	latestStableVersion := version.MustParse("v0.0.0")

	for _, r := range data {
		// We ignore versions that cannot be parsed; this should
		// correspond to really old beta releases.
		v, err := version.Parse(r.Name)
		if err != nil {
			continue
		}

		// Skip cloud-only releases, since they cannot be downloaded from
		// binaries.cockroachdb.com.
		if r.CloudOnly {
			continue
		}

		// Ignore any withdrawn releases, since they are known to be broken.
		if r.Withdrawn {
			continue
		}

		// Ignore alphas, betas, and RCs.
		if v.PreRelease() != "" {
			continue
		}

		if v.Compare(latestStableVersion) > 0 {
			latestStableVersion = v
		}
	}

	downloadUrl := getDownloadUrlForVersionWithPlatform(latestStableVersion.String(), runtime.GOOS, runtime.GOARCH)

	latestStableVerFormatted := strings.ReplaceAll(latestStableVersion.String(), ".", "-")
	return downloadUrl, latestStableVerFormatted, nil
}

func getDownloadUrlForVersionWithPlatform(version, goos, goarch string) string {
	switch goos {
	case "linux":
		return fmt.Sprintf(linuxUrlpat, version, goarch)
	case "darwin":
		switch goarch {
		case "arm64":
			return fmt.Sprintf(macUrlpat, version, "11.0", goarch)
		case "amd64":
			return fmt.Sprintf(macUrlpat, version, "10.9", goarch)
		}
	case "windows":
		return fmt.Sprintf(winUrlpat, version)
	}

	panic(fmt.Errorf("unsupported platform/architecture combination: %s-%s", goos, goarch))
}

// downloadBinaryFromResponse copies the http response's body directly into a local binary.
func downloadBinaryFromResponse(response *http.Response, output *os.File, filePath string) error {
	if _, err := io.Copy(output, response.Body); err != nil {
		return fmt.Errorf("problem saving %s to %s: %w", response.Request.URL, filePath, err)
	}

	// Download was successful, add the rw bits.
	if err := output.Chmod(finishedFileMode); err != nil {
		return err
	}

	return nil
}

// downloadBinaryFromTar writes the binary compressed in a tar from a http response
// to a local file.
// It is created because the download url from the release page only provides the tar.gz/zip
// for a pre-compiled binary.
func downloadBinaryFromTar(response *http.Response, output *os.File, filePath string) error {
	// Unzip the tar file from the response's body.
	gzf, err := gzip.NewReader(response.Body)
	if err != nil {
		return fmt.Errorf("cannot read tar from response body: %w", err)
	}
	// Read the files from the tar.
	tarReader := tar.NewReader(gzf)
	for {
		header, err := tarReader.Next()

		// No more file from tar to read.
		if err == io.EOF {
			return fmt.Errorf("cannot find the binary from tar")
		}

		if err != nil {
			return fmt.Errorf("cannot untar: %w", err)
		}

		// Only copy the cockroach binary.
		// The header.Name is of the form "zip_name/file_name".
		// We extract the file name.
		splitHeaderName := strings.Split(header.Name, "/")
		fileName := splitHeaderName[len(splitHeaderName)-1]
		if fileName == "cockroach" {
			// Copy the binary to desired path.
			if _, err := io.Copy(output, tarReader); err != nil {
				return fmt.Errorf(
					"problem saving %s to %s: %w",
					response.Request.URL, filePath,
					err,
				)
			}
			if err := output.Chmod(finishedFileMode); err != nil {
				return err
			}
			return nil
		}

	}
	// Unreachable, but left present for safety in case later changes make this branch reachable again.
	return fmt.Errorf("could not find cockroach binary in archive")
}

// downloadBinaryFromZip writes the binary compressed in a zip from a http response
// to a local file.
// It is created because the download url from the release page only provides the tar.gz/zip
// for a pre-compiled binary.
func downloadBinaryFromZip(response *http.Response, output *os.File, filePath string) error {
	body, err := io.ReadAll(response.Body)
	if err != nil {
		return fmt.Errorf("cannot read zip from response body: %w", err)
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		log.Fatal(err)
	}

	findFile := false
	// Read all the files from zip archive.
	for _, zipFile := range zipReader.File {
		splitHeaderName := strings.Split(zipFile.Name, "/")
		fileName := splitHeaderName[len(splitHeaderName)-1]
		fmt.Printf("filename=%s", fileName)
		if fileName == "cockroach" {
			findFile = true
			if err := readZipFile(zipFile, output); err != nil {
				return fmt.Errorf("problem saving %s to %s: %w",
					response.Request.URL,
					filePath,
					err)
			}
			if err := output.Chmod(finishedFileMode); err != nil {
				return err
			}
		}
	}
	if !findFile {
		return fmt.Errorf("cannot find the binary from zip")
	}

	return nil
}

func readZipFile(zf *zip.File, target *os.File) error {
	f, err := zf.Open()
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = io.Copy(target, f); err != nil {
		return err
	}
	return nil
}

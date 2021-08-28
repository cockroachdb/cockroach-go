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
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"time"

	"github.com/gofrs/flock"
)

const (
	latestSuffix     = "LATEST"
	finishedFileMode = 0555
	writingFileMode  = 0600 // Allow reads so that another process can check if there's a flock.
)

func downloadFile(response *http.Response, filePath string, tc *TestConfig) error {
	output, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, writingFileMode)
	if err != nil {
		return fmt.Errorf("error creating %s: %s", filePath, err)
	}
	defer func() { _ = output.Close() }()

	log.Printf("saving %s to %s, this may take some time", response.Request.URL, filePath)

	// Assign a flock to the local file.
	// If the downloading process is killed in the middle,
	// the lock will be automatically dropped.
	localFileLock := flock.New(filePath)

	if _, err := localFileLock.TryLock(); err != nil {
		return err
	}

	defer func() { _ = localFileLock.Unlock() }()

	if tc.IsTest && tc.StopDownloadInMiddle {
		log.Printf("download process killed")
		output.Close()
		return errStoppedInMiddle
	}

	if _, err := io.Copy(output, response.Body); err != nil {
		return fmt.Errorf("problem saving %s to %s: %s", response.Request.URL, filePath, err)
	}

	// Download was successful, add the rw bits.
	if err := output.Chmod(finishedFileMode); err != nil {
		return err
	}

	if err := localFileLock.Unlock(); err != nil {
		return err
	}

	// We explicitly close here to ensure the error is checked; the deferred
	// close above will likely error in this case, but that's harmless.
	return output.Close()
}

var muslRE = regexp.MustCompile(`(?i)\bmusl\b`)

// GetDownloadResponse return the http response of a CRDB download.
// It creates the url for downloading a CRDB binary for current runtime OS,
// makes a request to this url, and return the response.
func GetDownloadResponse() (*http.Response, error) {
	goos := runtime.GOOS
	if goos == "linux" {
		goos += func() string {
			// Detect which C library is present on the system. See
			// https://unix.stackexchange.com/a/120381.
			cmd := exec.Command("ldd", "--version")
			out, err := cmd.Output()
			if err != nil {
				log.Printf("%s: out=%q err=%s", cmd.Args, out, err)
			} else if muslRE.Match(out) {
				return "-musl"
			}
			return "-gnu"
		}()
	}
	binaryName := fmt.Sprintf("cockroach.%s-%s", goos, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	url := &url.URL{
		Scheme: "https",
		Host:   "edge-binaries.cockroachdb.com",
		Path:   path.Join("cockroach", fmt.Sprintf("%s.%s", binaryName, latestSuffix)),
	}
	log.Printf("GET %s", url)
	response, err := http.Get(url.String())
	if err != nil {
		return nil, err
	}

	if response.StatusCode != 200 {
		return nil, fmt.Errorf("error downloading %s: %d (%s)", url,
			response.StatusCode, response.Status)
	}
	return response, nil
}

func GetDownloadFilename(response *http.Response) (string, error) {
	const contentDisposition = "Content-Disposition"
	_, disposition, err := mime.ParseMediaType(response.Header.Get(contentDisposition))
	if err != nil {
		return "", fmt.Errorf("error parsing %s headers %s: %s", contentDisposition, response.Header, err)
	}

	filename, ok := disposition["filename"]
	if !ok {
		return "", fmt.Errorf("content disposition header %s did not contain filename", disposition)
	}
	return filename, nil
}

func downloadLatestBinary(tc *TestConfig) (string, error) {
	response, err := GetDownloadResponse()
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()

	filename, err := GetDownloadFilename(response)
	if err != nil {
		return "", err
	}
	localFile := filepath.Join(os.TempDir(), filename)
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

	err = downloadFile(response, localFile, tc)
	if err != nil {
		if !errors.Is(err, errStoppedInMiddle) {
			if err := os.Remove(localFile); err != nil {
				log.Printf("failed to remove %s: %s", localFile, err)
			}
		}
		return "", err
	}

	return localFile, nil
}

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
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
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
	"strings"
	"time"
)

const (
	latestSuffix     = "LATEST"
	finishedFileMode = 0555
)

// Regex-es to parse the release page to get the url of the latest stable version of CRDB.
const (
	releaseTableRegex         = "<table class=\"release-table\">(?s:.+)<\\/table>"
	latestVersionSectionRegex = "<tr class=\"latest\">(?s:.+?)<\\/tr>"
	osSectionRegexFmt         = "<section class=\"filter-content\" data-scope=\"%s\">(?s:.+?)<\\/section>"
	sourceUrlRegex            = "href=\"(.*?)\">Precompiled 64-bit Binary"
)

// Regex-es on a CRDB download url for a version number.
const versionNumRegex = "v(.+?).[A-z]"

// Url of the CRDB release page.
const releasePageUrl = "https://www.cockroachlabs.com/docs/releases/index.html?filters=source"

// Section labels for linux/macos/windows systems.
// For each release, CRDB provides distinct urls for these three operating systems.
// The html elements containing the urls are labelled with the following tokens
// in the release page.
const (
	linuxSectionLabel = "linux"
	macSectionLabel   = "mac"
	winSectionLabel   = "windows"
)

// downloadBinaryFromResponse copies the http reponse's body directly into a local binary.
func downloadBinaryFromResponse(response *http.Response, filePath string) error {
	output, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0200)
	if err != nil {
		return fmt.Errorf("error creating %s: %s", filePath, err)
	}
	defer func() { _ = output.Close() }()

	log.Printf("saving %s to %s, this may take some time", response.Request.URL, filePath)

	if _, err := io.Copy(output, response.Body); err != nil {
		return fmt.Errorf("problem saving %s to %s: %s", response.Request.URL, filePath, err)
	}

	// Download was successful, add the rw bits.
	if err := output.Chmod(finishedFileMode); err != nil {
		return err
	}

	// We explicitly close here to ensure the error is checked; the deferred
	// close above will likely error in this case, but that's harmless.
	return output.Close()
}

var muslRE = regexp.MustCompile(`(?i)\bmusl\b`)

// downloadLatestBinary save the latest version of CRDB into a local binary file,
// and return the path for this local binary.
// To download the latest STABLE version of CRDB, set `nonStable` to false.
// To download the latest and beta version of CRDB, set `nonStable` to true.
func downloadLatestBinary(nonStable bool) (string, error) {
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

	var dbUrl *url.URL

	// For the latest (beta) CRDB, we use the `edge-binaries.cockroachdb.com` host.
	if nonStable {
		dbUrl = &url.URL{
			Scheme: "https",
			Host:   "edge-binaries.cockroachdb.com",
			Path:   path.Join("cockroach", fmt.Sprintf("%s.%s", binaryName, latestSuffix)),
		}
	} else {
		// For the latest stable CRDB, we use the url provided in the CRDB release page.
		dbUrl = getLatestStableDbUrl()
	}

	log.Printf("GET %s", dbUrl)
	response, err := http.Get(dbUrl.String())
	if err != nil {
		return "", err
	}
	defer func() { _ = response.Body.Close() }()

	if response.StatusCode != 200 {
		return "", fmt.Errorf("error downloading %s: %d (%s)", dbUrl, response.StatusCode, response.Status)
	}

	var filename string

	if nonStable {
		const contentDisposition = "Content-Disposition"
		_, disposition, err := mime.ParseMediaType(response.Header.Get(contentDisposition))
		if err != nil {
			return "", fmt.Errorf("error parsing %s headers %s: %s", contentDisposition, response.Header, err)
		}

		tmpFilename, ok := disposition["filename"]
		if !ok {
			return "", fmt.Errorf("content disposition header %s did not contain filename", disposition)
		}
		filename = tmpFilename
	} else {
		versionNum := getVersionFromUrl(dbUrl)
		filename = fmt.Sprintf("cockroach-%s", versionNum)
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
		log.Printf("waiting for download of %s", localFile)
		time.Sleep(time.Millisecond * 10)
	}

	if nonStable {
		if err := downloadBinaryFromResponse(response, localFile); err != nil {
			if err := os.Remove(localFile); err != nil {
				log.Printf("failed to remove %s: %s", localFile, err)
			}
			return "", err
		}
	} else {

		if err := downloadBinaryFromTar(response, localFile); err != nil {
			if err := os.Remove(localFile); err != nil {
				log.Printf("failed to remove %s: %s", localFile, err)
			}
			return "", err
		}
	}

	return localFile, nil
}

// downloadBinaryFromTar writes the binary compressed in a tar from a http reponse
// to a local file.
// It is created because the download url from the release page only provides the tar.gz/zip
// for a pre-compiled binary.
func downloadBinaryFromTar(response *http.Response, filePath string) error {
	output, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0200)

	// Unzip the tar/zip file from the response'body.
	gzf, err := gzip.NewReader(response.Body)
	if err != nil {
		fmt.Errorf("Cannot read tar from response body: %s", err)
	}
	log.Printf("saving %s to %s, this may take some time", response.Request.URL, filePath)

	// Read the files from the tar/zip.
	tarReader := tar.NewReader(gzf)
	for {
		header, err := tarReader.Next()

		// No more file from tar to read.
		if err == io.EOF {
			fmt.Errorf("cannot find the binary")
		}

		if err != nil {
			fmt.Errorf("cannot untar: %s", err)
		}

		// Only copy the cockroach binary.
		// The header.Name is of the form "zip_name/file_name".
		// We extract the file name.
		splitHeaderName := strings.Split(header.Name, "/")
		fileName := splitHeaderName[len(splitHeaderName)-1]
		if fileName == "cockroach" {
			// Copy the binary to desired path.
			if _, err := io.Copy(output, tarReader); err != nil {
				fmt.Errorf("problem saving %s to %s: %s", response.Request.URL, filePath, err)
			}
			if err := output.Chmod(finishedFileMode); err != nil {
				return err
			}
			return nil
		}

	}
	return output.Close()
}

// getVersionFromUrl parse a download url for a CRDB
// to get its version number.
// E.g.
// https://binaries.cockroachdb.com/cockroach-v21.1.7.darwin-10.9-amd64.tgz
// -> 21.1.7
func getVersionFromUrl(curURl *url.URL) string {
	path := curURl.Path
	reVersion := regexp.MustCompile(versionNumRegex)
	versionNums := reVersion.FindAllStringSubmatch(path, -1)
	if len(versionNums) == 0 {
		log.Fatal("Cannot find version numbers")
	}
	versionNum := versionNums[0][1]
	return versionNum
}

// getLatestStableDbUrl return the url of the latest stable CRDB for current runtime OS.
// It parses the release page html to get the url.
func getLatestStableDbUrl() *url.URL {
	response, err := http.Get(releasePageUrl)
	if err != nil {
		log.Fatal(err)
	}
	defer response.Body.Close()

	// Read response data in to memory
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Fatal("Error reading HTTP body. ", err)
	}
	// The html structure of the release page:
	//<table class="release-table">
	//...
	//	<tr class="latest">
	//	...
	//		<section ... data-scope="linux">
	//			< ... href="https://binaries.cockroachdb.com/...linux...tgz" />
	//		</section>
	//
	//		<section ... data-scope="mac">
	//			< ... href="https://binaries.cockroachdb.com/...darwin...tgz" />
	//		</section>
	//
	//		<section ... data-scope="windows">
	//			< ... href="https://binaries.cockroachdb.com/...windows...zip" />
	//		</section>
	//	...
	//	</tr>
	//...
	//</table>

	// Create a regular expression to find the release table.
	reReleaseTable := regexp.MustCompile(releaseTableRegex)
	releaseTables := reReleaseTable.FindAllString(string(body), -1)
	if len(releaseTables) == 0 {
		log.Fatal("Cannot find release table")
	}

	// Create a regular expression to find the section for latest release.
	reLatestSection := regexp.MustCompile(latestVersionSectionRegex)
	latestSections := reLatestSection.FindAllString(releaseTables[0], -1)
	if len(latestSections) == 0 {
		log.Fatal("Cannot find latest section")
	}

	// Create a regular expression to find the content section
	// for current runtime os.
	var contentSectionRegex string
	switch runtime.GOOS {
	case "linux":
		contentSectionRegex = fmt.Sprintf(osSectionRegexFmt, linuxSectionLabel)
	case "darwin":
		contentSectionRegex = fmt.Sprintf(osSectionRegexFmt, macSectionLabel)
	case "windows":
		contentSectionRegex = fmt.Sprintf(osSectionRegexFmt, winSectionLabel)
	}
	recontentSection := regexp.MustCompile(contentSectionRegex)
	contentForOsSections := recontentSection.FindAllString(latestSections[0], -1)
	if len(contentForOsSections) == 0 {
		log.Fatal("Cannot find the content for current OS")
	}

	// Create a regular expression to extract the url from the content section.
	reSourceUrl := regexp.MustCompile(sourceUrlRegex)
	latestStableRawUrls := reSourceUrl.FindAllStringSubmatch(contentForOsSections[0], -1)[0]
	if len(latestStableRawUrls) < 2 {
		log.Fatal("Cannot find url for latest stable crdb")
	}
	latestStableUrl, err := url.Parse(latestStableRawUrls[1])
	if err != nil {
		log.Fatal("Cannot parse the raw url of latest stable crdb")
	}

	return latestStableUrl
}

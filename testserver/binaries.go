package testserver

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"time"
)

const (
	latestSuffix     = "LATEST"
	finishedFileMode = 0555
)

var client = http.Client{
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

func downloadFile(url, filePath string) error {
	output, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0200)
	if err != nil {
		return fmt.Errorf("error creating %s: %s", filePath, err)
	}
	defer output.Close()

	log.Printf("downloading %s to %s, this may take some time", url, filePath)

	response, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("error downloading %s: %s", url, err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return fmt.Errorf("error downloading %s: %d (%s)", url, response.StatusCode, response.Status)
	}

	if _, err := io.Copy(output, response.Body); err != nil {
		return fmt.Errorf("problem downloading %s to %s: %s", url, filePath, err)
	}

	// Download was successful, add the rw bits.
	return os.Chmod(filePath, finishedFileMode)
}

func downloadLatestBinary() (string, error) {
	binaryName := fmt.Sprintf("cockroach.%s-%s", runtime.GOOS, runtime.GOARCH)
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	resp, err := client.Head((&url.URL{
		Scheme: "https",
		Host:   "edge-binaries.cockroachdb.com",
		Path:   path.Join("cockroach", fmt.Sprintf("%s.%s", binaryName, latestSuffix)),
	}).String())
	if err != nil {
		return "", err
	}
	u, err := resp.Location()
	if err != nil {
		return "", err
	}

	localFile := filepath.Join(os.TempDir(), path.Base(u.Path))
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
		time.Sleep(time.Millisecond * 10)
	}

	if err := downloadFile(u.String(), localFile); err != nil {
		_ = os.Remove(localFile)
		return "", err
	}

	return localFile, nil
}

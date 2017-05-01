package testserver

import (
	"fmt"
	"io"
	"log"
	"mime"
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

func downloadFile(response *http.Response, filePath string) error {
	output, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0200)
	if err != nil {
		return fmt.Errorf("error creating %s: %s", filePath, err)
	}
	defer output.Close()

	log.Printf("saving %s to %s, this may take some time", response.Request.URL, filePath)

	if _, err := io.Copy(output, response.Body); err != nil {
		return fmt.Errorf("problem saving %s to %s: %s", response.Request.URL, filePath, err)
	}

	// Download was successful, add the rw bits.
	return os.Chmod(filePath, finishedFileMode)
}

func downloadLatestBinary() (string, error) {
	binaryName := fmt.Sprintf("cockroach.%s-%s", runtime.GOOS, runtime.GOARCH)
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
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return "", fmt.Errorf("error downloading %s: %d (%s)", url, response.StatusCode, response.Status)
	}

	const contentDisposition = "Content-Disposition"
	_, disposition, err := mime.ParseMediaType(response.Header.Get(contentDisposition))
	if err != nil {
		return "", fmt.Errorf("error parsing %s headers %s: %s", contentDisposition, response.Header, err)
	}

	filename, ok := disposition["filename"]
	if !ok {
		return "", fmt.Errorf("content disposition header %s did not contain filename", disposition)
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
		time.Sleep(time.Millisecond * 10)
	}

	if err := downloadFile(response, localFile); err != nil {
		_ = os.Remove(localFile)
		return "", err
	}

	return localFile, nil
}

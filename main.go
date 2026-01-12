package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"sync"

	"github.com/grafov/m3u8"
)

func main() {
	urlPtr := flag.String("url", "", "The URL of the Master M3U8 playlist")
	outPtr := flag.String("out", "output.mp4", "The output filename")
	langPtr := flag.String("lang", "", "The 3-letter language code for audio (e.g. eng, ger, jpn)")
	flag.Parse()

	if *urlPtr == "" {
		fmt.Println("Please provide a URL using -url")
		return
	}

	// Parse the Master URL strictly as a URL object
	masterURL, err := url.Parse(*urlPtr)
	if err != nil {
		panic(fmt.Sprintf("Invalid Master URL: %v", err))
	}

	fmt.Println("1. Fetching Master Playlist...")

	// 1. Get the highest resolution stream URL
	// We pass the full masterURL object so we can resolve relative paths correctly
	bestVariantPath, err := getBestVariant(masterURL.String())
	if err != nil {
		fmt.Printf("Error finding variant: %v\n", err)
		return
	}

	// Resolve the variant URL relative to the master URL
	fullVariantURL, err := resolveURL(masterURL, bestVariantPath)
	if err != nil {
		panic(err)
	}
	fmt.Printf("   Selected Best Variant: %s\n", fullVariantURL)

	// 2. Parse the specific Media Playlist to get segments
	segments, err := getSegmentList(fullVariantURL)
	if err != nil {
		panic(err)
	}

	// 3. Download Segments
	fmt.Printf("2. Downloading %d segments...\n", len(segments))
	tempDir, err := os.MkdirTemp("", "hls_dl")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	// We need the variant URL object to resolve segment paths
	variantURLObj, _ := url.Parse(fullVariantURL)
	segmentFiles, err := downloadSegments(segments, variantURLObj, tempDir)
	if err != nil {
		panic(err)
	}

	// 4. Merge Segments
	fmt.Println("3. Merging segments...")
	rawTsFile := filepath.Join(tempDir, "combined.ts")
	err = mergeSegments(segmentFiles, rawTsFile)
	if err != nil {
		panic(err)
	}

	// 5. Convert to MP4
	fmt.Println("4. Remuxing to MP4 (FFmpeg)...")
	err = convertToMP4(rawTsFile, *outPtr, *langPtr)

	if err != nil {
		fmt.Printf("Error running ffmpeg: %v\n", err)

		// Create a specific backup name based on the intended output
		// e.g., if out="myvideo.mp4", backup="myvideo.mp4-combined.ts"
		backupFile := *outPtr + "-combined.ts"

		fmt.Printf("The raw TS file has been saved as '%s' as a backup.\n", backupFile)

		// Move the file from temp dir to current execution dir (or output path)
		renameErr := os.Rename(rawTsFile, backupFile)
		if renameErr != nil {
			fmt.Printf("Warning: Could not save backup file (permission or cross-drive issue): %v\n", renameErr)
		}
	} else {
		fmt.Printf("Done! Saved to %s\n", *outPtr)
	}
}

func getBestVariant(masterURL string) (string, error) {
	resp, err := http.Get(masterURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	p, listType, err := m3u8.DecodeFrom(bufio.NewReader(resp.Body), true)
	if err != nil {
		return "", err
	}

	// If it's already a Media playlist (simple m3u8), just return the original URL
	if listType == m3u8.MEDIA {
		return masterURL, nil
	}

	masterPL := p.(*m3u8.MasterPlaylist)

	// Sort variants by Bandwidth descending
	sort.Slice(masterPL.Variants, func(i, j int) bool {
		return masterPL.Variants[i].Bandwidth > masterPL.Variants[j].Bandwidth
	})

	if len(masterPL.Variants) == 0 {
		return "", fmt.Errorf("no variants found")
	}

	return masterPL.Variants[0].URI, nil
}

func getSegmentList(mediaPlaylistURL string) ([]string, error) {
	resp, err := http.Get(mediaPlaylistURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	p, listType, err := m3u8.DecodeFrom(bufio.NewReader(resp.Body), true)
	if err != nil {
		return nil, err
	}

	if listType != m3u8.MEDIA {
		return nil, fmt.Errorf("URL provided is not a Media Playlist")
	}

	mediaPL := p.(*m3u8.MediaPlaylist)
	var segments []string

	for _, seg := range mediaPL.Segments {
		if seg != nil {
			segments = append(segments, seg.URI)
		}
	}
	return segments, nil
}

func downloadSegments(segmentURLs []string, baseURL *url.URL, destDir string) ([]string, error) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // Limit to 10 concurrent downloads
	var files []string
	var mu sync.Mutex
	var firstErr error

	files = make([]string, len(segmentURLs))

	for i, segmentPath := range segmentURLs {
		wg.Add(1)
		go func(idx int, u string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Robust URL resolution
			fullURL, err := resolveURL(baseURL, u)
			if err != nil {
				fmt.Printf("Error resolving URL: %v\n", err)
				return
			}

			filename := filepath.Join(destDir, fmt.Sprintf("segment_%04d.ts", idx))

			// Retry logic for unstable connections
			for retries := 0; retries < 3; retries++ {
				if err = downloadFile(fullURL, filename); err == nil {
					break
				}
			}

			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			files[idx] = filename
			mu.Unlock()

			if idx%10 == 0 {
				fmt.Print(".")
			}
		}(i, segmentPath)
	}
	wg.Wait()
	fmt.Println()

	return files, firstErr
}

func downloadFile(url string, filepath string) error {
	out, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer out.Close()

	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("server returned %s", resp.Status)
	}

	_, err = io.Copy(out, resp.Body)
	return err
}

func mergeSegments(files []string, outFile string) error {
	out, err := os.Create(outFile)
	if err != nil {
		return err
	}
	defer out.Close()

	for _, f := range files {
		// Skip if download failed for this segment
		if f == "" {
			continue
		}
		in, err := os.Open(f)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, in)
		in.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func convertToMP4(tsFile string, mp4File string, lang string) error {
	// Start with the base arguments
	args := []string{
		"-y",
		"-i", tsFile,
		"-c", "copy",
		"-bsf:a", "aac_adtstoasc",
	}

	// NEW: If a language is provided, add the metadata flag
	// -metadata:s:a:0 means: Set metadata for Stream (s), Audio (a), Index 0 (first track)
	if lang != "" {
		args = append(args, "-metadata:s:a:0", fmt.Sprintf("language=%s", lang))
	}

	// Append the output filename last
	args = append(args, mp4File)

	cmd := exec.Command("ffmpeg", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ffmpeg error: %s : %v", string(output), err)
	}
	return nil
}

// resolveURL uses net/url to correctly handle relative paths and query params
func resolveURL(base *url.URL, relative string) (string, error) {
	relURL, err := url.Parse(relative)
	if err != nil {
		return "", err
	}
	// This handles the logic of replacing the filename or appending correctly
	return base.ResolveReference(relURL).String(), nil
}

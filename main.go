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
	"strings"
	"sync"
	"sync/atomic"

	"github.com/grafov/m3u8"
)

func main() {
	urlPtr := flag.String("url", "", "The URL of the Master M3U8 playlist")
	outPtr := flag.String("out", "output.mp4", "The output filename")
	langPtr := flag.String("lang", "", "The 2 or 3-letter language code for audio (e.g., en, fr, de, cz)")
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

	fmt.Println("1. Fetching Master Playlist & Finding Streams...")

	bestVideoPath, bestAudioPath, err := getStreams(masterURL.String(), *langPtr)
	if err != nil {
		fmt.Printf("Error finding streams: %v\n", err)
		return
	}

	tempDir, err := os.MkdirTemp("", "hls_dl")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tempDir)

	fmt.Println("\n--- Processing Video Stream ---")
	videoFile, isVideoFMP4, err := processStream(masterURL, bestVideoPath, tempDir, "video")
	if err != nil {
		panic(fmt.Sprintf("Failed to process video: %v", err))
	}

	var audioFile string
	var isAudioFMP4 bool
	if bestAudioPath != "" {
		fmt.Println("\n--- Processing Audio Stream ---")
		audioFile, isAudioFMP4, err = processStream(masterURL, bestAudioPath, tempDir, "audio")
		if err != nil {
			panic(fmt.Sprintf("Failed to process audio: %v", err))
		}
	} else {
		fmt.Println("\nNo separate audio stream found. Assuming audio is muxed into the video.")
	}

	fmt.Println("\n4. Remuxing to MP4 (FFmpeg)...")

	// Convert the 2-letter code to a 3-letter code right here
	safeLangCode := getISO639_2(*langPtr)

	err = convertToMP4(videoFile, audioFile, *outPtr, safeLangCode, isVideoFMP4, isAudioFMP4)

	if err != nil {
		fmt.Printf("Error running ffmpeg: %v\n", err)
		fmt.Printf("Raw files are kept in: %s\n", tempDir)
		os.MkdirAll("failed_downloads", 0755)
		os.Rename(videoFile, filepath.Join("failed_downloads", "video_backup.raw"))
		if audioFile != "" {
			os.Rename(audioFile, filepath.Join("failed_downloads", "audio_backup.raw"))
		}
	} else {
		fmt.Printf("Done! Saved to %s\n", *outPtr)
	}
}

func processStream(masterURL *url.URL, streamPath string, tempDir string, prefix string) (string, bool, error) {
	fullStreamURL, err := resolveURL(masterURL, streamPath)
	if err != nil {
		return "", false, err
	}

	streamURLObj, _ := url.Parse(fullStreamURL)
	initURI, segments, err := getSegmentList(fullStreamURL)
	if err != nil {
		return "", false, err
	}

	isFMP4 := initURI != ""
	var allFilesToMerge []string

	if isFMP4 {
		fmt.Printf(" > Downloading Initialization Segment (%s fMP4)...\n", prefix)
		initFullURL, err := resolveURL(streamURLObj, initURI)
		if err != nil {
			return "", false, err
		}
		initFile := filepath.Join(tempDir, fmt.Sprintf("%s_init.mp4", prefix))
		if err = downloadFile(initFullURL, initFile); err != nil {
			return "", false, err
		}
		allFilesToMerge = append(allFilesToMerge, initFile)
	}

	fmt.Printf(" > Downloading %d %s segments...\n", len(segments), prefix)
	destDir := filepath.Join(tempDir, prefix)
	os.MkdirAll(destDir, 0755)

	segmentFiles, err := downloadSegments(segments, streamURLObj, destDir)
	if err != nil {
		return "", false, err
	}
	allFilesToMerge = append(allFilesToMerge, segmentFiles...)

	rawExt := "ts"
	if isFMP4 {
		rawExt = "m4s"
	}
	combinedFile := filepath.Join(tempDir, fmt.Sprintf("%s_combined.%s", prefix, rawExt))

	fmt.Printf(" > Merging %s segments...\n", prefix)
	err = mergeSegments(allFilesToMerge, combinedFile)
	return combinedFile, isFMP4, err
}

func getStreams(masterURL string, lang string) (videoURI string, audioURI string, err error) {
	resp, err := http.Get(masterURL)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()

	p, listType, err := m3u8.DecodeFrom(bufio.NewReader(resp.Body), true)
	if err != nil {
		return "", "", err
	}

	// If it's already a Media playlist (simple m3u8), just return the original URL
	if listType == m3u8.MEDIA {
		return masterURL, "", nil
	}

	masterPL := p.(*m3u8.MasterPlaylist)

	// Sort variants by Bandwidth descending
	sort.Slice(masterPL.Variants, func(i, j int) bool {
		return masterPL.Variants[i].Bandwidth > masterPL.Variants[j].Bandwidth
	})

	if len(masterPL.Variants) == 0 {
		return "", "", fmt.Errorf("no variants found")
	}

	bestVariant := masterPL.Variants[0]
	videoURI = bestVariant.URI
	audioGroupID := bestVariant.Audio

	if audioGroupID != "" {
		// FIX: We loop through the Alternatives attached specifically to this Variant
		for _, alt := range bestVariant.Alternatives {
			if alt.Type == "AUDIO" && alt.GroupId == audioGroupID {
				if lang != "" {
					if alt.Language == lang {
						audioURI = alt.URI
						return
					}
				} else if alt.Default {
					audioURI = alt.URI
				}
			}
		}

		// Fallback if the requested language wasn't found, or no default was set
		if audioURI == "" {
			for _, alt := range bestVariant.Alternatives {
				if alt.Type == "AUDIO" && alt.GroupId == audioGroupID {
					audioURI = alt.URI
					fmt.Printf("Warning: Requested language '%s' not found. Falling back to '%s'\n", lang, alt.Language)
					break
				}
			}
		}
	}

	return videoURI, audioURI, nil
}

func getSegmentList(mediaPlaylistURL string) (string, []string, error) {
	resp, err := http.Get(mediaPlaylistURL)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()

	p, listType, err := m3u8.DecodeFrom(bufio.NewReader(resp.Body), true)
	if err != nil {
		return "", nil, err
	}

	if listType != m3u8.MEDIA {
		return "", nil, fmt.Errorf("URL provided is not a Media Playlist")
	}

	mediaPL := p.(*m3u8.MediaPlaylist)

	var initSegment string
	if mediaPL.Map != nil && mediaPL.Map.URI != "" {
		initSegment = mediaPL.Map.URI
	}

	var segments []string
	for _, seg := range mediaPL.Segments {
		if seg != nil {
			segments = append(segments, seg.URI)
		}
	}
	return initSegment, segments, nil
}

func downloadSegments(segmentURLs []string, baseURL *url.URL, destDir string) ([]string, error) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, 10) // Limit to 10 concurrent downloads
	var files []string
	var mu sync.Mutex
	var firstErr error

	total := int32(len(segmentURLs))
	var completed int32
	files = make([]string, total)

	if total == 0 {
		return files, nil
	}

	// Helper function to draw the progress bar
	printProgress := func() {
		c := atomic.LoadInt32(&completed)
		percent := float64(c) / float64(total) * 100
		barWidth := 40
		filled := int(float64(barWidth) * float64(c) / float64(total))

		bar := strings.Repeat("=", filled)
		if filled < barWidth {
			bar += ">" + strings.Repeat(" ", barWidth-filled-1)
		}

		// Use the mutex to prevent jumbled terminal output from concurrent threads
		mu.Lock()
		fmt.Printf("\r   [%s] %3.0f%% (%d/%d)", bar, percent, c, total)
		mu.Unlock()
	}

	// Initialize the progress bar at 0%
	printProgress()

	for i, segmentPath := range segmentURLs {
		wg.Add(1)
		go func(idx int, u string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Robust URL resolution
			fullURL, err := resolveURL(baseURL, u)
			if err != nil {
				return
			}

			filename := filepath.Join(destDir, fmt.Sprintf("segment_%04d.tmp", idx))

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

			// Safely increment the completed counter and update the UI
			atomic.AddInt32(&completed, 1)
			printProgress()
		}(i, segmentPath)
	}
	wg.Wait()
	fmt.Println() // Print a final newline so the next console output doesn't overwrite the bar

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
	}
	return nil
}

// Maps the 2-letter stream codes to strict 3-letter ISO 639-2 codes for MP4 compliance
func getISO639_2(lang string) string {
	mapping := map[string]string{
		"en": "eng", // English
		"fr": "fra", // French
		"de": "deu", // German (FFmpeg prefers deu over ger)
		"it": "ita", // Italian
		"jp": "jpn", // Japanese (Stream uses 'jp', standard is 'ja')
		"cz": "ces", // Czech (Stream uses 'cz', standard is 'cs')
		"pl": "pol", // Polish
		"pt": "por", // Portuguese
	}

	if val, ok := mapping[lang]; ok {
		return val
	}
	// Fallback to what was passed if it's not in the map
	return lang
}

func convertToMP4(videoFile string, audioFile string, mp4File string, lang string, isVideoFMP4 bool, isAudioFMP4 bool) error {
	args := []string{"-y"}

	args = append(args, "-i", videoFile)

	if audioFile != "" {
		args = append(args, "-i", audioFile)
	}

	args = append(args, "-c", "copy")

	if audioFile != "" {
		args = append(args, "-map", "0:v:0", "-map", "1:a:0")
	}

	if !isVideoFMP4 || !isAudioFMP4 {
		args = append(args, "-bsf:a", "aac_adtstoasc")
	}

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

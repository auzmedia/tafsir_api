package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	defaultResourceID = 523
	defaultSlug       = "ar-tafsir-al-jalalayn"
	defaultSource     = "https://qul.tarteel.ai/resources/tafsir/523"
	fileMode          = 0o644
)

var errMissingTafsir = errors.New("missing tafsir text")

type SurahInfo struct {
	Surah int `json:"surah"`
	Ayah  int `json:"ayah"`
}

type Ayah struct {
	Text  string `json:"text"`
	Ayah  int    `json:"ayah"`
	Surah int    `json:"surah"`
}

type EmptyAyah struct {
	Surah int `json:"surah"`
	Ayah  int `json:"ayah"`
}

type Edition struct {
	AuthorName   string `json:"author_name"`
	ID           int    `json:"id"`
	LanguageName string `json:"language_name"`
	Name         string `json:"name"`
	Slug         string `json:"slug"`
	Source       string `json:"source"`
}

type job struct {
	Surah int
	Ayah  int
}

type result struct {
	Ayah  Ayah
	Empty EmptyAyah
}

func main() {
	var (
		ayahDataPath = flag.String("ayah-data", "data/ayah_data.json", "path to ayah count data")
		outputDir    = flag.String("output-dir", filepath.Join("tafsir", defaultSlug), "directory for generated tafsir files")
		workers      = flag.Int("workers", 8, "number of concurrent requests")
		delay        = flag.Duration("delay", 150*time.Millisecond, "delay before each request per worker")
	)
	flag.Parse()

	surahs, err := loadSurahs(*ayahDataPath)
	if err != nil {
		log.Fatal(err)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	ayahs, emptyAyahs, err := scrapeAll(client, surahs, *workers, *delay)
	if err != nil {
		log.Fatal(err)
	}

	if err := writeAyahs(*outputDir, ayahs, emptyAyahs); err != nil {
		log.Fatal(err)
	}
	if err := addEdition("data/editions.json"); err != nil {
		log.Fatal(err)
	}
	if err := addEdition("tafsir/editions.json"); err != nil {
		log.Fatal(err)
	}
}

func loadSurahs(path string) ([]SurahInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var surahs []SurahInfo
	if err := json.Unmarshal(data, &surahs); err != nil {
		return nil, err
	}
	return surahs, nil
}

func scrapeAll(client *http.Client, surahs []SurahInfo, workers int, delay time.Duration) ([]Ayah, []EmptyAyah, error) {
	if workers < 1 {
		workers = 1
	}

	jobs := make(chan job)
	results := make(chan result)
	errs := make(chan error, 1)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range jobs {
				time.Sleep(delay)

				ayah, err := scrapeAyah(client, item.Surah, item.Ayah)
				if err != nil {
					select {
					case errs <- err:
					default:
					}
					return
				}
				results <- ayah
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, surah := range surahs {
			for ayah := 1; ayah <= surah.Ayah; ayah++ {
				select {
				case err := <-errs:
					select {
					case errs <- err:
					default:
					}
					return
				case jobs <- job{Surah: surah.Surah, Ayah: ayah}:
				}
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var (
		ayahs      []Ayah
		emptyAyahs []EmptyAyah
		seen       int
	)
	for item := range results {
		seen++
		if item.Ayah.Text == "" {
			emptyAyahs = append(emptyAyahs, item.Empty)
			log.Printf("missing tafsir for %d:%d", item.Empty.Surah, item.Empty.Ayah)
		} else {
			ayahs = append(ayahs, item.Ayah)
		}
		if seen%100 == 0 {
			log.Printf("processed %d ayahs", seen)
		}
	}

	select {
	case err := <-errs:
		return nil, nil, err
	default:
	}

	expected := 0
	for _, surah := range surahs {
		expected += surah.Ayah
	}
	if seen != expected {
		return nil, nil, fmt.Errorf("processed %d ayahs, expected %d", seen, expected)
	}

	sort.Slice(ayahs, func(i, j int) bool {
		if ayahs[i].Surah == ayahs[j].Surah {
			return ayahs[i].Ayah < ayahs[j].Ayah
		}
		return ayahs[i].Surah < ayahs[j].Surah
	})
	sort.Slice(emptyAyahs, func(i, j int) bool {
		if emptyAyahs[i].Surah == emptyAyahs[j].Surah {
			return emptyAyahs[i].Ayah < emptyAyahs[j].Ayah
		}
		return emptyAyahs[i].Surah < emptyAyahs[j].Surah
	})

	return ayahs, emptyAyahs, nil
}

func scrapeAyah(client *http.Client, surah, ayah int) (result, error) {
	url := fmt.Sprintf("%s?ayah=%d%%3A%d", defaultSource, surah, ayah)
	var lastErr error

	for attempt := 1; attempt <= 3; attempt++ {
		text, err := fetchTafsirText(client, url)
		if err == nil {
			return result{Ayah: Ayah{Text: text, Ayah: ayah, Surah: surah}}, nil
		}
		if errors.Is(err, errMissingTafsir) {
			return result{Empty: EmptyAyah{Surah: surah, Ayah: ayah}}, nil
		}
		lastErr = err
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	return result{}, fmt.Errorf("scrape %d:%d: %w", surah, ayah, lastErr)
}

func fetchTafsirText(client *http.Client, url string) (string, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "tafsir-api-jalalayn-import/1.0")

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return "", err
	}

	node := doc.Find(".tafsir.arabic .ar").First()
	if node.Length() == 0 {
		if strings.Contains(doc.Text(), "Tafisr is not available") || strings.Contains(doc.Text(), "Tafsir is not available") {
			return "", errMissingTafsir
		}
		return "", errors.New("missing tafsir text")
	}

	text := strings.Join(strings.Fields(html.UnescapeString(node.Text())), " ")
	if text == "" {
		return "", errors.New("empty tafsir text")
	}
	return text, nil
}

func writeAyahs(outputDir string, ayahs []Ayah, emptyAyahs []EmptyAyah) error {
	bySurah := map[int][]Ayah{}
	for _, ayah := range ayahs {
		bySurah[ayah.Surah] = append(bySurah[ayah.Surah], ayah)

		dir := filepath.Join(outputDir, fmt.Sprintf("%d", ayah.Surah))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		if err := writeJSON(filepath.Join(dir, fmt.Sprintf("%d.json", ayah.Ayah)), ayah); err != nil {
			return err
		}
	}

	emptyBySurah := map[int][]EmptyAyah{}
	for _, emptyAyah := range emptyAyahs {
		emptyBySurah[emptyAyah.Surah] = append(emptyBySurah[emptyAyah.Surah], emptyAyah)
	}

	for surah, surahAyahs := range bySurah {
		sort.Slice(surahAyahs, func(i, j int) bool {
			return surahAyahs[i].Ayah < surahAyahs[j].Ayah
		})

		data := struct {
			Ayahs      []Ayah      `json:"ayahs"`
			EmptyAyahs []EmptyAyah `json:"empty_ayahs,omitempty"`
		}{
			Ayahs:      surahAyahs,
			EmptyAyahs: emptyBySurah[surah],
		}

		if err := writeJSON(filepath.Join(outputDir, fmt.Sprintf("%d.json", surah)), data); err != nil {
			return err
		}
	}

	for surah, surahEmptyAyahs := range emptyBySurah {
		dir := filepath.Join(outputDir, fmt.Sprintf("%d", surah))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		if err := writeJSON(filepath.Join(dir, "empty_ayahs.json"), surahEmptyAyahs); err != nil {
			return err
		}
		if _, ok := bySurah[surah]; !ok {
			data := struct {
				Ayahs      []Ayah      `json:"ayahs"`
				EmptyAyahs []EmptyAyah `json:"empty_ayahs,omitempty"`
			}{EmptyAyahs: surahEmptyAyahs}
			if err := writeJSON(filepath.Join(outputDir, fmt.Sprintf("%d.json", surah)), data); err != nil {
				return err
			}
		}
	}

	return nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, fileMode)
}

func addEdition(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var editions []Edition
	if err := json.Unmarshal(data, &editions); err != nil {
		return err
	}

	next := Edition{
		AuthorName:   "Jalal al-Din al-Mahalli and Jalal al-Din al-Suyuti",
		ID:           defaultResourceID,
		LanguageName: "arabic",
		Name:         "Tafsir al-Jalalayn",
		Slug:         defaultSlug,
		Source:       defaultSource,
	}

	for i, edition := range editions {
		if edition.Slug == defaultSlug {
			editions[i] = next
			return writeJSON(path, editions)
		}
	}

	editions = append(editions, next)
	return writeJSON(path, editions)
}

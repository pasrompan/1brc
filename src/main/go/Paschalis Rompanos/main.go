package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type WeatherData struct {
	min, max, sum, mean float64
	count               int
}

func main() {
	start := time.Now()

	const filename = "../../../../measurements_big.txt"
	//const filename = "../../../../data/weather_stations.csv"

	const maxGoroutines = 8 // You can adjust this value based on your system's capabilities

	parts, err := splitFile(filename, maxGoroutines)
	if err != nil {
		fmt.Println("Error splitting file:", err)
		return
	}

	resultsCh := make(chan map[string]*WeatherData, len(parts))
	for _, part := range parts {
		go processPart(filename, part.offset, part.size, resultsCh)
	}

	weatherStats := make(map[string]*WeatherData)
	for i := 0; i < len(parts); i++ {
		partialResults := <-resultsCh
		for city, data := range partialResults {
			if _, exists := weatherStats[city]; !exists {
				weatherStats[city] = data
			} else {
				weatherStats[city].min = min(weatherStats[city].min, data.min)
				weatherStats[city].max = max(weatherStats[city].max, data.max)
				weatherStats[city].sum += data.sum
				weatherStats[city].count += data.count
			}
		}
	}

	// Calculate mean for each city
	for _, data := range weatherStats {
		data.mean = data.sum / float64(data.count)
	}

	err = writeWeatherData("output.csv", weatherStats)
	if err != nil {
		fmt.Println("Error writing weather data:", err)
		return
	}

	elapsed := time.Since(start)
	minutes := int(elapsed.Minutes())
	seconds := int(elapsed.Seconds()) % 60
	milliseconds := int(elapsed.Milliseconds()) % 1000

	fmt.Printf("Time taken to read and process the file: %02d:%02d.%03d\n", minutes, seconds, milliseconds)
}

func processWeatherData(filePath string) (map[string]*WeatherData, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	weatherStats := make(map[string]*WeatherData)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		city, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			fmt.Println("Invalid line format:", line)
			continue
		}
		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			fmt.Println("Error parsing temperature:", err)
			continue
		}

		s := weatherStats[city]
		if s == nil {
			s = &WeatherData{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
		} else {
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++
		}
		weatherStats[city] = s
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return weatherStats, nil
}

func writeWeatherData(outputPath string, weatherStats map[string]*WeatherData) error {
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating output file: %w", err)
	}
	defer outputFile.Close()

	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	cities := make([]string, 0, len(weatherStats))
	for city := range weatherStats {
		cities = append(cities, city)
	}
	sort.Strings(cities)

	var result string
	for i, city := range cities {
		data := weatherStats[city]
		data.mean = math.Ceil(data.sum/float64(data.count)*10) / 10
		if i == 0 {
			result = fmt.Sprintf("{%s=%.1f/%.1f/%.1f", city, data.min, data.mean, data.max)
		} else {
			result += fmt.Sprintf(", %s=%.1f/%.1f/%.1f", city, data.min, data.mean, data.max)
		}
	}
	result += "}"
	writer.WriteString(result + "\n")

	return nil
}

type part struct {
	offset, size int64
}

func splitFile(inputPath string, numParts int) ([]part, error) {
	const maxLineLength = 100

	f, err := os.Open(inputPath)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := st.Size()
	splitSize := size / int64(numParts)

	buf := make([]byte, maxLineLength)

	parts := make([]part, 0, numParts)
	offset := int64(0)
	for offset < size {
		seekOffset := max(offset+splitSize-maxLineLength, 0)
		if seekOffset > size {
			break
		}
		_, err := f.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		n, _ := io.ReadFull(f, buf)
		chunk := buf[:n]
		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			return nil, fmt.Errorf("newline not found at offset %d", offset+splitSize-maxLineLength)
		}
		remaining := len(chunk) - newline - 1
		nextOffset := seekOffset + int64(len(chunk)) - int64(remaining)
		parts = append(parts, part{offset, nextOffset - offset})
		offset = nextOffset
	}
	return parts, nil
}

func processPart(inputPath string, fileOffset, fileSize int64, resultsCh chan map[string]*WeatherData) {
	file, err := os.Open(inputPath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		panic(err)
	}
	f := io.LimitedReader{R: file, N: fileSize}

	stationStats := make(map[string]*WeatherData)

	scanner := bufio.NewScanner(&f)
	for scanner.Scan() {
		line := scanner.Text()
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		temp, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			panic(err)
		}

		s, ok := stationStats[station]
		if !ok {
			s = &WeatherData{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
		} else {
			s.min = min(s.min, temp)
			s.max = max(s.max, temp)
			s.sum += temp
			s.count++
		}
		stationStats[station] = s
	}

	resultsCh <- stationStats
}

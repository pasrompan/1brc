package main

import (
	"bufio"
	"fmt"
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

	weatherStats, err := processWeatherData(filename)
	if err != nil {
		fmt.Println("Error processing weather data:", err)
		return
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

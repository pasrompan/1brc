package main

import (
	"bufio"
	"fmt"
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

	const filename = "../../../../src/test/resources/samples/measurements-10000-unique-keys.txt"
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
		parts := strings.Split(line, ";")
		if len(parts) != 2 {
			fmt.Println("Invalid line format:", line)
			continue
		}

		city := parts[0]
		temp, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			fmt.Println("Error parsing temperature:", err)
			continue
		}

		if _, exists := weatherStats[city]; !exists {
			weatherStats[city] = &WeatherData{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
		} else {
			data := weatherStats[city]
			if temp < data.min {
				data.min = temp
			}
			if temp > data.max {
				data.max = temp
			}
			data.sum += temp
			data.count++
		}
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
		data.mean = data.sum / float64(data.count)
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

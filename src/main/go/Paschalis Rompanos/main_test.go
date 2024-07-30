package main

import (
	"os"
	"strings"
	"testing"
)

func TestProcessWeatherData(t *testing.T) {
	// Prepare a sample CSV file content
	sampleData := `New York;10.5
Los Angeles;20.3
New York;15.6
Los Angeles;18.7`

	file, err := os.CreateTemp("", "weather_stations.csv")
	if err != nil {
		t.Fatalf("unable to create temp file: %v", err)
	}
	defer os.Remove(file.Name())

	if _, err := file.WriteString(sampleData); err != nil {
		t.Fatalf("unable to write to temp file: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("unable to close temp file: %v", err)
	}

	weatherStats, err := processWeatherData(file.Name())
	if err != nil {
		t.Fatalf("processWeatherData returned an error: %v", err)
	}

	// Check the results
	if len(weatherStats) != 2 {
		t.Fatalf("expected 2 cities, got %d", len(weatherStats))
	}

	newYorkData, ok := weatherStats["New York"]
	if !ok {
		t.Fatalf("expected New York data, but not found")
	}
	if newYorkData.min != 10.5 || newYorkData.max != 15.6 || newYorkData.sum != 26.1 || newYorkData.count != 2 {
		t.Errorf("New York data is incorrect: %+v", newYorkData)
	}

	laData, ok := weatherStats["Los Angeles"]
	if !ok {
		t.Fatalf("expected Los Angeles data, but not found")
	}
	if laData.min != 18.7 || laData.max != 20.3 || laData.sum != 39.0 || laData.count != 2 {
		t.Errorf("Los Angeles data is incorrect: %+v", laData)
	}
}

func TestWriteWeatherData(t *testing.T) {
	// Prepare sample weather stats
	weatherStats := map[string]*WeatherData{
		"New York": {
			min:   10.5,
			max:   15.6,
			sum:   26.1,
			count: 2,
			mean:  13.05,
		},
		"Los Angeles": {
			min:   18.7,
			max:   20.3,
			sum:   39.0,
			count: 2,
			mean:  19.5,
		},
	}

	outputFile, err := os.CreateTemp("", "output.csv")
	if err != nil {
		t.Fatalf("unable to create temp output file: %v", err)
	}
	defer os.Remove(outputFile.Name())

	err = writeWeatherData(outputFile.Name(), weatherStats)
	if err != nil {
		t.Fatalf("writeWeatherData returned an error: %v", err)
	}

	content, err := os.ReadFile(outputFile.Name())
	if err != nil {
		t.Fatalf("unable to read temp output file: %v", err)
	}

	expectedOutput := "Los Angeles=18.7/19.5/20.3\nNew York=10.5/13.1/15.6\n"
	if strings.TrimSpace(string(content)) != strings.TrimSpace(expectedOutput) {
		t.Errorf("output content is incorrect:\nExpected:\n%s\nGot:\n%s", expectedOutput, string(content))
	}
}

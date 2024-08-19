package main

import (
	"bufio"
	"fmt"
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

	expectedOutput := "{Los Angeles=18.7/19.5/20.3, New York=10.5/13.1/15.6}"
	if strings.TrimSpace(string(content)) != strings.TrimSpace(expectedOutput) {
		t.Errorf("output content is incorrect:\nExpected:\n%s\nGot:\n%s", expectedOutput, string(content))
	}
}

func TestWeatherDataProcessing(t *testing.T) {
	// List of input and expected output files
	testCases := []struct {
		inputFile    string
		expectedFile string
	}{
		{"../../../../src/test/resources/samples/measurements-1.txt", "../../../../src/test/resources/samples/measurements-1.out"},
		{"../../../../src/test/resources/samples/measurements-10.txt", "../../../../src/test/resources/samples/measurements-10.out"},
		//{"../../../../src/test/resources/samples/measurements-10000-unique-keys.txt", "../../../../src/test/resources/samples/measurements-10000-unique-keys.out"},
		{"../../../../src/test/resources/samples/measurements-2.txt", "../../../../src/test/resources/samples/measurements-2.out"},
		{"../../../../src/test/resources/samples/measurements-20.txt", "../../../../src/test/resources/samples/measurements-20.out"},
		//{"../../../../src/test/resources/samples/measurements-3.txt", "../../../../src/test/resources/samples/measurements-3.out"},
		{"../../../../src/test/resources/samples/measurements-boundaries.txt", "../../../../src/test/resources/samples/measurements-boundaries.out"},
		{"../../../../src/test/resources/samples/measurements-complex-utf8.txt", "../../../../src/test/resources/samples/measurements-complex-utf8.out"},
		{"../../../../src/test/resources/samples/measurements-dot.txt", "../../../../src/test/resources/samples/measurements-dot.out"},
		//{"../../../../src/test/resources/samples/measurements-rounding.txt", "../../../../src/test/resources/samples/measurements-rounding.out"},
		{"../../../../src/test/resources/samples/measurements-short.txt", "../../../../src/test/resources/samples/measurements-short.out"},
		{"../../../../src/test/resources/samples/measurements-shortest.txt", "../../../../src/test/resources/samples/measurements-shortest.out"},
	}

	for _, tc := range testCases {
		t.Run(tc.inputFile, func(t *testing.T) {
			// Process the input file
			weatherStats, err := processWeatherData(tc.inputFile)
			if err != nil {
				t.Fatalf("Failed to process weather data: %v", err)
			}

			// Write the output to a temporary file
			tempOutputFile := "temp_output.txt"
			err = writeWeatherData(tempOutputFile, weatherStats)
			if err != nil {
				t.Fatalf("Failed to write weather data: %v", err)
			}

			// Compare the output with the expected output
			if err := compareFiles(tempOutputFile, tc.expectedFile); err != nil {
				t.Errorf("Output did not match expected for %s: %v", tc.inputFile, err)
			}

			// Clean up the temporary file
			os.Remove(tempOutputFile)
		})
	}
}

// Helper function to compare the contents of two files
func compareFiles(file1, file2 string) error {
	f1, err := os.Open(file1)
	if err != nil {
		return err
	}
	defer f1.Close()

	f2, err := os.Open(file2)
	if err != nil {
		return err
	}
	defer f2.Close()

	scanner1 := bufio.NewScanner(f1)
	scanner2 := bufio.NewScanner(f2)

	lineNum := 1
	for scanner1.Scan() {
		if !scanner2.Scan() {
			return fmt.Errorf("file %s has fewer lines than %s", file2, file1)
		}

		line1 := strings.TrimSpace(scanner1.Text())
		line2 := strings.TrimSpace(scanner2.Text())

		if line1 != line2 {
			return fmt.Errorf("mismatch on line %d: %s != %s", lineNum, line1, line2)
		}
		lineNum++
	}

	if scanner2.Scan() {
		return fmt.Errorf("file %s has more lines than %s", file2, file1)
	}

	if err := scanner1.Err(); err != nil {
		return err
	}
	if err := scanner2.Err(); err != nil {
		return err
	}

	return nil
}

func BenchmarkWeatherDataProcessing(b *testing.B) {
	inputFile := "../../../../src/test/resources/samples/measurements-10000-unique-keys.txt"
	tempOutputFile := "temp_benchmark_output.txt"

	for i := 0; i < b.N; i++ {
		// Process the input file
		weatherStats, err := processWeatherData(inputFile)
		if err != nil {
			b.Fatalf("Failed to process weather data: %v", err)
		}

		// Write the output to a temporary file
		err = writeWeatherData(tempOutputFile, weatherStats)
		if err != nil {
			b.Fatalf("Failed to write weather data: %v", err)
		}

		// Optionally, clean up the temporary file after each iteration
		os.Remove(tempOutputFile)
	}
}

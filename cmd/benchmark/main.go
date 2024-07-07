package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"time"
)

var GREEN = "\033[0;32m"
var RESET = "\033[0m"

func main() {
	fmt.Println(GREEN, "--- Benchmark ---", RESET)

	runNew := flag.Bool("new", false, "Run new benchmark")
	flag.Parse()
	fmt.Println("new: ", *runNew)

	if *runNew {
		fmt.Println("Running new benchmark")

		// run the benchmark os command
		createdTime := time.Now().UTC().Format(time.RFC3339)
		outputFile := fmt.Sprintf("benchmarkResults/benchmark_%s.txt", createdTime)
		goTestBenchCommand := exec.Command("sh", "-c", fmt.Sprintf("go test -v -bench=. -cpu=1 -benchtime=1x -count=10 -benchmem ./... > %s 2>&1", outputFile))

		output, err := goTestBenchCommand.CombinedOutput()
		if err != nil {
			fmt.Println("Error running benchmark:", err)
			fmt.Printf("Command output:\n%s\n", string(output))
			return
		}

	}

	// read in the benchmark file names
	files, err := os.ReadDir("benchmarkResults/")
	if err != nil {
		fmt.Println("Error reading benchmark files: ", err)
		return
	}

	times := make([]time.Time, 0)
	for _, file := range files {
		// parse the file name and get the time
		// append to times
		fileNameComponents := strings.Split(strings.Replace(file.Name(), ".txt", "", 1), "_")
		if len(fileNameComponents) != 2 {
			fmt.Println("Ignoring file name: ", file.Name())
			continue
		}
		fileTime, err := time.Parse(time.RFC3339, fileNameComponents[1])
		if err != nil {
			fmt.Println("Error parsing file time: ", err)
			return
		}
		times = append(times, fileTime)
	}

	// sor the times and find the newest two items
	sort.Slice(times, func(i, j int) bool {
		return times[i].Before(times[j])
	})

	var newestFileName, secondNewestFileName string
	if len(times) == 0 {
		fmt.Println("No benchmark files found")
		return
	}
	newestFileName = fmt.Sprintf("benchmarkResults/benchmark_%s.txt", times[len(times)-1].Format(time.RFC3339))
	if len(times) > 1 {
		secondNewestFileName = fmt.Sprintf("benchmarkResults/benchmark_%s.txt", times[len(times)-2].Format(time.RFC3339))
	}

	fmt.Println("Newest file: ", newestFileName)
	fmt.Println("Second newest file: ", secondNewestFileName)

	benchstatCommand := exec.Command("sh", "-c", fmt.Sprintf("benchstat %s %s 2>&1", secondNewestFileName, newestFileName))
	output, err := benchstatCommand.CombinedOutput()
	if err != nil {
		fmt.Println("Error running benchmark comparision:", err)
		fmt.Printf("Command output:\n%s\n", string(output))
		return
	}

	fmt.Printf("%s\n", string(output))

}

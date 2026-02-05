package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var traceFile string

	// Check for help flag
	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		printHelp()
		return nil
	}

	// Check command-line arguments
	if len(os.Args) == 2 {
		// Explicit file provided
		traceFile = os.Args[1]
		fmt.Fprintf(os.Stderr, "Loading trace file: %s\n", traceFile)
	} else if len(os.Args) == 1 {
		// No argument - find latest trace*.xml in current directory
		latestFile, err := findLatestTraceFile()
		if err != nil {
			return fmt.Errorf("no trace file specified and failed to find latest trace*.xml: %w", err)
		}
		traceFile = latestFile
		fmt.Fprintf(os.Stderr, "Loading latest trace file: %s\n", traceFile)
	} else {
		return fmt.Errorf("usage: %s [trace-file.xml]", os.Args[0])
	}

	// Parse the trace file
	traceData, err := parseTraceFile(traceFile)
	if err != nil {
		return fmt.Errorf("failed to parse trace file: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Loaded %d events (%.2fs - %.2fs)\n",
		len(traceData.Events), traceData.MinTime, traceData.MaxTime)
	fmt.Fprintf(os.Stderr, "Found %d DB configurations\n", len(traceData.Configs))
	fmt.Fprintf(os.Stderr, "Found %d recovery states\n", len(traceData.RecoveryStates))

	// Start the TUI
	return runUI(traceData)
}

// printHelp prints usage and help information
func printHelp() {
	fmt.Fprintf(os.Stderr, `replay - Interactive TUI for replaying FDB simulation trace files

Usage:
  replay [trace-file.xml]
  replay -h | --help

Arguments:
  trace-file.xml    Path to FDB trace XML file (optional)
                    If not provided, uses the most recently modified trace*.xml
                    file in the current directory

Options:
  -h, --help        Show this help message

Examples:
  replay trace.xml              # Load specific trace file
  replay                        # Auto-load latest trace*.xml in current directory

Tip:
  Create an alias for quick access: alias r=replay

Interactive Commands:
  Press 'h' within the application to see all available navigation commands.
`)
}

// findLatestTraceFile finds the most recently modified trace*.xml file in the current directory
func findLatestTraceFile() (string, error) {
	// Get all trace*.xml files in current directory
	matches, err := filepath.Glob("trace*.xml")
	if err != nil {
		return "", err
	}

	if len(matches) == 0 {
		return "", fmt.Errorf("no trace*.xml files found in current directory")
	}

	// Find the most recently modified file
	var latestFile string
	var latestTime time.Time

	for _, file := range matches {
		info, err := os.Stat(file)
		if err != nil {
			continue // Skip files we can't stat
		}

		if latestFile == "" || info.ModTime().After(latestTime) {
			latestFile = file
			latestTime = info.ModTime()
		}
	}

	if latestFile == "" {
		return "", fmt.Errorf("no accessible trace*.xml files found")
	}

	return latestFile, nil
}

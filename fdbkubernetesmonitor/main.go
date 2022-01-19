// main.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2021 Apple Inc. and the FoundationDB project authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/zapr"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	inputDir                string
	fdbserverPath           string
	versionFilePath         string
	sharedBinaryDir         string
	monitorConfFile         string
	logPath                 string
	executionModeString     string
	outputDir               string
	copyFiles               []string
	copyBinaries            []string
	binaryOutputDirectory   string
	copyLibraries           []string
	copyPrimaryLibrary      string
	requiredCopyFiles       []string
	mainContainerVersion    string
	currentContainerVersion string
	additionalEnvFile       string
	processCount            int
)

type executionMode string

const (
	executionModeLauncher executionMode = "launcher"
	executionModeInit     executionMode = "init"
	executionModeSidecar  executionMode = "sidecar"
)

func main() {
	pflag.StringVar(&executionModeString, "mode", "launcher", "Execution mode. Valid options are launcher, sidecar, and init")
	pflag.StringVar(&fdbserverPath, "fdbserver-path", "/usr/bin/fdbserver", "Path to the fdbserver binary")
	pflag.StringVar(&inputDir, "input-dir", ".", "Directory containing input files")
	pflag.StringVar(&monitorConfFile, "input-monitor-conf", "config.json", "Name of the file in the input directory that contains the monitor configuration")
	pflag.StringVar(&logPath, "log-path", "", "Name of a file to send logs to. Logs will be sent to stdout in addition the file you pass in this argument. If this is blank, logs will only by sent to stdout")
	pflag.StringVar(&outputDir, "output-dir", ".", "Directory to copy files into")
	pflag.StringArrayVar(&copyFiles, "copy-file", nil, "A list of files to copy")
	pflag.StringArrayVar(&copyBinaries, "copy-binary", nil, "A list of binaries to copy from /usr/bin")
	pflag.StringVar(&versionFilePath, "version-file", "/var/fdb/version", "Path to a file containing the current FDB version")
	pflag.StringVar(&sharedBinaryDir, "shared-binary-dir", "/var/fdb/shared-binaries/bin", "A directory containing binaries that are copied from a sidecar process")
	pflag.StringVar(&binaryOutputDirectory, "binary-output-dir", "", "A subdirectory within $(output-dir)/bin to store binaries in. This defaults to the value in /var/fdb/version")
	pflag.StringArrayVar(&copyLibraries, "copy-library", nil, "A list of libraries to copy from /usr/lib/fdb/multiversion to $(output-dir)/lib/multiversion")
	pflag.StringVar(&copyPrimaryLibrary, "copy-primary-library", "", "A library to copy from /usr/lib/fdb/multiversion to $(output-dir)/lib. This file will be renamed to libfdb_c.so")
	pflag.StringArrayVar(&requiredCopyFiles, "require-not-empty", nil, "When copying this file, exit with an error if the file is empty")
	pflag.StringVar(&mainContainerVersion, "main-container-version", "", "For sidecar mode, this specifies the version of the main container. If this is equal to the current container version, no files will be copied")
	pflag.StringVar(&additionalEnvFile, "additional-env-file", "", "A file with additional environment variables to use when interpreting the monitor configuration")
	pflag.IntVar(&processCount, "process-count", 1, "The number of processes to start")
	pflag.Parse()

	zapConfig := zap.NewProductionConfig()
	if logPath != "" {
		zapConfig.OutputPaths = append(zapConfig.OutputPaths, logPath)
	}
	zapLogger, err := zapConfig.Build()
	if err != nil {
		panic(err)
	}

	versionBytes, err := os.ReadFile(versionFilePath)
	if err != nil {
		panic(err)
	}
	currentContainerVersion = strings.TrimSpace(string(versionBytes))

	logger := zapr.NewLogger(zapLogger)
	copyDetails, requiredCopies, err := getCopyDetails()
	if err != nil {
		logger.Error(err, "Error getting list of files to copy")
		os.Exit(1)
	}

	mode := executionMode(executionModeString)
	switch mode {
	case executionModeLauncher:
		customEnvironment, err := loadAdditionalEnvironment(logger)
		if err != nil {
			logger.Error(err, "Error loading additional environment")
			os.Exit(1)
		}
		StartMonitor(logger, fmt.Sprintf("%s/%s", inputDir, monitorConfFile), customEnvironment, processCount)
	case executionModeInit:
		err = CopyFiles(logger, outputDir, copyDetails, requiredCopies)
		if err != nil {
			logger.Error(err, "Error copying files")
			os.Exit(1)
		}
	case executionModeSidecar:
		if mainContainerVersion != currentContainerVersion {
			err = CopyFiles(logger, outputDir, copyDetails, requiredCopies)
			if err != nil {
				logger.Error(err, "Error copying files")
				os.Exit(1)
			}
		}
		logger.Info("Waiting for process to be terminated")
		done := make(chan bool)
		<-done
	default:
		logger.Error(nil, "Unknown execution mode", "mode", mode)
		os.Exit(1)
	}
}

func getCopyDetails() (map[string]string, map[string]bool, error) {
	copyDetails := make(map[string]string, len(copyFiles)+len(copyBinaries))

	for _, filePath := range copyFiles {
		copyDetails[path.Join(inputDir, filePath)] = ""
	}
	if copyBinaries != nil {
		if binaryOutputDirectory == "" {
			binaryOutputDirectory = currentContainerVersion
		}
		for _, copyBinary := range copyBinaries {
			copyDetails[fmt.Sprintf("/usr/bin/%s", copyBinary)] = path.Join("bin", binaryOutputDirectory, copyBinary)
		}
	}
	for _, library := range copyLibraries {
		copyDetails[fmt.Sprintf("/usr/lib/fdb/multiversion/libfdb_c_%s.so", library)] = path.Join("lib", "multiversion", fmt.Sprintf("libfdb_c_%s.so", library))
	}
	if copyPrimaryLibrary != "" {
		copyDetails[fmt.Sprintf("/usr/lib/fdb/multiversion/libfdb_c_%s.so", copyPrimaryLibrary)] = path.Join("lib", "libfdb_c.so")
	}
	requiredCopyMap := make(map[string]bool, len(requiredCopyFiles))
	for _, filePath := range requiredCopyFiles {
		fullFilePath := path.Join(inputDir, filePath)
		_, present := copyDetails[fullFilePath]
		if !present {
			return nil, nil, fmt.Errorf("File %s is required, but is not in the --copy-file list", filePath)
		}
		requiredCopyMap[fullFilePath] = true
	}
	return copyDetails, requiredCopyMap, nil
}

func loadAdditionalEnvironment(logger logr.Logger) (map[string]string, error) {
	var customEnvironment = make(map[string]string)
	environmentPattern := regexp.MustCompile(`export ([A-Za-z0-9_]+)=([^\n]*)`)
	if additionalEnvFile != "" {
		file, err := os.Open(additionalEnvFile)
		if err != nil {
			return nil, err
		}

		envScanner := bufio.NewScanner(file)
		for envScanner.Scan() {
			envLine := envScanner.Text()
			matches := environmentPattern.FindStringSubmatch(envLine)
			if matches == nil || envLine == "" {
				logger.Error(nil, "Environment file contains line that we cannot parse", "line", envLine, "environmentPattern", environmentPattern)
				continue
			}
			customEnvironment[matches[1]] = matches[2]
		}
	}
	return customEnvironment, nil
}

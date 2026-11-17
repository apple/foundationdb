// copy.go
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
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"regexp"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/go-logr/logr"
)

const (
	libraryTestDirectoryEnv = "TEST_LIBRARY_DIRECTORY"
	binaryTestDirectoryEnv  = "TEST_BINARY_DIRECTORY"
)

// versionRegex represents the regex to parse the compact version string.
var versionRegex = regexp.MustCompile(`^(\d+)\.(\d+)`)

// copyFile copies a file into the output directory.
func copyFile(logger logr.Logger, inputPath string, outputPath string, required bool) error {
	logger.Info("Copying file", "inputPath", inputPath, "outputPath", outputPath)
	inputFile, err := os.Open(inputPath)
	if err != nil {
		logger.Error(err, "Error opening file", "path", inputPath)
		return err
	}
	defer inputFile.Close()

	inputInfo, err := inputFile.Stat()
	if err != nil {
		logger.Error(err, "Error getting stats for file", "path", inputPath)
		return err
	}

	if required && inputInfo.Size() == 0 {
		return fmt.Errorf("file %s is empty", inputPath)
	}

	tempFile, err := os.CreateTemp(path.Dir(outputPath), "")
	if err != nil {
		return err
	}
	defer tempFile.Close()

	_, err = tempFile.ReadFrom(inputFile)
	if err != nil {
		return err
	}

	err = os.Chmod(tempFile.Name(), inputInfo.Mode())
	if err != nil {
		return err
	}

	err = os.Rename(tempFile.Name(), outputPath)
	if err != nil {
		return err
	}

	return nil
}

// CopyFiles copies a list of files into the output directory.
func copyFiles(logger logr.Logger, outputDir string, copyDetails map[string]string, requiredCopies map[string]bool) error {
	for inputPath, outputSubpath := range copyDetails {
		if outputSubpath == "" {
			outputSubpath = path.Base(inputPath)
		}
		outputPath := path.Join(outputDir, outputSubpath)
		err := os.MkdirAll(path.Dir(outputPath), os.ModeDir|os.ModePerm)
		if err != nil {
			return err
		}

		err = copyFile(logger, inputPath, outputPath, requiredCopies[inputPath])
		if err != nil {
			return err
		}
	}

	return nil
}

// restoreDelay is the time to wait after a file was removed before restoring it. It batches the events of a recursive
// removal into a single restore. This is a variable to allow overwriting it in unit-tests.
var restoreDelay = time.Second

// restoreDeletedFiles blocks until the context is cancelled and copies every file of copyDetails again that
// disappears from the output directory, e.g. because the monitor pruned a staged binary directory before it was needed.
// The initial copy must have happened before this is called.
func restoreDeletedFiles(ctx context.Context, logger logr.Logger, outputDir string, copyDetails map[string]string, requiredCopies map[string]bool) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	outputPaths := make(map[string]string, len(copyDetails))
	for inputPath, outputSubpath := range copyDetails {
		if outputSubpath == "" {
			outputSubpath = path.Base(inputPath)
		}
		outputPaths[inputPath] = path.Join(outputDir, outputSubpath)
	}

	// The watches of removed directories are dropped by the kernel, so they have to be added again after a restore.
	addWatches := func() {
		for _, outputPath := range outputPaths {
			err := watcher.Add(path.Dir(outputPath))
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				logger.Error(err, "Could not watch directory", "path", path.Dir(outputPath))
			}
		}
	}
	addWatches()

	// Files removed before the watches were added generate no event, so check once right away.
	restoreTimer := time.After(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}

			if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
				restoreTimer = time.After(restoreDelay)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}

			logger.Error(err, "Error watching the output directory")
		case <-restoreTimer:
			restoreTimer = nil
			missing := make(map[string]string)
			for inputPath, outputPath := range outputPaths {
				_, err := os.Stat(outputPath)
				if errors.Is(err, os.ErrNotExist) {
					missing[inputPath] = copyDetails[inputPath]
				}
			}

			if len(missing) == 0 {
				continue
			}

			logger.Info("Restoring deleted files", "count", len(missing))
			err := copyFiles(logger, outputDir, missing, requiredCopies)
			if err != nil {
				logger.Error(err, "Error restoring deleted files")
				restoreTimer = time.After(restoreDelay)
				continue
			}

			addWatches()
		}
	}
}

// getCompactVersion will return the compact version representation for the input version. The compact version consists
// of the major and minor version.
func getCompactVersion(version string) (string, error) {
	matches := versionRegex.FindStringSubmatch(version)
	if matches == nil {
		return "", fmt.Errorf("could not parse FDB compact version from %s", version)
	}

	return matches[0], nil
}

// getBinaryDirectory returns the directory where the binaries are located. This will return `/usr/bin` if the env
// variable TEST_BINARY_DIRECTORY is unset.
func getBinaryDirectory() string {
	binaryDirectory, ok := os.LookupEnv(binaryTestDirectoryEnv)
	if ok {
		return binaryDirectory
	}

	return "/usr/bin"
}

// getLibraryPath returns the library where the binaries are located. This will return `/usr/lib/fdb/multiversion` if the env
// variable TEST_LIBRARY_DIRECTORY is unset.
func getLibraryPath() string {
	libraryDirectory, ok := os.LookupEnv(libraryTestDirectoryEnv)
	if ok {
		return libraryDirectory
	}

	return "/usr/lib/fdb/multiversion"
}

// getCopyDetails will generate the details for all the files that should be copied.
func getCopyDetails(inputDir string, copyPrimaryLibrary string, binaryOutputDirectory string, copyFiles []string, copyBinaries []string, copyLibraries []string, requiredCopyFiles []string, currentContainerVersion string, mode executionMode) (map[string]string, map[string]bool, error) {
	copyDetails := make(map[string]string, len(copyFiles)+len(copyBinaries))

	for _, filePath := range copyFiles {
		copyDetails[path.Join(inputDir, filePath)] = ""
	}

	if len(copyBinaries) > 0 {
		if binaryOutputDirectory == "" {
			// The operator expects that the sidecar will copy the binaries into /var/fdb/shared-binaries/bin/<new version>/fdbserver
			if mode == executionModeSidecar {
				binaryOutputDirectory = path.Join("bin", currentContainerVersion)
			} else {
				compactVersion, err := getCompactVersion(currentContainerVersion)
				if err != nil {
					return nil, nil, err
				}

				binaryOutputDirectory = compactVersion
			}
		}

		binaryDirectory := getBinaryDirectory()
		for _, copyBinary := range copyBinaries {
			copyDetails[path.Join(binaryDirectory, copyBinary)] = path.Join(binaryOutputDirectory, copyBinary)
		}
	}

	libraryPath := getLibraryPath()
	for _, library := range copyLibraries {
		libraryName := fmt.Sprintf("libfdb_c_%s.so", library)
		copyDetails[path.Join(libraryPath, libraryName)] = libraryName
	}

	if copyPrimaryLibrary != "" {
		copyDetails[path.Join(libraryPath, fmt.Sprintf("libfdb_c_%s.so", copyPrimaryLibrary))] = "libfdb_c.so"
	}

	requiredCopyMap := make(map[string]bool, len(requiredCopyFiles))
	for _, filePath := range requiredCopyFiles {
		fullFilePath := path.Join(inputDir, filePath)
		_, present := copyDetails[fullFilePath]
		if !present {
			return nil, nil, fmt.Errorf("file %s is required, but is not in the --copy-file list", filePath)
		}
		requiredCopyMap[fullFilePath] = true
	}

	return copyDetails, requiredCopyMap, nil
}

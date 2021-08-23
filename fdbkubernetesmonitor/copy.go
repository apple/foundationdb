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
	"fmt"
	"io"
	"os"
	"path"

	"github.com/go-logr/logr"
)

const (
	bufferSize = 1024
)

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
		return fmt.Errorf("File %s is empty", inputPath)
	}

	outputFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY, inputInfo.Mode())
	if err != nil {
		return err
	}
	defer outputFile.Close()

	var buffer = make([]byte, bufferSize)
	for {
		readLength, readError := inputFile.Read(buffer)
		if readError == io.EOF {
			break
		}
		if readError != nil {
			logger.Error(readError, "Error reading file", "path", inputPath)
			return readError
		}

		_, writeError := outputFile.Write(buffer[:readLength])
		if writeError != nil {
			logger.Error(writeError, "Error writing file", "path", outputPath)
			return writeError
		}
	}
	return nil
}

// CopyFiles copies a list of files into the output directory.
func CopyFiles(logger logr.Logger, outputDir string, copyDetails map[string]string, requiredCopies map[string]bool) error {
	for inputPath, outputSubpath := range copyDetails {
		if outputSubpath == "" {
			outputSubpath = path.Base(inputPath)
		}
		outputPath := path.Join(outputDir, outputSubpath)
		err := os.MkdirAll(path.Dir(outputPath), os.ModeDir|os.ModePerm)
		if err != nil {
			return err
		}

		required := requiredCopies[inputPath]
		err = copyFile(logger, inputPath, outputPath, required)
		if err != nil {
			return err
		}
	}
	return nil
}

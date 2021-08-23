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
	"fmt"

	"github.com/go-logr/zapr"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

var (
	inputDir        string
	fdbserverPath   string
	monitorConfFile string
	logPath         string
)

func main() {
	pflag.StringVar(&fdbserverPath, "fdbserver-path", "/usr/bin/fdbserver", "Path to the fdbserver binary")
	pflag.StringVar(&inputDir, "input-dir", ".", "Directory containing input files")
	pflag.StringVar(&monitorConfFile, "input-monitor-conf", "config.json", "Name of the file in the input directory that contains the monitor configuration")
	pflag.StringVar(&logPath, "log-path", "", "Name of a file to send logs to. Logs will be sent to stdout in addition the file you pass in this argument. If this is blank, logs will only by sent to stdout")
	pflag.Parse()

	zapConfig := zap.NewProductionConfig()
	if logPath != "" {
		zapConfig.OutputPaths = append(zapConfig.OutputPaths, logPath)
	}
	zapLogger, err := zapConfig.Build()
	if err != nil {
		panic(err)
	}

	logger := zapr.NewLogger(zapLogger)
	StartMonitor(logger, fmt.Sprintf("%s/%s", inputDir, monitorConfFile), fdbserverPath)
}

// monitor.go
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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/utils/pointer"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"github.com/fsnotify/fsnotify"
	"github.com/go-logr/logr"
)

// errorBackoffSeconds is the time to wait after a process fails before starting
// another process.
// This delay will only be applied when there has been more than one failure
// within this time window.
const errorBackoffSeconds = 60

// Monitor provides the main monitor loop
type Monitor struct {
	// ConfigFile defines the path to the config file to load.
	ConfigFile string

	// CurrentContainerVersion defines the version of the container. This will be the same as the fdbserver version.
	CurrentContainerVersion string

	// CustomEnvironment defines the custom environment variables to use when
	// interpreting the monitor configuration.
	CustomEnvironment map[string]string

	// ActiveConfiguration defines the active process configuration.
	ActiveConfiguration *api.ProcessConfiguration

	// ActiveConfigurationBytes defines the source data for the active process
	// configuration.
	ActiveConfigurationBytes []byte

	// LastConfigurationTime is the last time we successfully reloaded the
	// configuration file.
	LastConfigurationTime time.Time

	// ProcessCount defines how many processes the
	ProcessCount int

	// ProcessIDs stores the PIDs of the processes that are running. A PID of
	// zero will indicate that a process does not have a run loop. A PID of -1
	// will indicate that a process has a run loop but is not currently running
	// the subprocess.
	ProcessIDs []int

	// Mutex defines a mutex around working with configuration.
	// This is used to synchronize access to local state like the active
	// configuration and the process IDs from multiple goroutines.
	Mutex sync.Mutex

	// PodClient is a client for posting updates about this pod to
	// Kubernetes.
	PodClient *PodClient

	// Logger is the logger instance for this monitor.
	Logger logr.Logger
}

// StartMonitor starts the monitor loop.
func StartMonitor(ctx context.Context, logger logr.Logger, configFile string, customEnvironment map[string]string, processCount int, listenAddr string, enableDebug bool, currentContainerVersion string) {
	podClient, err := CreatePodClient(ctx, logger)
	if err != nil {
		logger.Error(err, "could not create Pod client")
		os.Exit(1)
	}

	monitor := &Monitor{
		ConfigFile:              configFile,
		PodClient:               podClient,
		Logger:                  logger,
		CustomEnvironment:       customEnvironment,
		ProcessCount:            processCount,
		CurrentContainerVersion: currentContainerVersion,
	}

	go func() { monitor.WatchPodTimestamps() }()

	mux := http.NewServeMux()
	// Enable pprof endpoints for debugging purposes.
	if enableDebug {
		mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
		mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
		mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
		mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
		mux.Handle("/debug/pprof/block", pprof.Handler("block"))
		mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	// Add Prometheus support
	mux.Handle("/metrics", promhttp.Handler())
	go func() {
		err := http.ListenAndServe(listenAddr, mux)
		if err != nil {
			logger.Error(err, "could not start HTTP server")
			os.Exit(1)
		}
	}()

	monitor.Run()
}

// LoadConfiguration loads the latest configuration from the config file.
func (monitor *Monitor) LoadConfiguration() {
	file, err := os.Open(monitor.ConfigFile)
	if err != nil {
		monitor.Logger.Error(err, "Error reading monitor config file", "monitorConfigPath", monitor.ConfigFile)
		return
	}
	defer file.Close()
	configuration := &api.ProcessConfiguration{}
	configurationBytes, err := io.ReadAll(file)
	if err != nil {
		monitor.Logger.Error(err, "Error reading monitor configuration", "monitorConfigPath", monitor.ConfigFile)
	}
	err = json.Unmarshal(configurationBytes, configuration)
	if err != nil {
		monitor.Logger.Error(err, "Error parsing monitor configuration", "rawConfiguration", string(configurationBytes))
		return
	}

	if monitor.CurrentContainerVersion == configuration.Version {
		configuration.BinaryPath = fdbserverPath
	} else {
		configuration.BinaryPath = path.Join(sharedBinaryDir, configuration.Version, "fdbserver")
	}

	err = checkOwnerExecutable(configuration.BinaryPath)
	if err != nil {
		monitor.Logger.Error(err, "Error with binary path for latest configuration", "configuration", configuration, "binaryPath", configuration.BinaryPath)
		return
	}

	_, err = configuration.GenerateArguments(1, monitor.CustomEnvironment)
	if err != nil {
		monitor.Logger.Error(err, "Error generating arguments for latest configuration", "configuration", configuration, "binaryPath", configuration.BinaryPath)
		return
	}

	monitor.acceptConfiguration(configuration, configurationBytes)
}

// checkOwnerExecutable validates that a path is a file that exists and is
// executable by its owner.
func checkOwnerExecutable(path string) error {
	binaryStat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if binaryStat.Mode()&0o100 == 0 {
		return fmt.Errorf("binary is not executable")
	}
	return nil
}

// acceptConfiguration is called when the monitor process parses and accepts
// a configuration from the local config file.
func (monitor *Monitor) acceptConfiguration(configuration *api.ProcessConfiguration, configurationBytes []byte) {
	monitor.Mutex.Lock()
	defer monitor.Mutex.Unlock()
	monitor.Logger.Info("Received new configuration file", "configuration", configuration)

	if monitor.ProcessIDs == nil {
		monitor.ProcessIDs = make([]int, monitor.ProcessCount+1)
	} else {
		for len(monitor.ProcessIDs) <= monitor.ProcessCount {
			monitor.ProcessIDs = append(monitor.ProcessIDs, 0)
		}
	}

	monitor.ActiveConfiguration = configuration
	monitor.ActiveConfigurationBytes = configurationBytes
	monitor.LastConfigurationTime = time.Now()

	for processNumber := 1; processNumber <= monitor.ProcessCount; processNumber++ {
		if monitor.ProcessIDs[processNumber] == 0 {
			monitor.ProcessIDs[processNumber] = -1
			tempNumber := processNumber
			go func() { monitor.RunProcess(tempNumber) }()
		}
	}

	err := monitor.PodClient.UpdateAnnotations(monitor)
	if err != nil {
		monitor.Logger.Error(err, "Error updating pod annotations")
	}
}

// RunProcess runs a loop to continually start and watch a process.
func (monitor *Monitor) RunProcess(processNumber int) {
	pid := 0
	logger := monitor.Logger.WithValues("processNumber", processNumber, "area", "RunProcess")
	logger.Info("Starting run loop")
	for {
		if !monitor.checkProcessRequired(processNumber) {
			return
		}

		arguments, err := monitor.ActiveConfiguration.GenerateArguments(processNumber, monitor.CustomEnvironment)
		if err != nil {
			logger.Error(err, "Error generating arguments for subprocess", "configuration", monitor.ActiveConfiguration)
			time.Sleep(errorBackoffSeconds * time.Second)
		}
		cmd := exec.Cmd{
			Path: arguments[0],
			Args: arguments,
		}

		logger.Info("Starting subprocess", "arguments", arguments)

		stdout, err := cmd.StdoutPipe()
		if err != nil {
			logger.Error(err, "Error getting stdout from subprocess")
		}

		stderr, err := cmd.StderrPipe()
		if err != nil {
			logger.Error(err, "Error getting stderr from subprocess")
		}

		err = cmd.Start()
		if err != nil {
			logger.Error(err, "Error starting subprocess")
			time.Sleep(errorBackoffSeconds * time.Second)
			continue
		}

		if cmd.Process != nil {
			pid = cmd.Process.Pid
		} else {
			logger.Error(nil, "No Process information available for subprocess")
		}

		startTime := time.Now()
		logger.Info("Subprocess started", "PID", pid)

		monitor.updateProcessID(processNumber, pid)

		if stdout != nil {
			stdoutScanner := bufio.NewScanner(stdout)
			go func() {
				for stdoutScanner.Scan() {
					logger.Info("Subprocess output", "msg", stdoutScanner.Text(), "PID", pid)
				}
			}()
		}

		if stderr != nil {
			stderrScanner := bufio.NewScanner(stderr)
			go func() {
				for stderrScanner.Scan() {
					logger.Error(nil, "Subprocess error log", "msg", stderrScanner.Text(), "PID", pid)
				}
			}()
		}

		err = cmd.Wait()
		if err != nil {
			logger.Error(err, "Error from subprocess", "PID", pid)
		}
		exitCode := -1
		if cmd.ProcessState != nil {
			exitCode = cmd.ProcessState.ExitCode()
		}

		logger.Info("Subprocess terminated", "exitCode", exitCode, "PID", pid)

		endTime := time.Now()
		monitor.updateProcessID(processNumber, -1)

		processDuration := endTime.Sub(startTime)
		if processDuration.Seconds() < errorBackoffSeconds {
			logger.Info("Backing off from restarting subprocess", "backOffTimeSeconds", errorBackoffSeconds, "lastExecutionDurationSeconds", processDuration)
			time.Sleep(errorBackoffSeconds * time.Second)
		}
	}
}

// checkProcessRequired determines if the latest configuration requires that a
// process stay running.
// If the process is no longer desired, this will remove it from the process ID
// list and return false. If the process is still desired, this will return
// true.
func (monitor *Monitor) checkProcessRequired(processNumber int) bool {
	monitor.Mutex.Lock()
	defer monitor.Mutex.Unlock()
	logger := monitor.Logger.WithValues("processNumber", processNumber, "area", "checkProcessRequired")
	runProcesses := pointer.BoolDeref(monitor.ActiveConfiguration.RunServers, true)
	if monitor.ProcessCount < processNumber || !runProcesses {
		logger.Info("Terminating run loop")
		monitor.ProcessIDs[processNumber] = 0
		return false
	}

	return true
}

// updateProcessID records a new Process ID from a newly launched process.
func (monitor *Monitor) updateProcessID(processNumber int, pid int) {
	monitor.Mutex.Lock()
	defer monitor.Mutex.Unlock()
	monitor.ProcessIDs[processNumber] = pid
}

// WatchConfiguration detects changes to the monitor configuration file.
func (monitor *Monitor) WatchConfiguration(watcher *fsnotify.Watcher) {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}
			monitor.Logger.Info("Detected event on monitor conf file", "event", event)
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				monitor.LoadConfiguration()
			} else if event.Op&fsnotify.Remove == fsnotify.Remove {
				err := watcher.Add(monitor.ConfigFile)
				if err != nil {
					panic(err)
				}
				monitor.LoadConfiguration()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			monitor.Logger.Error(err, "Error watching for file system events")
		}
	}
}

// Run runs the monitor loop.
func (monitor *Monitor) Run() {
	done := make(chan bool, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		latestSignal := <-signals
		monitor.Logger.Info("Received system signal", "signal", latestSignal)

		// Reset the ProcessCount to 0 to make sure the monitor doesn't try to restart the processes.
		monitor.ProcessCount = 0
		for processNumber, processID := range monitor.ProcessIDs {
			if processID > 0 {
				subprocessLogger := monitor.Logger.WithValues("processNumber", processNumber, "PID", processID)
				process, err := os.FindProcess(processID)
				if err != nil {
					subprocessLogger.Error(err, "Error finding subprocess")
					continue
				}
				subprocessLogger.Info("Sending signal to subprocess", "signal", latestSignal)
				err = process.Signal(latestSignal)
				if err != nil {
					subprocessLogger.Error(err, "Error signaling subprocess")
					continue
				}
			}
		}

		annotations := monitor.PodClient.metadata.Annotations
		if len(annotations) > 0 {
			delayValue, ok := annotations[DelayShutdownAnnotation]
			if ok {
				delay, err := time.ParseDuration(delayValue)
				if err == nil {
					time.Sleep(delay)
				}
			}
		}

		done <- true
	}()

	monitor.LoadConfiguration()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	err = watcher.Add(monitor.ConfigFile)
	if err != nil {
		panic(err)
	}

	defer func(watcher *fsnotify.Watcher) {
		err := watcher.Close()
		if err != nil {
			monitor.Logger.Error(err, "could not close watcher")
		}
	}(watcher)
	go func() { monitor.WatchConfiguration(watcher) }()

	<-done
}

// WatchPodTimestamps watches the timestamp feed to reload the configuration.
func (monitor *Monitor) WatchPodTimestamps() {
	for timestamp := range monitor.PodClient.TimestampFeed {
		if timestamp > monitor.LastConfigurationTime.Unix() {
			monitor.LoadConfiguration()
		}
	}
}

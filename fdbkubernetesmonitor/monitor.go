// monitor.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"github.com/fsnotify/fsnotify"
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/utils/pointer"
)

const (
	// maxErrorBackoffSeconds is the maximum time to wait after a process fails before starting another process.
	// The actual delay will be based on the observed errors and will increase until maxErrorBackoffSeconds is hit.
	maxErrorBackoffSeconds = 60 * time.Second

	// fdbClusterFilePath defines the default path to the fdb cluster file that contains the current connection string.
	// This file is managed by the fdbserver processes itself and they will automatically update the file if the
	// coordinators have changed.
	fdbClusterFilePath = "/var/fdb/data/fdb.cluster"
)

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

	// metrics represents the prometheus monitor metrics.
	metrics *metrics
}

type httpConfig struct {
	listenAddr, certFile, keyFile string
}

// StartMonitor starts the monitor loop.
func StartMonitor(ctx context.Context, logger logr.Logger, configFile string, customEnvironment map[string]string, processCount int, promConfig httpConfig, enableDebug bool, currentContainerVersion string, enableNodeWatcher bool) {
	podClient, err := CreatePodClient(ctx, logger, enableNodeWatcher, setupCache)
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
		ProcessIDs:              make([]int, processCount+1),
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

	reg := prometheus.NewRegistry()
	// Enable the default go metrics.
	reg.MustRegister(collectors.NewGoCollector())
	monitorMetrics := registerMetrics(reg)
	monitor.metrics = monitorMetrics
	promHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	// Add Prometheus support
	mux.Handle("/metrics", promHandler)
	go func() {
		if promConfig.keyFile != "" || promConfig.certFile != "" {
			err := http.ListenAndServeTLS(promConfig.listenAddr, promConfig.certFile, promConfig.keyFile, mux)
			if err != nil {
				logger.Error(err, "could not start HTTPS server")
				os.Exit(1)
			}
		}
		err := http.ListenAndServe(promConfig.listenAddr, mux)
		if err != nil {
			logger.Error(err, "could not start HTTP server")
			os.Exit(1)
		}
	}()

	monitor.Run()
}

// updateCustomEnvironment will add the node labels and their values to the custom environment map. All the generated
// environment variables will start with NODE_LABEL and "/" and "." will be replaced in the key as "_", e.g. from the
// label "foundationdb.org/testing = awesome" the env variables NODE_LABEL_FOUNDATIONDB_ORG_TESTING = awesome" will be
// generated.
func (monitor *Monitor) updateCustomEnvironmentFromNodeMetadata() {
	if monitor.PodClient.nodeMetadata == nil {
		return
	}

	nodeLabels := monitor.PodClient.nodeMetadata.Labels
	for key, value := range nodeLabels {
		sanitizedKey := strings.ReplaceAll(key, "/", "_")
		sanitizedKey = strings.ReplaceAll(sanitizedKey, ".", "_")
		envKey := "NODE_LABEL_" + strings.ToUpper(sanitizedKey)
		currentValue, ok := monitor.CustomEnvironment[envKey]
		if !ok {
			monitor.Logger.Info("adding new custom environment variable from node labels", "key", envKey, "value", value)
			monitor.CustomEnvironment[envKey] = value
			continue
		}

		if currentValue == value {
			continue
		}

		monitor.Logger.Info("update custom environment variable from node labels", "key", envKey, "newValue", value, "currentValue", currentValue)
		monitor.CustomEnvironment[envKey] = value
		continue
	}
}

// LoadConfiguration loads the latest configuration from the config file.
func (monitor *Monitor) LoadConfiguration() {
	file, err := os.Open(monitor.ConfigFile)
	if err != nil {
		monitor.Logger.Error(err, "Error reading monitor config file", "monitorConfigPath", monitor.ConfigFile)
		return
	}
	defer func() {
		err := file.Close()
		monitor.Logger.Error(err, "Error could not close file", "monitorConfigPath", monitor.ConfigFile)
	}()
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

	monitor.updateCustomEnvironmentFromNodeMetadata()

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

	// If the configuration hasn't changed ignore those events to prevent noisy logging.
	if equality.Semantic.DeepEqual(monitor.ActiveConfiguration, configuration) {
		return
	}

	monitor.Logger.Info("Received new configuration file", "configuration", configuration)
	monitor.ActiveConfiguration = configuration
	monitor.ActiveConfigurationBytes = configurationBytes
	monitor.LastConfigurationTime = time.Now()
	// Update the prometheus metrics.
	monitor.metrics.registerConfigurationChange(configuration.Version)

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

// getBackoffDuration returns the backoff duration. The backoff time will increase exponential with a maximum of 60 seconds.
func getBackoffDuration(errorCounter int) time.Duration {
	timeToBackoff := time.Duration(errorCounter*errorCounter) * time.Second
	if timeToBackoff > maxErrorBackoffSeconds {
		return maxErrorBackoffSeconds
	}

	return timeToBackoff
}

// RunProcess runs a loop to continually start and watch a process.
func (monitor *Monitor) RunProcess(processNumber int) {
	pid := 0
	logger := monitor.Logger.WithValues("processNumber", processNumber, "area", "RunProcess")
	logger.Info("Starting run loop")
	startTime := time.Now()
	// Counts the successive errors that occurred during process start up. Based on the error count the backoff time
	// will be calculated.
	var errorCounter int

	for {
		if !monitor.processRequired(processNumber) {
			return
		}

		durationSinceLastStart := time.Since(startTime)
		// If for more than 5 minutes no error have occurred we reset the error counter to reset the backoff time.
		if durationSinceLastStart > 5*time.Minute {
			errorCounter = 0
		}

		arguments, err := monitor.ActiveConfiguration.GenerateArguments(processNumber, monitor.CustomEnvironment)
		if err != nil {
			backoffDuration := getBackoffDuration(errorCounter)
			logger.Error(err, "Error generating arguments for subprocess", "configuration", monitor.ActiveConfiguration, "errorCounter", errorCounter, "backoffDuration", backoffDuration.String())
			time.Sleep(backoffDuration)
			errorCounter++
			continue
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
			backoffDuration := getBackoffDuration(errorCounter)
			logger.Error(err, "Error starting subprocess", "backoffDuration", backoffDuration.String())
			time.Sleep(backoffDuration)
			errorCounter++
			continue
		}

		// Update the prometheus metrics for the process.
		monitor.metrics.registerProcessStartup(processNumber, monitor.ActiveConfiguration.Version)

		if cmd.Process != nil {
			pid = cmd.Process.Pid
		} else {
			logger.Error(nil, "No Process information available for subprocess")
		}

		startTime = time.Now()
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

		processDuration := time.Since(startTime)
		logger.Info("Subprocess terminated", "exitCode", exitCode, "PID", pid, "lastExecutionDurationSeconds", processDuration.String())
		monitor.updateProcessID(processNumber, -1)

		// Only backoff if the exit code is non-zero.
		if exitCode != 0 {
			backoffDuration := getBackoffDuration(errorCounter)
			logger.Info("Backing off from restarting subprocess", "backoffDuration", backoffDuration.String(), "lastExecutionDurationSeconds", processDuration.String(), "errorCounter", errorCounter, "exitCode", exitCode)
			time.Sleep(backoffDuration)
			errorCounter++
		}
	}
}

// processRequired determines if the latest configuration requires that a
// process stay running.
// If the process is no longer desired, this will remove it from the process ID
// list and return false. If the process is still desired, this will return
// true.
func (monitor *Monitor) processRequired(processNumber int) bool {
	monitor.Mutex.Lock()
	defer monitor.Mutex.Unlock()
	logger := monitor.Logger.WithValues("processNumber", processNumber, "area", "processRequired")
	if monitor.ProcessCount < processNumber || !pointer.BoolDeref(monitor.ActiveConfiguration.RunServers, true) {
		if monitor.ProcessIDs[processNumber] != 0 {
			logger.Info("Terminating run loop")
			monitor.ProcessIDs[processNumber] = 0
		}

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

			monitor.Logger.Info("Detected event on monitor conf file or cluster file", "event", event)
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				monitor.handleFileChange(event.Name)
			} else if event.Op&fsnotify.Remove == fsnotify.Remove {
				err := watcher.Add(event.Name)
				if err != nil {
					panic(err)
				}
				monitor.handleFileChange(event.Name)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			monitor.Logger.Error(err, "Error watching for file system events")
		}
	}
}

// handleFileChange will perform the required action based on the changed/modified file.
func (monitor *Monitor) handleFileChange(changedFile string) {
	if changedFile == fdbClusterFilePath {
		err := monitor.PodClient.updateFdbClusterTimestampAnnotation()
		if err != nil {
			monitor.Logger.Error(err, fmt.Sprintf("could not update %s annotation", ClusterFileChangeDetectedAnnotation))
		}
		return
	}

	monitor.LoadConfiguration()
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
			if processID <= 0 {
				continue
			}

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

		annotations := monitor.PodClient.podMetadata.Annotations
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
	monitor.Logger.Info("adding watch for file", "path", path.Base(monitor.ConfigFile))
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

	// The cluster file will be created and managed by the fdbserver processes, so we have to wait until the fdbserver
	// processes have been started. Except for the initial cluster creation his file should be present as soon as the
	// monitor starts the processes.
	for {
		_, err = os.Stat(fdbClusterFilePath)
		if errors.Is(err, os.ErrNotExist) {
			monitor.Logger.Info("waiting for file to be created", "path", fdbClusterFilePath)
			time.Sleep(5 * time.Second)
			continue
		}

		monitor.Logger.Info("adding watch for file", "path", fdbClusterFilePath)
		err = watcher.Add(fdbClusterFilePath)
		if err != nil {
			panic(err)
		}
		break
	}

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

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
	"crypto/tls"
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"github.com/apple/foundationdb/fdbkubernetesmonitor/internal/certloader"
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

// monitor provides the main monitor loop
type monitor struct {
	// configFile defines the path to the config file to load.
	configFile string

	// currentContainerVersion defines the version of the container. This will be the same as the fdbserver version.
	currentContainerVersion api.Version

	// customEnvironment defines the custom environment variables to use when
	// interpreting the monitor configuration.
	customEnvironment map[string]string

	// activeConfiguration defines the active process configuration.
	activeConfiguration *api.ProcessConfiguration

	// activeConfigurationBytes defines the source data for the active process
	// configuration.
	activeConfigurationBytes []byte

	// lastConfigurationTime is the last time we successfully reloaded the
	// configuration file.
	lastConfigurationTime time.Time

	// processCount defines how many processes the
	processCount int

	// processIDs stores the PIDs of the processes that are running. A PID of
	// zero will indicate that a process does not have a run loop. A PID of -1
	// will indicate that a process has a run loop but is not currently running
	// the subprocess.
	processIDs []int

	// mutex defines a mutex around working with configuration.
	// This is used to synchronize access to local state like the active
	// configuration and the process IDs from multiple goroutines.
	mutex sync.Mutex

	// podClient is a client for posting updates about this pod to
	// Kubernetes.
	podClient *kubernetesClient

	// logger is the logger instance for this monitor.
	logger logr.Logger

	// metrics represents the prometheus monitor metrics.
	metrics *metrics
}

type httpConfig struct {
	listenAddr, certPath, keyPath, rootCaPath string
}

// startMonitor starts the monitor loop.
func startMonitor(ctx context.Context, logger logr.Logger, configFile string, customEnvironment map[string]string, processCount int, promConfig httpConfig, enableDebug bool, currentContainerVersion api.Version, enableNodeWatcher bool) {
	client, err := createPodClient(ctx, logger, enableNodeWatcher, setupCache)
	if err != nil {
		logger.Error(err, "could not create Pod client")
		os.Exit(1)
	}

	mon := &monitor{
		configFile:              configFile,
		podClient:               client,
		logger:                  logger,
		customEnvironment:       customEnvironment,
		processCount:            processCount,
		processIDs:              make([]int, processCount+1),
		currentContainerVersion: currentContainerVersion,
	}

	go func() { mon.watchPodTimestamps() }()

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
	mon.metrics = monitorMetrics
	promHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	// Add Prometheus support
	mux.Handle("/metrics", promHandler)
	go func() {
		if promConfig.keyPath != "" || promConfig.certPath != "" {
			certLoader := certloader.NewCertLoader(logger, promConfig.certPath, promConfig.keyPath)
			tlsConfig := &tls.Config{
				GetCertificate: certLoader.GetCertificate,
			}
			server := &http.Server{
				Addr:      promConfig.listenAddr,
				Handler:   mux,
				TLSConfig: tlsConfig,
			}
			err = server.ListenAndServeTLS("", "")
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

	mon.run()
}

// updateCustomEnvironment will add the node labels and their values to the custom environment map. All the generated
// environment variables will start with NODE_LABEL and "/" and "." will be replaced in the key as "_", e.g. from the
// label "foundationdb.org/testing = awesome" the env variables NODE_LABEL_FOUNDATIONDB_ORG_TESTING = awesome" will be
// generated.
func (monitor *monitor) updateCustomEnvironmentFromNodeMetadata() {
	if monitor.podClient.nodeMetadata == nil {
		return
	}

	nodeLabels := monitor.podClient.nodeMetadata.Labels
	for key, value := range nodeLabels {
		sanitizedKey := strings.ReplaceAll(key, "/", "_")
		sanitizedKey = strings.ReplaceAll(sanitizedKey, ".", "_")
		envKey := "NODE_LABEL_" + strings.ToUpper(sanitizedKey)
		currentValue, ok := monitor.customEnvironment[envKey]
		if !ok {
			monitor.logger.Info("adding new custom environment variable from node labels", "key", envKey, "value", value)
			monitor.customEnvironment[envKey] = value
			continue
		}

		if currentValue == value {
			continue
		}

		monitor.logger.Info("update custom environment variable from node labels", "key", envKey, "newValue", value, "currentValue", currentValue)
		monitor.customEnvironment[envKey] = value
		continue
	}
}

// readConfiguration reads the latest configuration from the monitor file.
func (monitor *monitor) readConfiguration() (*api.ProcessConfiguration, []byte) {
	file, err := os.Open(monitor.configFile)
	if err != nil {
		monitor.logger.Error(err, "Error reading monitor config file", "monitorConfigPath", monitor.configFile)
		return nil, nil
	}
	defer func() {
		err := file.Close()
		monitor.logger.Error(err, "Error could not close file", "monitorConfigPath", monitor.configFile)
	}()
	configuration := &api.ProcessConfiguration{}
	configurationBytes, err := io.ReadAll(file)
	if err != nil {
		monitor.logger.Error(err, "Error reading monitor configuration", "monitorConfigPath", monitor.configFile)
	}
	err = json.Unmarshal(configurationBytes, configuration)
	if err != nil {
		monitor.logger.Error(err, "Error parsing monitor configuration", "rawConfiguration", string(configurationBytes))
		return nil, nil
	}

	if configuration.Version == nil {
		monitor.logger.Error(err, "Error could not parse configured version", "rawConfiguration", string(configurationBytes))
		return nil, nil
	}

	// If the versions are protocol compatible don't try to point to another binary path. Otherwise, the processes will
	// cannot restart when a process crashes during a patch upgrade.
	if monitor.currentContainerVersion.IsProtocolCompatible(*configuration.Version) {
		configuration.BinaryPath = fdbserverPath
	} else {
		configuration.BinaryPath = path.Join(sharedBinaryDir, configuration.Version.String(), "fdbserver")
	}

	err = checkOwnerExecutable(configuration.BinaryPath)
	if err != nil {
		monitor.logger.Error(err, "Error with binary path for latest configuration", "configuration", configuration, "binaryPath", configuration.BinaryPath)
		return nil, nil
	}

	monitor.updateCustomEnvironmentFromNodeMetadata()
	_, err = configuration.GenerateArguments(1, monitor.customEnvironment)
	if err != nil {
		monitor.logger.Error(err, "Error generating arguments for latest configuration", "configuration", configuration, "binaryPath", configuration.BinaryPath)
		return nil, nil
	}

	if configuration.ShouldRunServers() {
		// In case that the process is isolated we don't want to start the servers and we should terminate the running fdbserver
		// instances.
		if monitor.processIsIsolated() {
			configuration.RunServers = pointer.Bool(false)
		}
	}

	return configuration, configurationBytes
}

// loadConfiguration loads the latest configuration from the config file.
func (monitor *monitor) loadConfiguration() {
	configuration, configurationBytes := monitor.readConfiguration()
	if configuration == nil || len(configurationBytes) == 0 {
		return
	}

	monitor.acceptConfiguration(configuration, configurationBytes)
	// Always update the annotations if needed to handle cases where the fdb-kubernetes-monitor
	// has loaded the new configuration but was not able to update the annotations.
	err := monitor.podClient.updateAnnotations(monitor)
	if err != nil {
		monitor.logger.Error(err, "Error updating pod annotations")
	}
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
func (monitor *monitor) acceptConfiguration(configuration *api.ProcessConfiguration, configurationBytes []byte) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	// If the configuration hasn't changed ignore those events to prevent noisy logging.
	if equality.Semantic.DeepEqual(monitor.activeConfiguration, configuration) {
		return
	}

	monitor.logger.Info("Received new configuration file", "configuration", configuration)
	monitor.activeConfiguration = configuration
	monitor.activeConfigurationBytes = configurationBytes
	monitor.lastConfigurationTime = time.Now()
	// Update the prometheus metrics.
	monitor.metrics.registerConfigurationChange(configuration.Version.String())

	var hasRunningProcesses bool
	for processNumber := 1; processNumber <= monitor.processCount; processNumber++ {
		if monitor.processIDs[processNumber] == 0 {
			monitor.processIDs[processNumber] = -1
			tempNumber := processNumber
			go func() { monitor.runProcess(tempNumber) }()
			continue
		}

		hasRunningProcesses = true
	}

	// If the monitor has running processes but the processes shouldn't be running, kill them with SIGTERM.
	if hasRunningProcesses && !monitor.activeConfiguration.ShouldRunServers() {
		monitor.sendSignalToProcesses(syscall.SIGTERM)
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

// runProcess runs a loop to continually start and watch a process.
func (monitor *monitor) runProcess(processNumber int) {
	pid := 0
	logger := monitor.logger.WithValues("processNumber", processNumber, "area", "runProcess")
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

		arguments, err := monitor.activeConfiguration.GenerateArguments(processNumber, monitor.customEnvironment)
		if err != nil {
			backoffDuration := getBackoffDuration(errorCounter)
			logger.Error(err, "Error generating arguments for subprocess", "configuration", monitor.activeConfiguration, "errorCounter", errorCounter, "backoffDuration", backoffDuration.String())
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
		monitor.metrics.registerProcessStartup(processNumber, monitor.activeConfiguration.Version.String())

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
func (monitor *monitor) processRequired(processNumber int) bool {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()
	logger := monitor.logger.WithValues("processNumber", processNumber, "area", "processRequired")
	if monitor.processCount < processNumber || !monitor.activeConfiguration.ShouldRunServers() {
		if monitor.processIDs[processNumber] != 0 {
			logger.Info("Terminating run loop")
			monitor.processIDs[processNumber] = 0
		}

		return false
	}

	return true
}

// processIsIsolated returns true if the IsolateProcessGroupAnnotation is set to "true".
func (monitor *monitor) processIsIsolated() bool {
	if monitor.podClient.podMetadata == nil {
		return false
	}

	if monitor.podClient.podMetadata.Annotations == nil {
		return false
	}

	val, ok := monitor.podClient.podMetadata.Annotations[api.IsolateProcessGroupAnnotation]
	if !ok {
		return false
	}

	isolated, err := strconv.ParseBool(val)
	if err != nil {
		monitor.logger.Error(err, "could not parse the value of the %s annotation", api.IsolateProcessGroupAnnotation)
		return false
	}

	return isolated
}

// updateProcessID records a new Process ID from a newly launched process.
func (monitor *monitor) updateProcessID(processNumber int, pid int) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()
	monitor.processIDs[processNumber] = pid
}

// watchConfiguration detects changes to the monitor configuration file.
func (monitor *monitor) watchConfiguration(watcher *fsnotify.Watcher) {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			monitor.logger.Info("Detected event on monitor conf file or cluster file", "event", event)
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
			monitor.logger.Error(err, "Error watching for file system events")
		}
	}
}

// handleFileChange will perform the required action based on the changed/modified file.
func (monitor *monitor) handleFileChange(changedFile string) {
	if changedFile == fdbClusterFilePath {
		err := monitor.podClient.updateFdbClusterTimestampAnnotation()
		if err != nil {
			monitor.logger.Error(err, fmt.Sprintf("could not update %s annotation", api.ClusterFileChangeDetectedAnnotation))
		}
		return
	}

	monitor.loadConfiguration()
}

func (monitor *monitor) sendSignalToProcesses(signal os.Signal) {
	for processNumber, processID := range monitor.processIDs {
		if processID <= 0 {
			continue
		}

		subprocessLogger := monitor.logger.WithValues("processNumber", processNumber, "PID", processID)
		process, err := os.FindProcess(processID)
		if err != nil {
			subprocessLogger.Error(err, "Error finding subprocess")
			continue
		}
		subprocessLogger.Info("Sending signal to subprocess", "signal", signal)
		err = process.Signal(signal)
		if err != nil {
			subprocessLogger.Error(err, "Error signaling subprocess")
			continue
		}
	}
}

// run runs the monitor loop.
func (monitor *monitor) run() {
	done := make(chan bool, 1)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		latestSignal := <-signals
		monitor.logger.Info("Received system signal", "signal", latestSignal)

		// Reset the processCount to 0 to make sure the monitor doesn't try to restart the processes.
		monitor.processCount = 0
		monitor.sendSignalToProcesses(latestSignal)

		annotations := monitor.podClient.podMetadata.Annotations
		if len(annotations) > 0 {
			delayValue, ok := annotations[api.DelayShutdownAnnotation]
			if ok {
				delay, err := time.ParseDuration(delayValue)
				if err == nil {
					time.Sleep(delay)
				}
			}
		}

		done <- true
	}()

	monitor.loadConfiguration()
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		panic(err)
	}
	monitor.logger.Info("adding watch for file", "path", path.Base(monitor.configFile))
	err = watcher.Add(monitor.configFile)
	if err != nil {
		panic(err)
	}

	defer func(watcher *fsnotify.Watcher) {
		err := watcher.Close()
		if err != nil {
			monitor.logger.Error(err, "could not close watcher")
		}
	}(watcher)
	go func() { monitor.watchConfiguration(watcher) }()

	// The cluster file will be created and managed by the fdbserver processes, so we have to wait until the fdbserver
	// processes have been started. Except for the initial cluster creation this file should be present as soon as the
	// monitor starts the processes.
	for {
		_, err = os.Stat(fdbClusterFilePath)
		if errors.Is(err, os.ErrNotExist) {
			monitor.logger.Info("waiting for file to be created", "path", fdbClusterFilePath)
			time.Sleep(5 * time.Second)
			continue
		}

		monitor.logger.Info("adding watch for file", "path", fdbClusterFilePath)
		err = watcher.Add(fdbClusterFilePath)
		if err != nil {
			panic(err)
		}
		break
	}

	<-done
}

// watchPodTimestamps watches the timestamp feed to reload the configuration.
func (monitor *monitor) watchPodTimestamps() {
	for timestamp := range monitor.podClient.TimestampFeed {
		if timestamp > monitor.lastConfigurationTime.Unix() {
			monitor.loadConfiguration()
		}
	}
}

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
	"maps"
	"math/rand/v2"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
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
	// maxErrorBackoffDuration is the maximum time to wait after a process fails before starting another process.
	// The actual delay will be based on the observed errors and will increase until maxErrorBackoffDuration is hit.
	maxErrorBackoffDuration = 60 * time.Second

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

	// processVersions stores the version of the configuration each running subprocess was started from. Processes
	// that are not running have no entry. Access is guarded by mutex.
	processVersions map[int]api.Version

	// processBinaries stores the binary path and the start time of each running subprocess. It has the same entries
	// as processVersions. Access is guarded by mutex.
	processBinaries map[int]processBinary

	// stagedVersions is the formatted list of staged versions that was listed last. It is only meaningful if
	// stagedVersionsKnown is set. Access is guarded by mutex.
	stagedVersions      string
	stagedVersionsKnown bool

	// pruneMutex serializes the pruning of the shared binary directory.
	pruneMutex sync.Mutex

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

	// binaryRetryCancel cancels the running retry for a configuration whose binary was not ready. It may be
	// non-nil after the retry has finished, cancelling it is then a no-op. Access is guarded by mutex.
	binaryRetryCancel context.CancelFunc

	// binaryRetryBackoff returns the wait time before the given retry attempt. If nil getBinaryRetryBackoff is used.
	// This is used to overwrite the default retry mechanism in unit-tests.
	binaryRetryBackoff func(attempt int) time.Duration
}

// processBinary describes the binary a running subprocess was started from and when.
type processBinary struct {
	path      string
	startTime time.Time
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

	go func() { mon.watchPodTimestamps(ctx) }()

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
	go func() { mon.watchBinaries(ctx) }()
	promHandler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	// Add Prometheus support
	mux.Handle("/metrics", promHandler)
	go func() {
		if promConfig.keyPath != "" || promConfig.certPath != "" {
			certLoader := certloader.NewCertLoader(logger, promConfig.certPath, promConfig.keyPath)
			tlsConfig := &tls.Config{
				GetCertificate: certLoader.GetCertificate,
				MinVersion:     tls.VersionTLS12,
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

	mon.run(ctx)
}

// updateCustomEnvironment will add the node labels and their values to the custom environment map. All the generated
// environment variables will start with NODE_LABEL and "/" and "." will be replaced in the key as "_", e.g. from the
// label "foundationdb.org/testing = awesome" the env variables NODE_LABEL_FOUNDATIONDB_ORG_TESTING = awesome" will be
// generated.
func (monitor *monitor) updateCustomEnvironmentFromNodeMetadata(ctx context.Context) error {
	nodeMeta, err := monitor.podClient.getNodeMetadata(ctx)
	if err != nil {
		return err
	}

	if nodeMeta == nil {
		return nil
	}

	// The map is read by the process runners and the configuration loaders, so all writes must hold the mutex.
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	nodeLabels := nodeMeta.Labels
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

	return nil
}

// copyCustomEnvironment returns a snapshot of the custom environment.
func (monitor *monitor) copyCustomEnvironment() map[string]string {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	customEnv := make(map[string]string, len(monitor.customEnvironment))
	maps.Copy(customEnv, monitor.customEnvironment)
	return customEnv
}

// errBinaryNotReady is returned if the binary of a configuration is missing, empty or not executable. Unlike other
// configuration errors this can resolve without a new configuration, e.g. once a sidecar finished copying the binary.
var errBinaryNotReady = errors.New("binary is not ready")

// readConfiguration reads the latest configuration from the monitor file. A nil configuration is returned if no usable
// configuration could be read. The returned error is only non-nil, and wraps errBinaryNotReady, if retrying the same
// configuration later can succeed.
func (monitor *monitor) readConfiguration(ctx context.Context) (*api.ProcessConfiguration, []byte, error) {
	file, err := os.Open(monitor.configFile)
	if err != nil {
		monitor.logger.Error(err, "Error reading monitor config file", "monitorConfigPath", monitor.configFile)
		return nil, nil, nil
	}
	defer func() {
		err := file.Close()
		if err != nil {
			monitor.logger.Error(err, "Error could not close file", "monitorConfigPath", monitor.configFile)
		}
	}()
	configuration := &api.ProcessConfiguration{}
	configurationBytes, err := io.ReadAll(file)
	if err != nil {
		monitor.logger.Error(err, "Error reading monitor configuration", "monitorConfigPath", monitor.configFile)
	}
	err = json.Unmarshal(configurationBytes, configuration)
	if err != nil {
		monitor.logger.Error(err, "Error parsing monitor configuration", "rawConfiguration", string(configurationBytes))
		return nil, nil, nil
	}

	if configuration.Version == nil {
		monitor.logger.Error(err, "Error could not parse configured version", "rawConfiguration", string(configurationBytes))
		return nil, nil, nil
	}

	// If the versions are protocol compatible don't try to point to another binary path. Otherwise, the processes
	// cannot restart when a process crashes during a patch upgrade. This is skipped if the configuration explicitly
	// requests the shared binary.
	if !configuration.ShouldUseSharedBinary() && monitor.currentContainerVersion.IsProtocolCompatible(*configuration.Version) {
		configuration.BinaryPath = fdbserverPath
	} else {
		configuration.BinaryPath = path.Join(sharedBinaryDir, configuration.Version.String(), "fdbserver")
	}

	err = checkOwnerExecutable(configuration.BinaryPath)
	if err != nil {
		monitor.logger.Error(err, "Error with binary path for latest configuration", "configuration", configuration, "binaryPath", configuration.BinaryPath, "error", err.Error())
		return nil, nil, fmt.Errorf("%w: %w", errBinaryNotReady, err)
	}

	err = monitor.updateCustomEnvironmentFromNodeMetadata(ctx)
	if err != nil {
		monitor.logger.Error(err, "Error updating custom environment from node metadata", "configuration", configuration, "binaryPath", configuration.BinaryPath)
		return nil, nil, nil
	}
	_, err = configuration.GenerateArguments(1, monitor.copyCustomEnvironment())
	if err != nil {
		monitor.logger.Error(err, "Error generating arguments for latest configuration", "configuration", configuration, "binaryPath", configuration.BinaryPath)
		return nil, nil, nil
	}

	if configuration.ShouldRunServers() {
		// In case that the process is isolated we don't want to start the servers and we should terminate the running fdbserver
		// instances.
		if monitor.processIsIsolated(ctx) {
			configuration.RunServers = pointer.Bool(false)
		}
	}

	return configuration, configurationBytes, nil
}

// loadConfiguration loads the latest configuration from the config file. If the binary of the configuration is not
// ready, the load is retried with a quadratic backoff until it succeeds or the next call supersedes it.
func (monitor *monitor) loadConfiguration(ctx context.Context) {
	// Any pending retry is stale once a new load is triggered, since the load below reads the latest file anyway.
	monitor.cancelBinaryRetry()

	err := monitor.tryLoadConfiguration(ctx)
	if errors.Is(err, errBinaryNotReady) {
		monitor.startBinaryRetry(ctx)
	}
}

// tryLoadConfiguration performs a single attempt to load and accept the latest configuration. It only returns an error
// wrapping errBinaryNotReady, other failures are logged and require a new configuration to resolve.
func (monitor *monitor) tryLoadConfiguration(ctx context.Context) error {
	configuration, configurationBytes, err := monitor.readConfiguration(ctx)
	if err != nil {
		return err
	}

	if configuration == nil || len(configurationBytes) == 0 {
		return nil
	}

	// acceptConfiguration re-checks ctx under the mutex, which is what makes skipping a superseded retry reliable.
	if !monitor.acceptConfiguration(ctx, configuration, configurationBytes) {
		return nil
	}

	// Always update the annotations if needed to handle cases where the fdb-kubernetes-monitor
	// has loaded the new configuration but was not able to update the annotations.
	err = monitor.podClient.updateAnnotations(ctx, monitor)
	if err != nil {
		monitor.logger.Error(err, "Error updating pod annotations")
	}

	return nil
}

// cancelBinaryRetry stops the running binary retry, if any.
func (monitor *monitor) cancelBinaryRetry() {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	if monitor.binaryRetryCancel != nil {
		monitor.binaryRetryCancel()
		monitor.binaryRetryCancel = nil
	}
}

// startBinaryRetry retries loading the configuration in the background until the binary is ready, a new
// loadConfiguration call cancels it, or ctx is done. Each attempt re-reads the config file, so a newer configuration is
// picked up as well.
func (monitor *monitor) startBinaryRetry(ctx context.Context) {
	retryCtx, cancel := context.WithCancel(ctx)

	monitor.mutex.Lock()
	if monitor.binaryRetryCancel != nil {
		monitor.binaryRetryCancel()
	}
	monitor.binaryRetryCancel = cancel
	monitor.mutex.Unlock()

	backoff := monitor.binaryRetryBackoff
	if backoff == nil {
		backoff = getBinaryRetryBackoff
	}

	go func() {
		defer cancel()

		for attempt := 1; ; attempt++ {
			timer := time.NewTimer(backoff(attempt))
			select {
			case <-retryCtx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}

			monitor.logger.Info("Retrying to load configuration with unready binary", "attempt", attempt)
			err := monitor.tryLoadConfiguration(retryCtx)
			if errors.Is(err, errBinaryNotReady) {
				continue
			}

			return
		}
	}()
}

// getBinaryRetryBackoff returns the wait time before the given binary retry attempt, starting at 1. It uses
// getBackoffDuration but never returns less than a second.
func getBinaryRetryBackoff(attempt int) time.Duration {
	return max(getBackoffDuration(attempt), time.Second)
}

// checkOwnerExecutable validates that a path is a non-empty file that exists and is
// executable by its owner.
func checkOwnerExecutable(path string) error {
	binaryStat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if binaryStat.Size() == 0 {
		return fmt.Errorf("binary is empty")
	}
	if binaryStat.Mode()&0o100 == 0 {
		return fmt.Errorf("binary is not executable")
	}
	return nil
}

// acceptConfiguration is called when the monitor process parses and accepts
// a configuration from the local config file. It returns false without applying the configuration if ctx is done.
// cancelBinaryRetry cancels under the same mutex, so a superseded retry can't apply its configuration after the
// load that superseded it started.
func (monitor *monitor) acceptConfiguration(ctx context.Context, configuration *api.ProcessConfiguration, configurationBytes []byte) bool {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	if ctx.Err() != nil {
		return false
	}

	// If the configuration hasn't changed ignore those events to prevent noisy logging.
	if equality.Semantic.DeepEqual(monitor.activeConfiguration, configuration) {
		return true
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
		monitor.signalProcesses(syscall.SIGTERM, monitor.processIDs)
	}

	return true
}

// getBackoffDuration returns the backoff duration. The backoff time will increase quadratically with a maximum of 60 seconds.
func getBackoffDuration(errorCounter int) time.Duration {
	return min(time.Duration(errorCounter*errorCounter)*time.Second, maxErrorBackoffDuration)
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

		monitor.mutex.Lock()
		config := monitor.activeConfiguration
		monitor.mutex.Unlock()
		customEnv := monitor.copyCustomEnvironment()

		arguments, err := config.GenerateArguments(processNumber, customEnv)
		if err != nil {
			backoffDuration := getBackoffDuration(errorCounter)
			logger.Error(err, "Error generating arguments for subprocess", "configuration", config, "errorCounter", errorCounter, "backoffDuration", backoffDuration.String())
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
		monitor.metrics.registerProcessStartup(processNumber, config.Version.String())

		if cmd.Process != nil {
			pid = cmd.Process.Pid
		} else {
			logger.Error(nil, "No Process information available for subprocess")
		}

		startTime = time.Now()
		logger.Info("Subprocess started", "PID", pid)

		monitor.updateProcessID(processNumber, pid)
		monitor.setProcessVersion(processNumber, config.Version, config.BinaryPath, startTime)
		monitor.metrics.registerRunningBinary(processNumber, config.Version.String(), config.BinaryPath)
		// Once the last process has switched to the desired version no bounce is pending and the binaries of all
		// other versions are unused.
		monitor.pruneSharedBinaries()

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
		monitor.setProcessVersion(processNumber, nil, "", time.Time{})
		monitor.metrics.unregisterRunningBinary(processNumber)

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
func (monitor *monitor) processIsIsolated(ctx context.Context) bool {
	podMeta, err := monitor.podClient.getPodMetadata(ctx)
	if err != nil {
		return false
	}

	if podMeta == nil {
		return false
	}

	if podMeta.Annotations == nil {
		return false
	}

	val, ok := podMeta.Annotations[api.IsolateProcessGroupAnnotation]
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

// addWatcher adds a watch for the provided file. If an error is returned it will be retried for a maximum of 10 retries.
// The delay between the retries increases with each retry and the maximum delay is 1 second.
func addWatcher(fileName string, watcher *fsnotify.Watcher) error {
	var retryCnt int
	maxRetryCnt := 10

	baseDelay := 100 * time.Millisecond
	maxDelay := 1 * time.Second

	var err error
	for retryCnt < maxRetryCnt {
		err = watcher.Add(fileName)
		if err == nil {
			return nil
		}

		retryCnt++

		// Double the delay each attempt (100ms → 200ms → 400ms → …)
		delay := min(baseDelay*(1<<retryCnt), maxDelay)
		// Add jitter to avoid thundering herd
		delay += time.Duration(rand.Int64N(int64(delay) / 2))

		time.Sleep(delay)
	}

	return err
}

// watchConfiguration detects changes to the monitor configuration file.
func (monitor *monitor) watchConfiguration(ctx context.Context, watcher *fsnotify.Watcher) {
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return
			}

			monitor.logger.Info("Detected event on monitor conf file or cluster file", "event", event)
			if event.Op&fsnotify.Write == fsnotify.Write || event.Op&fsnotify.Create == fsnotify.Create {
				monitor.handleFileChange(ctx, event.Name)
			} else if event.Op&fsnotify.Remove == fsnotify.Remove {
				err := addWatcher(event.Name, watcher)
				if err != nil {
					panic(err)
				}
				monitor.handleFileChange(ctx, event.Name)
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				return
			}
			monitor.logger.Error(err, "Error watching for file system events")
		// Stop watching if the context is shutdown.
		case <-ctx.Done():
			return
		}
	}
}

// handleFileChange will perform the required action based on the changed/modified file.
func (monitor *monitor) handleFileChange(ctx context.Context, changedFile string) {
	if changedFile == fdbClusterFilePath {
		err := monitor.podClient.updateFdbClusterTimestampAnnotation(ctx)
		if err != nil {
			monitor.logger.Error(err, fmt.Sprintf("could not update %s annotation", api.ClusterFileChangeDetectedAnnotation))
		}
		return
	}

	monitor.loadConfiguration(ctx)
}

// signalProcesses sends the given signal to all processes in the provided slice.
// Callers that already hold monitor.mutex should pass monitor.processIDs directly.
// Callers that do not hold the mutex should snapshot processIDs under the mutex first.
func (monitor *monitor) signalProcesses(signal os.Signal, processIDs []int) {
	for processNumber, processID := range processIDs {
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
func (monitor *monitor) run(ctx context.Context) {
	done := make(chan bool, 1)

	go func() {
		<-ctx.Done()
		monitor.logger.Info("Received system signal")

		var delayShutdown time.Duration
		// We cannot use ctx here as the context as cancelled.
		podMeta, err := monitor.podClient.getPodMetadata(context.Background())
		if err != nil {
			monitor.logger.Error(err, "Could not read pod metadata for delayed shutdown")
		}

		if podMeta != nil {
			if delayValue, ok := podMeta.Annotations[api.DelayShutdownAnnotation]; ok {
				if delay, err := time.ParseDuration(delayValue); err == nil {
					delayShutdown = delay
				}
			}
		}

		// Reset processCount and snapshot processIDs under the mutex, then signal
		// outside the lock to avoid holding it during OS calls.
		monitor.mutex.Lock()
		monitor.processCount = 0
		ids := make([]int, len(monitor.processIDs))
		copy(ids, monitor.processIDs)
		monitor.mutex.Unlock()
		monitor.signalProcesses(syscall.SIGTERM, ids)

		time.Sleep(delayShutdown)

		done <- true
	}()

	monitor.loadConfiguration(ctx)
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
		err = watcher.Close()
		if err != nil {
			monitor.logger.Error(err, "could not close watcher")
		}
	}(watcher)
	go func() { monitor.watchConfiguration(ctx, watcher) }()

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
func (monitor *monitor) watchPodTimestamps(ctx context.Context) {
	for {
		select {
		case timestamp, ok := <-monitor.podClient.TimestampFeed:
			if !ok {
				return
			}
			if timestamp > monitor.lastConfigurationTime.Unix() {
				monitor.loadConfiguration(ctx)
			}
		case <-ctx.Done():
			return
		}
	}
}

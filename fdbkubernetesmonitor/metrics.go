// metrics.go
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

package main

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	// versionLabel represents the version label for the prometheus metrics.
	versionLabel = "version"
	// processLabel represents the process label for the prometheus metrics.
	processLabel = "process"
	// namespace is the prometheus namespace for the metrics
	prometheusNamespace = "fdbkubernetesmonitor"
	// restartCountMetricName represents the name of the restart metric.
	restartCountMetricName = "restart_count"
	// configurationChangeCountMetricName represents the configuration_change_count metric.
	configurationChangeCountMetricName = "configuration_change_count"
	// lastAppliedConfigurationTimestampMetricName represents the last_applied_configuration_timestamp metric.
	lastAppliedConfigurationTimestampMetricName = "last_applied_configuration_timestamp"
	// startTimestampMetricName represents the start_timestamp metric.
	startTimestampMetricName = "start_timestamp"
	// runningVersionMetricName represents the running_version metric.
	runningVersionMetricName = "running_version"
	// binaryPathLabel represents the binary path label for the prometheus metrics.
	binaryPathLabel = "binary_path"
	// runningBinaryMetricName represents the running_binary metric.
	runningBinaryMetricName = "running_binary"
	// desiredVersionMetricName represents the desired_version metric.
	desiredVersionMetricName = "desired_version"
	// stagedVersionMetricName represents the staged_version metric.
	stagedVersionMetricName = "staged_version"
)

// metrics represents the custom prometheus metrics for the monitor.
type metrics struct {
	// restartCount represents the total number of fdbserver process restarts.
	restartCount *prometheus.CounterVec
	// configurationChangeCount represents the total number of observed configuration changes.
	configurationChangeCount prometheus.Counter
	// lastAppliedConfigurationTimestamp provides a unix timestamp when the last configuration was applied.
	lastAppliedConfigurationTimestamp prometheus.Gauge
	// startTimestamp represents the timestamp when a specific process was started.
	startTimestamp *prometheus.GaugeVec
	// runningVersion represents the current running version of the binaries.
	runningVersion *prometheus.GaugeVec
	// desiredVersion represents the desired running version of the binaries.
	desiredVersion *prometheus.GaugeVec
	// runningBinary reports the version and binary path each running fdbserver process was started from. The value is
	// always 1 and the series of a process is removed once the process terminated.
	runningBinary *prometheus.GaugeVec
	// runningBinaryMutex guards previousRunningBinaries.
	runningBinaryMutex sync.Mutex
	// previousRunningBinaries keeps the label values that were reported for each process.
	previousRunningBinaries map[int]prometheus.Labels
	// stagedVersion represents the versions with a ready binary in the shared binary directory.
	stagedVersion *prometheus.GaugeVec
	// stagedVersionMutex guards previousStagedVersions.
	stagedVersionMutex sync.Mutex
	// previousStagedVersions keeps the staged versions that were reported last.
	previousStagedVersions map[string]struct{}
	// runningVersionMutex guards previousRunningVersion, as the processes are started concurrently.
	runningVersionMutex sync.Mutex
	// previousDesiredVersion keeps the previous seen desired version.
	previousDesiredVersion string
	// previousRunningVersion keeps the prvious seen running version.
	previousRunningVersion string
}

// registerConfigurationChange will update the current prometheus metrics related to configuration changes.
// This method doesn't use latch as it will only be called from acceptConfiguration which already uses a mutex.
func (metrics *metrics) registerConfigurationChange(version string) {
	// Update the prometheus metrics.
	metrics.lastAppliedConfigurationTimestamp.SetToCurrentTime()
	metrics.configurationChangeCount.Inc()
	// Update the desired version for the fdbserver process.
	metrics.desiredVersion.With(prometheus.Labels{versionLabel: version}).Set(1.0)
	// If we had a different desired version before we want to make sure to reset this value.
	if metrics.previousDesiredVersion != "" && metrics.previousDesiredVersion != version {
		metrics.desiredVersion.With(prometheus.Labels{versionLabel: metrics.previousDesiredVersion}).Set(0.0)
	}
	metrics.previousDesiredVersion = version
}

// registerProcessStartup will update the current prometheus metrics related to process startup and running versions.
func (metrics *metrics) registerProcessStartup(processNumber int, version string) {
	metrics.runningVersionMutex.Lock()
	defer metrics.runningVersionMutex.Unlock()

	castedProcessNumber := strconv.Itoa(processNumber)
	metrics.restartCount.With(prometheus.Labels{processLabel: castedProcessNumber}).Inc()
	metrics.startTimestamp.With(prometheus.Labels{processLabel: castedProcessNumber}).SetToCurrentTime()
	metrics.runningVersion.With(prometheus.Labels{versionLabel: version}).Set(1.0)
	// If we had a different desired version before we want to make sure to reset this value.
	if metrics.previousRunningVersion != "" && metrics.previousRunningVersion != version {
		metrics.runningVersion.With(prometheus.Labels{versionLabel: metrics.previousRunningVersion}).Set(0.0)
	}
	metrics.previousRunningVersion = version
}

// registerRunningBinary sets the running_binary metric for the process and removes the series of the binary the process
// was started from before.
func (metrics *metrics) registerRunningBinary(processNumber int, version string, binaryPath string) {
	metrics.runningBinaryMutex.Lock()
	defer metrics.runningBinaryMutex.Unlock()

	metrics.deleteRunningBinary(processNumber)
	labels := prometheus.Labels{
		processLabel:    strconv.Itoa(processNumber),
		versionLabel:    version,
		binaryPathLabel: binaryPath,
	}
	metrics.runningBinary.With(labels).Set(1.0)
	if metrics.previousRunningBinaries == nil {
		metrics.previousRunningBinaries = make(map[int]prometheus.Labels)
	}

	metrics.previousRunningBinaries[processNumber] = labels
}

// unregisterRunningBinary removes the running_binary series of a terminated process.
func (metrics *metrics) unregisterRunningBinary(processNumber int) {
	metrics.runningBinaryMutex.Lock()
	defer metrics.runningBinaryMutex.Unlock()

	metrics.deleteRunningBinary(processNumber)
}

// deleteRunningBinary requires runningBinaryMutex to be held.
func (metrics *metrics) deleteRunningBinary(processNumber int) {
	if labels, present := metrics.previousRunningBinaries[processNumber]; present {
		metrics.runningBinary.Delete(labels)
		delete(metrics.previousRunningBinaries, processNumber)
	}
}

// registerStagedVersions sets the staged_version metric for the provided versions and removes the series of versions
// that are no longer staged.
func (metrics *metrics) registerStagedVersions(versions []string) {
	metrics.stagedVersionMutex.Lock()
	defer metrics.stagedVersionMutex.Unlock()

	current := make(map[string]struct{}, len(versions))
	for _, version := range versions {
		current[version] = struct{}{}
		metrics.stagedVersion.With(prometheus.Labels{versionLabel: version}).Set(1.0)
	}

	for version := range metrics.previousStagedVersions {
		if _, present := current[version]; !present {
			metrics.stagedVersion.DeleteLabelValues(version)
		}
	}

	metrics.previousStagedVersions = current
}

// registerMetrics will register the monitor metrics and returns a metrics struct to update the current metrics.
func registerMetrics(reg prometheus.Registerer) *metrics {
	monitorMetrics := &metrics{
		restartCount: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: prometheusNamespace,
				Name:      restartCountMetricName,
				Help:      "Number of fdbserver process restarts in total.",
			}, []string{processLabel}),
		configurationChangeCount: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: prometheusNamespace,
			Name:      configurationChangeCountMetricName,
			Help:      "Number of observed configuration changes.",
		}),
		lastAppliedConfigurationTimestamp: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: prometheusNamespace,
			Name:      lastAppliedConfigurationTimestampMetricName,
			Help:      "Timestamp when the last time the configuration was applied.",
		}),
		startTimestamp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: prometheusNamespace,
			Name:      startTimestampMetricName,
			Help:      "Timestamp when the last time the configuration was applied.",
		}, []string{processLabel}),
		runningVersion: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: prometheusNamespace,
			Name:      runningVersionMetricName,
			Help:      "The current running version of the fdbserver processes started by this monitor.",
		}, []string{versionLabel}),
		desiredVersion: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: prometheusNamespace,
			Name:      desiredVersionMetricName,
			Help:      "The desired running version of the fdbserver processes started by this monitor.",
		}, []string{versionLabel}),
		runningBinary: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: prometheusNamespace,
			Name:      runningBinaryMetricName,
			Help:      "The version and binary path each running fdbserver process was started from.",
		}, []string{processLabel, versionLabel, binaryPathLabel}),
		stagedVersion: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: prometheusNamespace,
			Name:      stagedVersionMetricName,
			Help:      "The versions with a ready fdbserver binary in the shared binary directory.",
		}, []string{versionLabel}),
	}

	reg.MustRegister(monitorMetrics.restartCount)
	reg.MustRegister(monitorMetrics.configurationChangeCount)
	reg.MustRegister(monitorMetrics.lastAppliedConfigurationTimestamp)
	reg.MustRegister(monitorMetrics.startTimestamp)
	reg.MustRegister(monitorMetrics.runningVersion)
	reg.MustRegister(monitorMetrics.desiredVersion)
	reg.MustRegister(monitorMetrics.runningBinary)
	reg.MustRegister(monitorMetrics.stagedVersion)

	return monitorMetrics
}

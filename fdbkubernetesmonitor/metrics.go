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
	// desiredVersionMetricName represents the desired_version metric.
	desiredVersionMetricName = "desired_version"
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
	}

	reg.MustRegister(monitorMetrics.restartCount)
	reg.MustRegister(monitorMetrics.configurationChangeCount)
	reg.MustRegister(monitorMetrics.lastAppliedConfigurationTimestamp)
	reg.MustRegister(monitorMetrics.startTimestamp)
	reg.MustRegister(monitorMetrics.runningVersion)
	reg.MustRegister(monitorMetrics.desiredVersion)

	return monitorMetrics
}

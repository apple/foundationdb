// metrics_test.go
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
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/utils/pointer"
)

var _ = Describe("Testing monitor metrics", func() {
	When("getting the copy details", func() {
		sevenOneVersion := "7.1.57"
		sevenThreeVersion := "7.3.37"
		var registry *prometheus.Registry
		var monitorMetrics *metrics

		BeforeEach(func() {
			registry = prometheus.NewRegistry()
			monitorMetrics = registerMetrics(registry)
		})

		It("shouldn't throw any error", func() {
			Expect(monitorMetrics).NotTo(BeNil())
		})

		When("no metrics are added", func() {
			It("only the counters should be setup", func() {
				metrics, err := registry.Gather()
				Expect(err).NotTo(HaveOccurred())
				Expect(metrics).To(HaveLen(2))
			})
		})

		When("a configuration change is registered", func() {
			BeforeEach(func() {
				monitorMetrics.registerConfigurationChange(sevenOneVersion)
			})

			It("should update the metrics", func() {
				metrics, err := registry.Gather()
				Expect(err).NotTo(HaveOccurred())
				Expect(metrics).To(HaveLen(3))

				for _, metric := range metrics {
					Expect(metric.Metric).To(HaveLen(1))
					name := pointer.StringDeref(metric.Name, "")
					if strings.HasSuffix(name, configurationChangeCountMetricName) {
						Expect(*metric.Metric[0].Counter.Value).To(BeNumerically("==", 1))
						continue
					}
					if strings.HasSuffix(name, desiredVersionMetricName) {
						Expect(*metric.Metric[0].Gauge.Value).To(BeNumerically("==", 1))
						Expect(metric.Metric[0].Label).To(HaveLen(1))
						Expect(*metric.Metric[0].Label[0].Name).To(Equal(versionLabel))
						Expect(*metric.Metric[0].Label[0].Value).To(Equal(sevenOneVersion))
						continue
					}
				}
			})

			When("a new configuration change is registered for the same version", func() {
				BeforeEach(func() {
					monitorMetrics.registerConfigurationChange(sevenOneVersion)
				})

				It("should update the metrics", func() {
					metrics, err := registry.Gather()
					Expect(err).NotTo(HaveOccurred())
					Expect(metrics).To(HaveLen(3))

					for _, metric := range metrics {
						Expect(metric.Metric).To(HaveLen(1))
						name := pointer.StringDeref(metric.Name, "")
						if strings.HasSuffix(name, configurationChangeCountMetricName) {
							Expect(*metric.Metric[0].Counter.Value).To(BeNumerically("==", 2))
							continue
						}
						if strings.HasSuffix(name, desiredVersionMetricName) {
							Expect(*metric.Metric[0].Gauge.Value).To(BeNumerically("==", 1))
							Expect(metric.Metric[0].Label).To(HaveLen(1))
							Expect(*metric.Metric[0].Label[0].Name).To(Equal(versionLabel))
							Expect(*metric.Metric[0].Label[0].Value).To(Equal(sevenOneVersion))
							continue
						}
					}
				})
			})

			When("a new configuration change is registered for a different version", func() {
				BeforeEach(func() {
					monitorMetrics.registerConfigurationChange(sevenThreeVersion)
				})

				It("should update the metrics", func() {
					metrics, err := registry.Gather()
					Expect(err).NotTo(HaveOccurred())
					Expect(metrics).To(HaveLen(3))

					for _, metric := range metrics {
						name := pointer.StringDeref(metric.Name, "")
						if strings.HasSuffix(name, desiredVersionMetricName) {
							Expect(metric.Metric).To(HaveLen(2))
							for _, versionMetric := range metric.Metric {
								Expect(versionMetric.Label).To(HaveLen(1))
								Expect(*versionMetric.Label[0].Name).To(Equal(versionLabel))
								expected := 0
								if *versionMetric.Label[0].Value == sevenThreeVersion {
									expected = 1
								}
								Expect(*versionMetric.Gauge.Value).To(BeNumerically("==", expected))
							}
							continue
						}

						Expect(metric.Metric).To(HaveLen(1))
						if strings.HasSuffix(name, configurationChangeCountMetricName) {
							Expect(*metric.Metric[0].Counter.Value).To(BeNumerically("==", 2))
							continue
						}
					}
				})
			})
		})

		When("a process startup registered", func() {
			BeforeEach(func() {
				monitorMetrics.registerProcessStartup(1, sevenOneVersion)
			})

			It("should update the metrics", func() {
				metrics, err := registry.Gather()
				Expect(err).NotTo(HaveOccurred())
				Expect(metrics).To(HaveLen(5))

				for _, metric := range metrics {
					Expect(metric.Metric).To(HaveLen(1))
					name := pointer.StringDeref(metric.Name, "")
					if strings.HasSuffix(name, restartCountMetricName) {
						Expect(*metric.Metric[0].Counter.Value).To(BeNumerically("==", 1))
						Expect(metric.Metric[0].Label).To(HaveLen(1))
						Expect(*metric.Metric[0].Label[0].Name).To(Equal(processLabel))
						Expect(*metric.Metric[0].Label[0].Value).To(Equal("1"))
						continue
					}
					if strings.HasSuffix(name, runningVersionMetricName) {
						Expect(*metric.Metric[0].Gauge.Value).To(BeNumerically("==", 1))
						Expect(metric.Metric[0].Label).To(HaveLen(1))
						Expect(*metric.Metric[0].Label[0].Name).To(Equal(versionLabel))
						Expect(*metric.Metric[0].Label[0].Value).To(Equal(sevenOneVersion))
						continue
					}
				}
			})

			When("the same process is restarted with the same version", func() {
				BeforeEach(func() {
					monitorMetrics.registerProcessStartup(1, sevenOneVersion)
				})

				It("should update the metrics", func() {
					metrics, err := registry.Gather()
					Expect(err).NotTo(HaveOccurred())
					Expect(metrics).To(HaveLen(5))

					for _, metric := range metrics {
						Expect(metric.Metric).To(HaveLen(1))
						name := pointer.StringDeref(metric.Name, "")
						if strings.HasSuffix(name, restartCountMetricName) {
							Expect(*metric.Metric[0].Counter.Value).To(BeNumerically("==", 2))
							Expect(metric.Metric[0].Label).To(HaveLen(1))
							Expect(*metric.Metric[0].Label[0].Name).To(Equal(processLabel))
							Expect(*metric.Metric[0].Label[0].Value).To(Equal("1"))
							continue
						}
						if strings.HasSuffix(name, runningVersionMetricName) {
							Expect(*metric.Metric[0].Gauge.Value).To(BeNumerically("==", 1))
							Expect(metric.Metric[0].Label).To(HaveLen(1))
							Expect(*metric.Metric[0].Label[0].Name).To(Equal(versionLabel))
							Expect(*metric.Metric[0].Label[0].Value).To(Equal(sevenOneVersion))
							continue
						}
					}
				})
			})

			When("the same process is restarted with a different version", func() {
				BeforeEach(func() {
					monitorMetrics.registerProcessStartup(1, sevenThreeVersion)
				})

				It("should update the metrics", func() {
					metrics, err := registry.Gather()
					Expect(err).NotTo(HaveOccurred())
					Expect(metrics).To(HaveLen(5))

					for _, metric := range metrics {
						name := pointer.StringDeref(metric.Name, "")
						if strings.HasSuffix(name, runningVersionMetricName) {
							Expect(metric.Metric).To(HaveLen(2))
							for _, versionMetric := range metric.Metric {
								Expect(versionMetric.Label).To(HaveLen(1))
								Expect(*versionMetric.Label[0].Name).To(Equal(versionLabel))
								expected := 0
								if *versionMetric.Label[0].Value == sevenThreeVersion {
									expected = 1
								}
								Expect(*versionMetric.Gauge.Value).To(BeNumerically("==", expected))
							}
							continue
						}

						Expect(metric.Metric).To(HaveLen(1))
						if strings.HasSuffix(name, restartCountMetricName) {
							Expect(*metric.Metric[0].Counter.Value).To(BeNumerically("==", 2))
							Expect(metric.Metric[0].Label).To(HaveLen(1))
							Expect(*metric.Metric[0].Label[0].Name).To(Equal(processLabel))
							Expect(*metric.Metric[0].Label[0].Value).To(Equal("1"))
							continue
						}
					}
				})
			})

			When("a new process is restarted with a the same version", func() {
				BeforeEach(func() {
					monitorMetrics.registerProcessStartup(2, sevenOneVersion)
				})

				It("should update the metrics", func() {
					metrics, err := registry.Gather()
					Expect(err).NotTo(HaveOccurred())
					Expect(metrics).To(HaveLen(5))

					for _, metric := range metrics {
						name := pointer.StringDeref(metric.Name, "")

						if strings.HasSuffix(name, restartCountMetricName) {
							Expect(metric.Metric).To(HaveLen(2))
							for _, versionMetric := range metric.Metric {
								Expect(versionMetric.Label).To(HaveLen(1))
								Expect(*versionMetric.Label[0].Name).To(Equal(processLabel))
								Expect(*versionMetric.Counter.Value).To(BeNumerically("==", 1))
							}
							continue
						}
					}
				})
			})
		})
	})
})

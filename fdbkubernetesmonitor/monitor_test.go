// monitor_test.go
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
	"os"
	"path"
	"time"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"k8s.io/apimachinery/pkg/util/json"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Testing FDB Kubernetes monitor", func() {
	When("updating the custom environment variables from the node metadata", func() {
		var mon *monitor

		BeforeEach(func() {
			mon = &monitor{
				logger: GinkgoLogr,
				customEnvironment: map[string]string{
					"testing": "testing",
				},
				podClient: &kubernetesClient{},
			}
		})

		JustBeforeEach(func() {
			mon.updateCustomEnvironmentFromNodeMetadata()
		})

		When("no node metadata is present", func() {
			It("shouldn't add new entries", func() {
				Expect(mon.customEnvironment).To(HaveLen(1))
				Expect(mon.customEnvironment).To(HaveKeyWithValue("testing", "testing"))
			})
		})

		When("node metadata is present", func() {
			BeforeEach(func() {
				mon.podClient.nodeMetadata = &metav1.PartialObjectMetadata{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							// Examples are taken from: https://kubernetes.io/docs/reference/labels-annotations-taints/
							"topology.kubernetes.io/zone": "us-east-1c",
							"kubernetes.io/hostname":      "ip-172-20-114-199.ec2.internal",
						},
					},
				}
			})

			It("should add the new entries", func() {
				Expect(mon.customEnvironment).To(HaveLen(3))
				Expect(mon.customEnvironment).To(HaveKeyWithValue("testing", "testing"))
				Expect(mon.customEnvironment).To(HaveKeyWithValue("NODE_LABEL_TOPOLOGY_KUBERNETES_IO_ZONE", "us-east-1c"))
				Expect(mon.customEnvironment).To(HaveKeyWithValue("NODE_LABEL_KUBERNETES_IO_HOSTNAME", "ip-172-20-114-199.ec2.internal"))
			})
		})
	})

	DescribeTable("when getting the backoff time", func(errorCount int, expected time.Duration) {
		Expect(getBackoffDuration(errorCount)).To(Equal(expected))
	},
		Entry("no errors have occurred",
			0,
			time.Duration(0),
		),
		Entry("one error have occurred",
			1,
			1*time.Second,
		),
		Entry("two errors have occurred",
			2,
			4*time.Second,
		),
		Entry("three errors have occurred",
			3,
			9*time.Second,
		),
		Entry("ten errors have occurred, should return the max backoff seconds",
			100,
			60*time.Second,
		),
	)

	When("reading the configuration file", func() {
		var mon *monitor
		var configurationFilePath string
		var configuration *api.ProcessConfiguration
		var configurationBytes []byte
		defaultVersion := api.Version{Major: 7, Minor: 1, Patch: 51}

		BeforeEach(func() {
			tmpDir := GinkgoT().TempDir()
			// Set the fdbserverPath to make sure the executable check works.
			fdbserverPath = path.Join(tmpDir, "fdberver")
			Expect(os.WriteFile(fdbserverPath, []byte(""), 0700)).NotTo(HaveOccurred())
			configurationFilePath = path.Join(tmpDir, "config.json")
			mon = &monitor{
				logger:                  GinkgoLogr,
				configFile:              configurationFilePath,
				customEnvironment:       map[string]string{},
				podClient:               &kubernetesClient{},
				currentContainerVersion: defaultVersion,
			}
		})

		JustBeforeEach(func() {
			configuration, configurationBytes = mon.readConfiguration()
		})

		When("the configuration file is empty", func() {
			BeforeEach(func() {
				Expect(os.WriteFile(configurationFilePath, []byte("{}"), 0600)).NotTo(HaveOccurred())
			})

			It("should return an empty configuration", func() {
				Expect(configuration).To(BeNil())
				Expect(configurationBytes).To(BeNil())
			})
		})

		When("the configuration is valid", func() {
			var out []byte

			BeforeEach(func() {
				config := &api.ProcessConfiguration{
					Version: &defaultVersion,
				}

				var err error
				out, err = json.Marshal(config)
				Expect(err).NotTo(HaveOccurred())
				Expect(os.WriteFile(configurationFilePath, out, 0600)).NotTo(HaveOccurred())
			})

			It("should return the configuration", func() {
				Expect(configuration).NotTo(BeNil())
				Expect(configuration.Version).To(Equal(&defaultVersion))
				Expect(configuration.BinaryPath).To(Equal(fdbserverPath))
				Expect(configuration.RunServers).To(BeNil())
				Expect(configuration.Arguments).To(BeEmpty())
				Expect(configurationBytes).To(Equal(out))
			})

			When("the pod has the isolate annotation set to true", func() {
				BeforeEach(func() {
					mon.podClient = &kubernetesClient{
						podMetadata: &metav1.PartialObjectMetadata{
							ObjectMeta: metav1.ObjectMeta{
								Annotations: map[string]string{
									api.IsolateProcessGroupAnnotation: "true",
								},
							},
						},
					}
				})

				It("should return the configuration with runServers set to false", func() {
					Expect(configuration).NotTo(BeNil())
					Expect(configuration.Version).To(Equal(&defaultVersion))
					Expect(configuration.BinaryPath).To(Equal(fdbserverPath))
					Expect(configuration.RunServers).NotTo(BeNil())
					Expect(*configuration.RunServers).To(BeFalse())
					Expect(configuration.Arguments).To(BeEmpty())
					Expect(configurationBytes).To(Equal(out))
				})
			})
		})
	})
})

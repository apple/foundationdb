// monitor_test.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2021-2026 Apple Inc. and the FoundationDB project authors
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
	"context"
	"os"
	"path"
	"time"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("Testing FDB Kubernetes monitor", func() {
	When("updating the custom environment variables from the node metadata", func() {
		var mon *monitor
		var environment map[string]string

		BeforeEach(func() {
			mon = &monitor{
				logger: GinkgoLogr,
				baseEnvironment: map[string]string{
					"testing": "testing",
				},
				podClient: &kubernetesClient{},
			}
		})

		JustBeforeEach(func() {
			var err error
			environment, err = mon.environmentFromNodeMetadata(context.Background())
			Expect(err).NotTo(HaveOccurred())
		})

		When("no node metadata is present", func() {
			It("shouldn't add new entries", func() {
				Expect(environment).To(HaveLen(1))
				Expect(environment).To(HaveKeyWithValue("testing", "testing"))
			})
		})

		When("node metadata is present", func() {
			BeforeEach(func() {
				mon.podClient.nodeName = "test-node"
				mon.podClient.Client = fake.NewFakeClient(&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Name: "test-node",
						Labels: map[string]string{
							// Examples are taken from: https://kubernetes.io/docs/reference/labels-annotations-taints/
							"topology.kubernetes.io/zone": "us-east-1c",
							"kubernetes.io/hostname":      "ip-172-20-114-199.ec2.internal",
						},
					},
				})
			})

			It("keeps previous snapshots stable and drops removed labels", func() {
				ctx := context.Background()
				node := &corev1.Node{}
				Expect(mon.podClient.Client.Get(ctx, client.ObjectKey{Name: mon.podClient.nodeName}, node)).To(Succeed())
				delete(node.Labels, "topology.kubernetes.io/zone")
				node.Labels["kubernetes.io/hostname"] = "replacement"
				Expect(mon.podClient.Client.Update(ctx, node)).To(Succeed())

				current, err := mon.environmentFromNodeMetadata(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(current).NotTo(HaveKey("NODE_LABEL_TOPOLOGY_KUBERNETES_IO_ZONE"))
				Expect(current).To(HaveKeyWithValue("NODE_LABEL_KUBERNETES_IO_HOSTNAME", "replacement"))
				Expect(environment).To(HaveKeyWithValue("NODE_LABEL_TOPOLOGY_KUBERNETES_IO_ZONE", "us-east-1c"))
				Expect(mon.baseEnvironment).To(Equal(map[string]string{"testing": "testing"}))
			})

			It("should add the new entries", func() {
				Expect(environment).To(HaveLen(3))
				Expect(environment).To(HaveKeyWithValue("testing", "testing"))
				Expect(environment).To(HaveKeyWithValue("NODE_LABEL_TOPOLOGY_KUBERNETES_IO_ZONE", "us-east-1c"))
				Expect(environment).To(HaveKeyWithValue("NODE_LABEL_KUBERNETES_IO_HOSTNAME", "ip-172-20-114-199.ec2.internal"))
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
				logger:          GinkgoLogr,
				configFile:      configurationFilePath,
				baseEnvironment: map[string]string{},
				podClient: &kubernetesClient{
					podName:   "test-pod",
					namespace: "test",
					Client: fake.NewFakeClient(&corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod",
							Namespace: "test",
						},
					}),
				},
				currentContainerVersion: defaultVersion,
			}
		})

		JustBeforeEach(func() {
			configuration, configurationBytes, _ = mon.readConfiguration(context.Background())
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
					Expect(mon.podClient.Client.Update(context.Background(), &corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "test-pod",
							Namespace: "test",
							Annotations: map[string]string{
								api.IsolateProcessGroupAnnotation: "true",
							},
						},
					})).To(Succeed())
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

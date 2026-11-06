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
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/json"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

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
			Expect(mon.updateCustomEnvironmentFromNodeMetadata(context.Background())).To(Succeed())
		})

		When("no node metadata is present", func() {
			It("shouldn't add new entries", func() {
				Expect(mon.customEnvironment).To(HaveLen(1))
				Expect(mon.customEnvironment).To(HaveKeyWithValue("testing", "testing"))
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
		var readErr error
		defaultVersion := api.Version{Major: 7, Minor: 1, Patch: 51}

		BeforeEach(func() {
			tmpDir := GinkgoT().TempDir()
			// Set the fdbserverPath to make sure the executable check works.
			fdbserverPath = path.Join(tmpDir, "fdberver")
			Expect(os.WriteFile(fdbserverPath, []byte("fdbserver"), 0700)).NotTo(HaveOccurred())
			configurationFilePath = path.Join(tmpDir, "config.json")
			mon = &monitor{
				logger:            GinkgoLogr,
				configFile:        configurationFilePath,
				customEnvironment: map[string]string{},
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
			configuration, configurationBytes, readErr = mon.readConfiguration(context.Background())
		})

		When("the configuration file is empty", func() {
			BeforeEach(func() {
				Expect(os.WriteFile(configurationFilePath, []byte("{}"), 0600)).NotTo(HaveOccurred())
			})

			It("should return an empty configuration", func() {
				Expect(configuration).To(BeNil())
				Expect(configurationBytes).To(BeNil())
				Expect(readErr).NotTo(HaveOccurred())
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

			When("the binary is missing", func() {
				BeforeEach(func() {
					Expect(os.Remove(fdbserverPath)).To(Succeed())
				})

				It("should return a retryable error", func() {
					Expect(configuration).To(BeNil())
					Expect(configurationBytes).To(BeNil())
					Expect(readErr).To(MatchError(errBinaryNotReady))
				})
			})

			When("the binary is empty", func() {
				BeforeEach(func() {
					Expect(os.WriteFile(fdbserverPath, nil, 0700)).To(Succeed())
				})

				It("should return a retryable error", func() {
					Expect(configuration).To(BeNil())
					Expect(readErr).To(MatchError(errBinaryNotReady))
				})
			})

			When("the binary is not executable", func() {
				BeforeEach(func() {
					Expect(os.Chmod(fdbserverPath, 0600)).To(Succeed())
				})

				It("should return a retryable error", func() {
					Expect(configuration).To(BeNil())
					Expect(readErr).To(MatchError(errBinaryNotReady))
				})
			})

			It("should return the configuration", func() {
				Expect(readErr).NotTo(HaveOccurred())
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
	DescribeTable("when checking if a binary is executable", func(content []byte, mode os.FileMode, create bool, expectedErr string) {
		binaryPath := path.Join(GinkgoT().TempDir(), "fdbserver")
		if create {
			Expect(os.WriteFile(binaryPath, content, mode)).To(Succeed())
			// Make sure the mode is not affected by the umask.
			Expect(os.Chmod(binaryPath, mode)).To(Succeed())
		}

		err := checkOwnerExecutable(binaryPath)
		if expectedErr == "" {
			Expect(err).NotTo(HaveOccurred())
			return
		}

		Expect(err).To(MatchError(ContainSubstring(expectedErr)))
	},
		Entry("the binary is valid", []byte("fdbserver"), os.FileMode(0700), true, ""),
		Entry("the binary is missing", nil, os.FileMode(0), false, "no such file"),
		Entry("the binary is empty", []byte{}, os.FileMode(0700), true, "binary is empty"),
		Entry("the binary is not executable", []byte("fdbserver"), os.FileMode(0600), true, "not executable"),
	)

	DescribeTable("when getting the binary retry backoff", func(attempt int, expected time.Duration) {
		Expect(getBinaryRetryBackoff(attempt)).To(Equal(expected))
	},
		Entry("first attempt is at least a second", 0, time.Second),
		Entry("one", 1, time.Second),
		Entry("two", 2, 4*time.Second),
		Entry("three", 3, 9*time.Second),
		Entry("capped at the max backoff", 100, maxErrorBackoffDuration),
	)

	When("the binary of the configuration is not ready", func() {
		var mon *monitor
		var ctx context.Context
		var cancel context.CancelFunc
		var configurationFilePath, binaryPath string
		var attempts atomic.Int32
		unreadyVersion := api.Version{Major: 7, Minor: 3, Patch: 0}
		currentVersion := api.Version{Major: 7, Minor: 1, Patch: 51}

		writeConfiguration := func(version api.Version) {
			out, err := json.Marshal(&api.ProcessConfiguration{Version: &version})
			Expect(err).NotTo(HaveOccurred())
			Expect(os.WriteFile(configurationFilePath, out, 0600)).To(Succeed())
		}

		activeVersion := func() string {
			mon.mutex.Lock()
			defer mon.mutex.Unlock()

			if mon.activeConfiguration == nil {
				return ""
			}

			return mon.activeConfiguration.Version.String()
		}

		hasRetry := func() bool {
			mon.mutex.Lock()
			defer mon.mutex.Unlock()

			return mon.binaryRetryCancel != nil
		}

		createBinary := func() {
			Expect(os.MkdirAll(path.Dir(binaryPath), 0700)).To(Succeed())
			Expect(os.WriteFile(binaryPath, []byte("fdbserver"), 0700)).To(Succeed())
		}

		BeforeEach(func() {
			ctx, cancel = context.WithCancel(context.Background())
			DeferCleanup(cancel)
			attempts.Store(0)

			tmpDir := GinkgoT().TempDir()
			fdbserverPath = path.Join(tmpDir, "fdbserver")
			Expect(os.WriteFile(fdbserverPath, []byte("fdbserver"), 0700)).To(Succeed())
			previousSharedBinaryDir := sharedBinaryDir
			sharedBinaryDir = path.Join(tmpDir, "shared")
			DeferCleanup(func() { sharedBinaryDir = previousSharedBinaryDir })
			binaryPath = path.Join(sharedBinaryDir, unreadyVersion.String(), "fdbserver")
			configurationFilePath = path.Join(tmpDir, "config.json")

			mon = &monitor{
				logger:            GinkgoLogr,
				configFile:        configurationFilePath,
				customEnvironment: map[string]string{},
				// No processes are started, this test only covers loading the configuration.
				processCount: 0,
				processIDs:   make([]int, 1),
				metrics:      registerMetrics(prometheus.NewRegistry()),
				podClient: &kubernetesClient{
					podName:   "test-pod",
					namespace: "test",
					Logger:    GinkgoLogr,
					Client: fake.NewFakeClient(&corev1.Pod{
						ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test"},
					}),
				},
				currentContainerVersion: currentVersion,
				binaryRetryBackoff: func(int) time.Duration {
					attempts.Add(1)
					return 10 * time.Millisecond
				},
			}
			Expect(currentVersion.IsProtocolCompatible(unreadyVersion)).To(BeFalse())
		})

		When("the binary appears later", func() {
			BeforeEach(func() {
				writeConfiguration(unreadyVersion)
				mon.loadConfiguration(ctx)
			})

			It("should retry until the configuration is accepted", func() {
				Expect(activeVersion()).To(BeEmpty())
				Eventually(attempts.Load).Should(BeNumerically(">=", 2))
				Expect(hasRetry()).To(BeTrue())

				createBinary()
				Eventually(activeVersion).Should(Equal(unreadyVersion.String()))
				mon.mutex.Lock()
				Expect(mon.activeConfiguration.BinaryPath).To(Equal(binaryPath))
				mon.mutex.Unlock()

				// The retry must stop once the configuration was accepted.
				Eventually(func() int32 {
					before := attempts.Load()
					time.Sleep(100 * time.Millisecond)
					return attempts.Load() - before
				}).Should(BeZero())
			})
		})

		When("a new configuration arrives while retrying", func() {
			BeforeEach(func() {
				writeConfiguration(unreadyVersion)
				mon.loadConfiguration(ctx)
				Eventually(attempts.Load).Should(BeNumerically(">=", 1))
				Expect(hasRetry()).To(BeTrue())
			})

			It("should cancel the retry and accept the new configuration", func() {
				writeConfiguration(currentVersion)
				mon.loadConfiguration(ctx)

				Expect(activeVersion()).To(Equal(currentVersion.String()))
				Expect(hasRetry()).To(BeFalse())

				// The superseded retry must not switch back to the old configuration.
				createBinary()
				Consistently(activeVersion, 300*time.Millisecond, 20*time.Millisecond).Should(Equal(currentVersion.String()))
			})

			It("should replace the retry if the binary is still not ready", func() {
				mon.loadConfiguration(ctx)

				Expect(hasRetry()).To(BeTrue())
				createBinary()
				Eventually(activeVersion).Should(Equal(unreadyVersion.String()))
			})
		})

		When("the context is cancelled while retrying", func() {
			It("should stop retrying", func() {
				writeConfiguration(unreadyVersion)
				mon.loadConfiguration(ctx)
				Eventually(attempts.Load).Should(BeNumerically(">=", 1))

				cancel()
				// Allow an in-flight attempt to finish.
				time.Sleep(100 * time.Millisecond)
				stopped := attempts.Load()
				createBinary()
				Consistently(attempts.Load, 300*time.Millisecond, 20*time.Millisecond).Should(Equal(stopped))
				Expect(activeVersion()).To(BeEmpty())
			})
		})

		When("the configuration is invalid", func() {
			It("should not start a retry", func() {
				Expect(os.WriteFile(configurationFilePath, []byte("not json"), 0600)).To(Succeed())
				mon.loadConfiguration(ctx)

				Expect(hasRetry()).To(BeFalse())
				Consistently(attempts.Load, 200*time.Millisecond, 20*time.Millisecond).Should(BeZero())
			})
		})

		When("the binary is ready", func() {
			It("should not start a retry", func() {
				writeConfiguration(currentVersion)
				mon.loadConfiguration(ctx)

				Expect(activeVersion()).To(Equal(currentVersion.String()))
				Expect(hasRetry()).To(BeFalse())
				Expect(attempts.Load()).To(BeZero())
			})
		})
	})
})

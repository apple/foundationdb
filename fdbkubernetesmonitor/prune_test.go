// prune_test.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2026 Apple Inc. and the FoundationDB project authors
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
	"context"
	"os"
	"path"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"github.com/prometheus/client_golang/prometheus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Testing the pruning of shared binaries", func() {
	var mon *monitor
	oldVersion := api.Version{Major: 7, Minor: 1, Patch: 51}
	midVersion := api.Version{Major: 7, Minor: 3, Patch: 10}
	newVersion := api.Version{Major: 7, Minor: 4, Patch: 1}

	listDir := func() []string {
		entries, err := os.ReadDir(sharedBinaryDir)
		Expect(err).NotTo(HaveOccurred())
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		return names
	}

	BeforeEach(func() {
		sharedBinaryDir = GinkgoT().TempDir()
		for _, name := range []string{oldVersion.String(), midVersion.String(), newVersion.String(), "unrelated"} {
			Expect(os.MkdirAll(path.Join(sharedBinaryDir, name), 0700)).To(Succeed())
			Expect(os.WriteFile(path.Join(sharedBinaryDir, name, "fdbserver"), []byte("fdbserver"), 0700)).To(Succeed())
		}
		Expect(os.WriteFile(path.Join(sharedBinaryDir, "7.0.0"), []byte("a file"), 0600)).To(Succeed())

		mon = &monitor{
			logger:       GinkgoLogr,
			processCount: 2,
			activeConfiguration: &api.ProcessConfiguration{
				Version:         &newVersion,
				UseSharedBinary: new(true),
			},
		}
	})

	When("the configuration doesn't use the shared binary", func() {
		BeforeEach(func() {
			mon.activeConfiguration.UseSharedBinary = nil
		})

		It("should not prune anything", func() {
			mon.pruneSharedBinaries()
			Expect(listDir()).To(HaveLen(5))
		})
	})

	When("no process is running", func() {
		It("should only keep the desired version", func() {
			mon.pruneSharedBinaries()
			Expect(listDir()).To(ConsistOf(newVersion.String(), "unrelated", "7.0.0"))
		})
	})

	When("all processes run the desired version", func() {
		BeforeEach(func() {
			mon.setProcessVersion(1, &newVersion)
			mon.setProcessVersion(2, &newVersion)
		})

		It("should only keep the desired version", func() {
			mon.pruneSharedBinaries()
			Expect(listDir()).To(ConsistOf(newVersion.String(), "unrelated", "7.0.0"))
		})
	})

	When("a process still runs an older version", func() {
		BeforeEach(func() {
			mon.setProcessVersion(1, &newVersion)
			mon.setProcessVersion(2, &oldVersion)
		})

		It("should not prune anything as a bounce is pending", func() {
			mon.pruneSharedBinaries()
			Expect(listDir()).To(ConsistOf(oldVersion.String(), midVersion.String(), newVersion.String(), "unrelated", "7.0.0"))
		})

		When("the process is stopped", func() {
			BeforeEach(func() {
				mon.setProcessVersion(2, nil)
			})

			It("should prune the older versions", func() {
				mon.pruneSharedBinaries()
				Expect(listDir()).To(ConsistOf(newVersion.String(), "unrelated", "7.0.0"))
			})
		})
	})

	When("a newer version than the desired one is staged", func() {
		BeforeEach(func() {
			mon.activeConfiguration.Version = &midVersion
		})

		It("should keep the newer version and prune the older one", func() {
			mon.pruneSharedBinaries()
			Expect(listDir()).To(ConsistOf(midVersion.String(), newVersion.String(), "unrelated", "7.0.0"))
		})
	})

	When("there is no active configuration", func() {
		BeforeEach(func() {
			mon.activeConfiguration = nil
		})

		It("should not prune anything", func() {
			mon.pruneSharedBinaries()
			Expect(listDir()).To(HaveLen(5))
		})
	})

	When("the shared binary directory does not exist", func() {
		BeforeEach(func() {
			sharedBinaryDir = path.Join(GinkgoT().TempDir(), "missing")
		})

		It("should not fail", func() {
			Expect(func() { mon.pruneSharedBinaries() }).NotTo(Panic())
		})
	})
})

var _ = Describe("Testing the staged versions", func() {
	writeBinary := func(version string, content []byte, mode os.FileMode) {
		Expect(os.MkdirAll(path.Join(sharedBinaryDir, version), 0700)).To(Succeed())
		Expect(os.WriteFile(path.Join(sharedBinaryDir, version, "fdbserver"), content, mode)).To(Succeed())
	}

	BeforeEach(func() {
		previous := sharedBinaryDir
		sharedBinaryDir = GinkgoT().TempDir()
		DeferCleanup(func() { sharedBinaryDir = previous })
	})

	When("getting the staged versions", func() {
		It("should only return ready versions in version order", func() {
			writeBinary("7.3.10", []byte("fdbserver"), 0700)
			writeBinary("7.3.9", []byte("fdbserver"), 0700)
			writeBinary("7.1.51", []byte("fdbserver"), 0700)
			writeBinary("7.4.1", []byte("fdbserver"), 0700)
			// Not ready or not a version.
			writeBinary("7.2.0", []byte{}, 0700)
			writeBinary("7.5.0", []byte("fdbserver"), 0600)
			Expect(os.MkdirAll(path.Join(sharedBinaryDir, "7.6.0"), 0700)).To(Succeed())
			writeBinary("unrelated", []byte("fdbserver"), 0700)
			Expect(os.WriteFile(path.Join(sharedBinaryDir, "7.0.0"), []byte("a file"), 0600)).To(Succeed())

			versions, err := getStagedVersions()
			Expect(err).NotTo(HaveOccurred())
			Expect(versions).To(Equal([]string{"7.1.51", "7.3.9", "7.3.10", "7.4.1"}))
			Expect(formatStagedVersions(versions)).To(Equal("7.1.51,7.3.9,7.3.10,7.4.1"))
		})

		It("should return no versions if the directory doesn't exist", func() {
			sharedBinaryDir = path.Join(sharedBinaryDir, "missing")

			versions, err := getStagedVersions()
			Expect(err).NotTo(HaveOccurred())
			Expect(versions).To(BeEmpty())
		})
	})

	When("reporting the staged versions", func() {
		var mon *monitor
		var podClient *kubernetesClient
		var fakeClient client.Client
		version := api.Version{Major: 7, Minor: 3, Patch: 10}

		environmentAnnotation := func() string {
			pod := &corev1.Pod{}
			Expect(fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "test", Name: "test-pod"}, pod)).To(Succeed())
			return pod.Annotations[api.EnvironmentAnnotation]
		}

		BeforeEach(func() {
			fakeClient = fake.NewFakeClient(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod", Namespace: "test"},
			})
			podClient = &kubernetesClient{
				podName:   "test-pod",
				namespace: "test",
				Logger:    GinkgoLogr,
				Client:    fakeClient,
			}
			mon = &monitor{
				logger:            GinkgoLogr,
				customEnvironment: map[string]string{},
				metrics:           registerMetrics(prometheus.NewRegistry()),
				podClient:         podClient,
			}
			writeBinary("7.3.10", []byte("fdbserver"), 0700)
		})

		When("there is no active configuration", func() {
			It("should report the version without updating the annotations", func() {
				Expect(mon.reportStagedVersions(context.Background(), "")).To(Equal("7.3.10"))
				Expect(environmentAnnotation()).To(BeEmpty())
			})
		})

		When("there is an active configuration", func() {
			BeforeEach(func() {
				mon.activeConfiguration = &api.ProcessConfiguration{Version: &version, BinaryPath: "/usr/bin/fdbserver"}
				mon.activeConfigurationBytes = []byte("{}")
			})

			It("should update the annotations only if the versions changed", func() {
				reported := mon.reportStagedVersions(context.Background(), "")
				Expect(reported).To(Equal("7.3.10"))
				Expect(environmentAnnotation()).To(ContainSubstring(`"STAGED_VERSIONS":"7.3.10"`))

				// Nothing changed, the annotation must not be written again.
				pod := &corev1.Pod{}
				Expect(fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "test", Name: "test-pod"}, pod)).To(Succeed())
				delete(pod.Annotations, api.EnvironmentAnnotation)
				Expect(fakeClient.Update(context.Background(), pod)).To(Succeed())
				Expect(mon.reportStagedVersions(context.Background(), reported)).To(Equal(reported))
				Expect(environmentAnnotation()).To(BeEmpty())

				writeBinary("7.4.1", []byte("fdbserver"), 0700)
				reported = mon.reportStagedVersions(context.Background(), reported)
				Expect(reported).To(Equal("7.3.10,7.4.1"))
				Expect(environmentAnnotation()).To(ContainSubstring(`"STAGED_VERSIONS":"7.3.10,7.4.1"`))
			})

			When("updating the annotations fails", func() {
				BeforeEach(func() {
					podClient.Client = fake.NewFakeClient()
				})

				It("should report the previous value to retry later", func() {
					Expect(mon.reportStagedVersions(context.Background(), "7.1.51")).To(Equal("7.1.51"))
				})
			})

			When("listing the staged versions fails", func() {
				It("should keep reporting the last known versions", func() {
					Expect(mon.reportStagedVersions(context.Background(), "")).To(Equal("7.3.10"))

					// A file instead of a directory makes the listing fail with an error other than not-exist.
					brokenDir := path.Join(GinkgoT().TempDir(), "file")
					Expect(os.WriteFile(brokenDir, []byte("a file"), 0600)).To(Succeed())
					sharedBinaryDir = brokenDir

					Expect(podClient.updateAnnotations(context.Background(), mon)).To(Succeed())
					Expect(environmentAnnotation()).To(ContainSubstring(`"STAGED_VERSIONS":"7.3.10"`))
				})
			})
		})
	})
})

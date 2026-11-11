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
	"os"
	"path"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"

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

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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

var _ = Describe("Testing FDB Kubernetes Monitor", func() {
	When("updating the custom environment variables from the node metadata", func() {
		var monitor *Monitor

		BeforeEach(func() {
			monitor = &Monitor{
				Logger: GinkgoLogr,
				CustomEnvironment: map[string]string{
					"testing": "testing",
				},
				PodClient: &PodClient{},
			}
		})

		JustBeforeEach(func() {
			monitor.updateCustomEnvironmentFromNodeMetadata()
		})

		When("no node metadata is present", func() {
			It("shouldn't add new entries", func() {
				Expect(monitor.CustomEnvironment).To(HaveLen(1))
				Expect(monitor.CustomEnvironment).To(HaveKeyWithValue("testing", "testing"))
			})
		})

		When("node metadata is present", func() {
			BeforeEach(func() {
				monitor.PodClient.nodeMetadata = &metav1.PartialObjectMetadata{
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
				Expect(monitor.CustomEnvironment).To(HaveLen(3))
				Expect(monitor.CustomEnvironment).To(HaveKeyWithValue("testing", "testing"))
				Expect(monitor.CustomEnvironment).To(HaveKeyWithValue("NODE_LABEL_TOPOLOGY_KUBERNETES_IO_ZONE", "us-east-1c"))
				Expect(monitor.CustomEnvironment).To(HaveKeyWithValue("NODE_LABEL_KUBERNETES_IO_HOSTNAME", "ip-172-20-114-199.ec2.internal"))
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
})

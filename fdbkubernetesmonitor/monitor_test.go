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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Testing FDB Kubernetes Monitor", func() {
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

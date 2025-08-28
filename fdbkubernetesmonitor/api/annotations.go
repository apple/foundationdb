// annotations.go
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

package api

const (
	// FoundationDBPrefix represents the prefix for all FoundationDB related annotations.
	FoundationDBPrefix = "foundationdb.org/"

	// CurrentConfigurationAnnotation is the annotation we use to store the
	// latest configuration.
	CurrentConfigurationAnnotation = FoundationDBPrefix + "launcher-current-configuration"

	// EnvironmentAnnotation is the annotation we use to store the environment
	// variables.
	EnvironmentAnnotation = FoundationDBPrefix + "launcher-environment"

	// OutdatedConfigMapAnnotation is the annotation we read to get notified of
	// outdated configuration.
	OutdatedConfigMapAnnotation = FoundationDBPrefix + "outdated-config-map-seen"

	// DelayShutdownAnnotation defines how long the FDB Kubernetes monitor process should sleep before shutting itself down.
	// The FDB Kubernetes monitor will always shutdown all fdbserver processes, independent of this setting.
	// The value of this annotation must be a duration like "60s".
	DelayShutdownAnnotation = FoundationDBPrefix + "delay-shutdown"

	// ClusterFileChangeDetectedAnnotation is the annotation that will be updated if the fdb.cluster file is updated.
	ClusterFileChangeDetectedAnnotation = FoundationDBPrefix + "cluster-file-change"

	// IsolateProcessGroupAnnotation is the annotation that defines if the current Pod should be isolated. Isolated
	// process groups will shutdown the fdbserver instance but keep the Pod and other Kubernetes resources running
	// for debugging purpose.
	IsolateProcessGroupAnnotation = FoundationDBPrefix + "isolate-process-group"

	// AvailableBinariesAnnotation is the annotation we use to store the available binaries on this Pod.
	AvailableBinariesAnnotation = FoundationDBPrefix + "available-binaries"
)

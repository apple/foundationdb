// prune.go
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
//

package main

import (
	"errors"
	"os"
	"path"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
)

// setProcessVersion records the version of the configuration a subprocess was started from. A nil version clears the
// entry, which must be done once the subprocess terminated.
func (monitor *monitor) setProcessVersion(processNumber int, version *api.Version) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	if version == nil {
		delete(monitor.processVersions, processNumber)
		return
	}

	if monitor.processVersions == nil {
		monitor.processVersions = make(map[int]api.Version)
	}

	monitor.processVersions[processNumber] = *version
}

// versionsToKeep returns the versions whose shared binaries must be kept: the desired version and every version a
// running process was started from. The returned bool is false if a bounce is pending, i.e. a running process was
// started from a version other than the desired one. It is also false if there is no active configuration or the
// configuration doesn't use the shared binary, as nothing is staged for the monitor in that case.
func (monitor *monitor) versionsToKeep() (map[string]struct{}, api.Version, bool) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	if monitor.activeConfiguration == nil || monitor.activeConfiguration.Version == nil || !monitor.activeConfiguration.ShouldUseSharedBinary() {
		return nil, api.Version{}, false
	}

	desired := *monitor.activeConfiguration.Version
	keep := map[string]struct{}{desired.String(): {}}
	bouncePending := false
	for _, version := range monitor.processVersions {
		keep[version.String()] = struct{}{}
		if version.String() != desired.String() {
			bouncePending = true
		}
	}

	return keep, desired, !bouncePending
}

// pruneSharedBinaries removes the version directories in the shared binary directory that are neither the desired
// version nor used by a running process. It is a no-op while a bounce is pending, as the staged binaries of the
// upcoming version must stay available until every process has switched. Versions newer than the desired one are kept
// as they might be staged for an upcoming upgrade, and directories that are not named after a FoundationDB version are
// never touched.
func (monitor *monitor) pruneSharedBinaries() {
	monitor.pruneMutex.Lock()
	defer monitor.pruneMutex.Unlock()

	keep, desired, ok := monitor.versionsToKeep()
	if !ok {
		return
	}

	entries, err := os.ReadDir(sharedBinaryDir)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			monitor.logger.Error(err, "Could not list shared binary directory", "sharedBinaryDir", sharedBinaryDir)
		}
		return
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		version, err := api.ParseFdbVersion(entry.Name())
		if err != nil {
			continue
		}

		if _, present := keep[version.String()]; present {
			continue
		}

		// A newer version might be staged for an upgrade that was not yet started.
		if version.IsAtLeast(desired) {
			continue
		}

		versionDir := path.Join(sharedBinaryDir, entry.Name())
		err = os.RemoveAll(versionDir)
		if err != nil {
			monitor.logger.Error(err, "Could not prune unused binary directory", "path", versionDir)
			continue
		}

		monitor.logger.Info("Pruned unused binary directory", "path", versionDir)
	}
}

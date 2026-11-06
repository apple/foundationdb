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
	"context"
	"errors"
	"os"
	"path"
	"slices"
	"strings"
	"time"

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

// stagedVersionsPollInterval defines how often the shared binary directory is scanned for staged versions.
const stagedVersionsPollInterval = 10 * time.Second

// getStagedVersions returns the sorted versions that have a ready fdbserver binary in the shared binary directory.
func getStagedVersions() ([]string, error) {
	entries, err := os.ReadDir(sharedBinaryDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}

	versions := make([]api.Version, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		version, err := api.ParseFdbVersion(entry.Name())
		if err != nil {
			continue
		}

		if checkOwnerExecutable(path.Join(sharedBinaryDir, entry.Name(), "fdbserver")) != nil {
			continue
		}

		versions = append(versions, version)
	}

	slices.SortFunc(versions, func(a, b api.Version) int {
		switch {
		case a.Equal(b):
			return 0
		case a.IsAtLeast(b):
			return 1
		default:
			return -1
		}
	})

	result := make([]string, 0, len(versions))
	for _, version := range versions {
		result = append(result, version.String())
	}

	return result, nil
}

// formatStagedVersions returns the representation of the staged versions that is reported in the pod annotation.
func formatStagedVersions(versions []string) string {
	return strings.Join(versions, ",")
}

// setStagedVersions records the formatted staged versions that were listed last.
func (monitor *monitor) setStagedVersions(formatted string) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	monitor.stagedVersions = formatted
	monitor.stagedVersionsKnown = true
}

// getStagedVersions returns the formatted staged versions that were listed last and whether a listing succeeded yet.
func (monitor *monitor) getStagedVersions() (string, bool) {
	monitor.mutex.Lock()
	defer monitor.mutex.Unlock()

	return monitor.stagedVersions, monitor.stagedVersionsKnown
}

// reportStagedVersions updates the staged version metric and, if the staged versions changed, the pod annotations.
func (monitor *monitor) reportStagedVersions(ctx context.Context, previous string) string {
	versions, err := getStagedVersions()
	if err != nil {
		monitor.logger.Error(err, "Could not list staged versions", "sharedBinaryDir", sharedBinaryDir)
		return previous
	}

	monitor.metrics.registerStagedVersions(versions)
	current := formatStagedVersions(versions)
	if current == previous {
		return previous
	}

	monitor.logger.Info("Staged versions changed", "stagedVersions", current)
	monitor.mutex.Lock()
	hasConfiguration := monitor.activeConfiguration != nil
	monitor.mutex.Unlock()
	// Without a configuration the annotations are written once the first configuration is loaded.
	if !hasConfiguration {
		return current
	}

	err = monitor.podClient.updateAnnotations(ctx, monitor)
	if err != nil {
		monitor.logger.Error(err, "Error updating pod annotations with the staged versions")
		// Report the previous value to retry in the next iteration.
		return previous
	}

	return current
}

// watchStagedVersions periodically reports the staged versions until the context is cancelled.
func (monitor *monitor) watchStagedVersions(ctx context.Context) {
	ticker := time.NewTicker(stagedVersionsPollInterval)
	defer ticker.Stop()

	var reported string
	for {
		reported = monitor.reportStagedVersions(ctx, reported)

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

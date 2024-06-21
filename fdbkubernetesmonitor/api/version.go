/*
 * version.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2021-2024 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Version represents a version of FoundationDB.
//
// This provides convenience methods for checking features available in
// different versions.
type Version struct {
	// Major is the major version
	Major int

	// Minor is the minor version
	Minor int

	// Patch is the patch version
	Patch int

	// ReleaseCandidate is the number from the `-rc\d+` suffix version
	// of the version if it exists
	ReleaseCandidate int
}

// versionRegex describes the format of a FoundationDB version.
var versionRegex = regexp.MustCompile(`(\d+)\.(\d+)\.(\d+)(-rc(\d+))?`)

// MarshalJSON custom implementation of MarshalJSON
func (version *Version) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("\"%s\"", version.String())), nil
}

// UnmarshalJSON custom implementation of UnmarshalJSON
func (version *Version) UnmarshalJSON(data []byte) error {
	trimmed := strings.Trim(string(data), "\"")
	currentVersion, err := ParseFdbVersion(trimmed)
	if err != nil {
		return err
	}

	version.Major = currentVersion.Major
	version.Minor = currentVersion.Minor
	version.Patch = currentVersion.Patch
	version.ReleaseCandidate = currentVersion.ReleaseCandidate
	return nil
}

// ParseFdbVersion parses a version from its string representation.
func ParseFdbVersion(version string) (Version, error) {
	matches := versionRegex.FindStringSubmatch(version)
	if matches == nil {
		return Version{}, fmt.Errorf("could not parse FDB version from %s", version)
	}

	major, err := strconv.Atoi(matches[1])
	if err != nil {
		return Version{}, err
	}

	minor, err := strconv.Atoi(matches[2])
	if err != nil {
		return Version{}, err
	}

	patch, err := strconv.Atoi(matches[3])
	if err != nil {
		return Version{}, err
	}

	rc, err := strconv.Atoi(matches[5])
	if err != nil {
		rc = 0
	}

	return Version{Major: major, Minor: minor, Patch: patch, ReleaseCandidate: rc}, nil
}

// String gets the string representation of an FDB version.
func (version Version) String() string {
	if version.ReleaseCandidate == 0 {
		return fmt.Sprintf("%d.%d.%d", version.Major, version.Minor, version.Patch)
	}
	return fmt.Sprintf("%d.%d.%d-rc%d", version.Major, version.Minor, version.Patch, version.ReleaseCandidate)
}

// Compact prints the version in the major.minor format.
func (version Version) Compact() string {
	return fmt.Sprintf("%d.%d", version.Major, version.Minor)
}

// IsAtLeast determines if a version is greater than or equal to another version.
func (version Version) IsAtLeast(other Version) bool {
	if version.Major < other.Major {
		return false
	}
	if version.Major > other.Major {
		return true
	}
	if version.Minor < other.Minor {
		return false
	}
	if version.Minor > other.Minor {
		return true
	}
	if version.Patch < other.Patch {
		return false
	}
	if version.Patch > other.Patch {
		return true
	}
	if version.ReleaseCandidate == 0 {
		return true
	}
	if other.ReleaseCandidate == 0 {
		return false
	}
	if version.ReleaseCandidate < other.ReleaseCandidate {
		return false
	}
	if version.ReleaseCandidate > other.ReleaseCandidate {
		return true
	}
	return true
}

// GetBinaryVersion Returns a version string compatible with the log implemented in the sidecars
func (version Version) GetBinaryVersion() string {
	if version.ReleaseCandidate > 0 {
		return version.String()
	}
	return version.Compact()
}

// IsProtocolCompatible determines whether two versions of FDB are protocol
// compatible.
func (version Version) IsProtocolCompatible(other Version) bool {
	return version.Major == other.Major && version.Minor == other.Minor && version.ReleaseCandidate == other.ReleaseCandidate
}

// NextMajorVersion returns the next major version of FoundationDB.
func (version Version) NextMajorVersion() Version {
	return Version{Major: version.Major + 1, Minor: 0, Patch: 0}
}

// NextMinorVersion returns the next minor version of FoundationDB.
func (version Version) NextMinorVersion() Version {
	return Version{Major: version.Major, Minor: version.Minor + 1, Patch: 0}
}

// NextPatchVersion returns the next patch version of FoundationDB.
func (version Version) NextPatchVersion() Version {
	return Version{Major: version.Major, Minor: version.Minor, Patch: version.Patch + 1}
}

// Equal checks if two Version are the same.
func (version Version) Equal(other Version) bool {
	return version.Major == other.Major &&
		version.Minor == other.Minor &&
		version.Patch == other.Patch &&
		version.ReleaseCandidate == other.ReleaseCandidate
}

/*
 * TLogGroupVersionTracker.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_PTXN_TEAMVERSIONTRACKER_H
#define FDBSERVER_PTXN_TEAMVERSIONTRACKER_H

#pragma once

#include <map>
#include <vector>

#include "fdbclient/FDBTypes.h"

namespace ptxn {

// Tracks the previous commit version (PCV) and commit version (CV) for each
// group so that groups can progress at different rate, while down stream
// components, e.g., TLogs or Storage Servers, can have ordered mutation
// streams.
class TLogGroupVersionTracker {
public:
	TLogGroupVersionTracker();

	// Adds "groups" to the tracker with their "beginVersion", i.e., first PCV.
	void addGroups(const std::vector<TLogGroupID>& groups, Version beginVersion);

	// TODO: remove this function as TLog groups only gets updated at the beginning of an epoch
	// Removes stale "groups" from this tracker.
	void removeGroups(const std::vector<TLogGroupID>& groups);

	// Updates "groups" with new commitVersion. Returns each group's PCV in a map.
	std::map<TLogGroupID, Version> updateGroups(const std::vector<TLogGroupID>& groups, Version commitVersion);

	// Returns the most lagging group and its CV.
	std::pair<TLogGroupID, Version> mostLaggingGroup() const;

	// Returns the maximum commit version of all groups
	Version getMaxCommitVersion() const { return maxCV; }

	// Returns the CV of a group, or invalidVersion if not found.
	Version getCommitVersion(TLogGroupID tid) const {
		auto it = versions.find(tid);
		return it == versions.end() ? invalidVersion : it->second;
	}

private:
	std::map<TLogGroupID, Version> versions; // a map of TLogGroupID -> CV
	Version maxCV = invalidVersion; // the maximum commit version of all groups
};

} // namespace ptxn

#endif // FDBSERVER_PTXN_TEAMVERSIONTRACKER_H

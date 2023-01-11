/*
 * VersionIndexer.actor.cpp
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
#include "fdbclient/FDBTypes.h"
#include "fdbclient/Notified.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/WaitFailure.h"
#include "flow/ActorCollection.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/VersionIndexerInterface.h"
#include "flow/actorcompiler.h" // has to be last include

template <>
void TSSMetrics::recordLatency(const VersionIndexerPeekRequest& req, double ssLatency, double tssLatency) {}

template <>
bool TSS_doCompare(VersionIndexerPeekReply const&, VersionIndexerPeekReply const&) {
	return true;
}

template <>
char const* TSS_mismatchTraceName(VersionIndexerPeekRequest const&) {
	ASSERT(false);
	return "";
}

template <>
void TSS_traceMismatch(TraceEvent&,
                       VersionIndexerPeekRequest const&,
                       VersionIndexerPeekReply const&,
                       VersionIndexerPeekReply const&) {
	ASSERT(false);
}

struct VersionIndexerStats {
	CounterCollection cc;
	Counter commits, peeks;
	Version lastCommittedVersion, windowBegin, windowEnd;

	Future<Void> logger;

	VersionIndexerStats(UID id);
};

struct VersionIndexerState {
	struct VersionEntry {
		Version version;
		std::vector<Tag> tags;
		bool operator<(VersionEntry const& other) const { return version < other.version; }
	};
	UID id;
	NotifiedVersion version;
	Version committedVersion = invalidVersion, previousVersion = invalidVersion;
	std::deque<VersionEntry> versionWindow;
	VersionIndexerStats stats;
	explicit VersionIndexerState(UID id) : id(id), version(invalidVersion), stats(id) {}
	void truncate(Version to) {
		while (versionWindow.front().version > to) {
			previousVersion = versionWindow.front().version;
			versionWindow.pop_front();
		}
	}
};

VersionIndexerStats::VersionIndexerStats(UID id)
  : cc("VersionIndexerStats", id.toString()), commits("Commits", cc), peeks("PeekRequests", cc) {
	logger = cc.traceCounters("VersionIndexerMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL);
}

ACTOR Future<Void> versionPeek(VersionIndexerState* self, VersionIndexerPeekRequest req) {
	++self->stats.peeks;
	wait(self->version.whenAtLeast(req.lastKnownVersion + 1));
	VersionIndexerState::VersionEntry searchEntry;
	searchEntry.version = req.lastKnownVersion;
	auto iter = std::upper_bound(self->versionWindow.begin(), self->versionWindow.end(), searchEntry);
	ASSERT(iter != self->versionWindow.end());
	VersionIndexerPeekReply reply;
	reply.minKnownCommittedVersion = self->committedVersion;
	reply.previousVersion = iter != self->versionWindow.begin() ? (iter - 1)->version : self->previousVersion;
	std::vector<Tag> tags;
	tags.push_back(req.tag);
	while (req.history.size() && req.lastKnownVersion >= req.history.back().first) {
		req.history.pop_back();
	}
	for (const auto& p : req.history) {
		tags.push_back(p.second);
	}
	std::sort(tags.begin(), tags.end());
	for (; iter != self->versionWindow.end(); ++iter) {
		// here we could use std::set_intersection but that would potentially do allocations. The loop below should be
		// in the same complexity class but doesn't allocate memory.
		bool hasMutations = false;
		auto i = iter->tags.begin();
		for (int j = 0; !hasMutations && i != tags.end() && j < tags.size(); ++j) {
			i = std::lower_bound(i, iter->tags.end(), tags[j]);
			hasMutations = i != iter->tags.end() && *i == tags[j];
		}
		ASSERT(iter->version > req.lastKnownVersion);
		reply.versions.emplace_back(iter->version, hasMutations);
	}
	req.reply.send(std::move(reply));
	return Void();
}

void truncateWindow(VersionIndexerState* self) {
	if (self->versionWindow.front().version >
	    self->versionWindow.back().version + 4 * SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS) {
		VersionIndexerState::VersionEntry searchEntry;
		searchEntry.version = self->versionWindow.front().version + SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
		auto iter = std::lower_bound(self->versionWindow.begin(), self->versionWindow.end(), searchEntry);
		self->previousVersion = iter->version;
		self->versionWindow.erase(self->versionWindow.begin(), ++iter);
	}
}

ACTOR Future<Void> addVersion(VersionIndexerState* self, VersionIndexerCommitRequest req) {
	state bool firstCommit = self->version.get() == invalidVersion;
	self->committedVersion = std::max(self->committedVersion, req.minKnownCommittedVersion);
	if (!firstCommit) {
		req.reply.send(Void());
	}
	++self->stats.commits;
	self->stats.lastCommittedVersion = std::max(self->stats.lastCommittedVersion, req.minKnownCommittedVersion);
	if (self->version.get() != invalidVersion) {
		wait(self->version.whenAtLeast(req.previousVersion));
	}
	if (self->version.get() < req.commitVersion) {
		ASSERT(firstCommit || self->version.get() == req.previousVersion);
		VersionIndexerState::VersionEntry entry;
		entry.version = req.commitVersion;
		entry.tags = std::move(req.tags);
		std::sort(entry.tags.begin(), entry.tags.end());
		self->versionWindow.emplace_back(std::move(entry));
		self->version.set(req.commitVersion);
		self->stats.windowEnd = req.commitVersion;
		if (firstCommit) {
			req.reply.send(Void());
		}
	}
	wait(yield());
	truncateWindow(self);
	return Void();
}

ACTOR Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db,
                                uint64_t recoveryCount,
                                VersionIndexerInterface myInterface) {
	loop {
		if (db->get().recoveryCount >= recoveryCount &&
		    !std::count(db->get().versionIndexers.begin(), db->get().versionIndexers.end(), myInterface)) {
			TraceEvent("VersionIndexerRemoved", myInterface.id())
			    .detail("RecoveryCount", db->get().recoveryCount)
			    .detail("LastRecoveryCount", recoveryCount)
			    .detail("FirstInterface",
			            db->get().versionIndexers.size() > 0 ? db->get().versionIndexers[0].id() : UID())
			    .detail("NumVersionIndexers", db->get().versionIndexers.size());
			throw worker_removed();
		}
		wait(db->onChange());
	}
}

ACTOR Future<Void> versionIndexer(VersionIndexerInterface interface,
                                  InitializeVersionIndexerRequest req,
                                  Reference<AsyncVar<ServerDBInfo> const> db) {
	state VersionIndexerState self(interface.id());
	state ActorCollection actors(false);
	state Future<Void> removed = checkRemoved(db, req.recoveryCount, interface);
	self.previousVersion = req.epochEnd;
	actors.add(waitFailureServer(interface.waitFailure.getFuture()));
	try {
		loop {
			choose {
				when(VersionIndexerCommitRequest req = waitNext(interface.commit.getFuture())) {
					actors.add(addVersion(&self, req));
				}
				when(VersionIndexerPeekRequest req = waitNext(interface.peek.getFuture())) {
					actors.add(versionPeek(&self, req));
				}
				when(wait(actors.getResult())) { UNSTOPPABLE_ASSERT(false); }
				when(wait(removed)) {}
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled || e.code() == error_code_worker_removed) {
			TraceEvent("VersionIndexerTerminated", interface.id()).errorUnsuppressed(e);
			return Void();
		}
		throw;
	}
}

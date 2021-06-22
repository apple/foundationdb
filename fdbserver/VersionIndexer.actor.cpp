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
#include "fdbserver/Knobs.h"
#include "fdbserver/VersionIndexerInterface.actor.h"
#include "flow/ActorCollection.h"
#include "flow/actorcompiler.h" // has to be last include

struct VersionIndexerStats {
	CounterCollection cc;
	Counter reqIn, peeks;
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
	explicit VersionIndexerState(UID id) : id(id), stats(id) {}
	void truncate(Version to) {
		while (versionWindow.front().version > to) {
			previousVersion = versionWindow.front().version;
			versionWindow.pop_front();
		}
	}
};

VersionIndexerStats::VersionIndexerStats(UID id)
  : cc("VersionIndexerStats", id.toString()), reqIn("Requests", cc), peeks("PeekRequests", cc) {
	logger = traceCounters("VersionIndexerMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, &cc);
}

ACTOR Future<Void> versionPeek(VersionIndexerState* self, VersionIndexerPeekRequest req) {
	++self->stats.peeks;
	wait(self->version.whenAtLeast(req.lastKnownVersion + 1));
	VersionIndexerState::VersionEntry searchEntry;
	searchEntry.version = req.lastKnownVersion;
	auto iter = std::lower_bound(self->versionWindow.begin(), self->versionWindow.end(), searchEntry);
	ASSERT(iter != self->versionWindow.end());
	VersionIndexerPeekReply reply;
	if (iter->version != req.lastKnownVersion) {
		// storage fell behind and will need to catch up -- but we'll still send the
		reply.previousVersion = invalidVersion;
	} else if (iter == self->versionWindow.begin()) {
		reply.previousVersion = self->previousVersion;
		++iter;
	} else {
		reply.previousVersion = (iter - 1)->version;
		++iter;
	}
	for (; iter != self->versionWindow.end(); ++iter) {
		auto i = std::lower_bound(iter->tags.begin(), iter->tags.end(), req.tag);
		bool hasMutations = i != iter->tags.end() && *i != req.tag;
		reply.versions.emplace_back(iter->version, hasMutations);
	}
	req.reply.send(std::move(reply));
	return Void();
}

ACTOR Future<Void> addVersion(VersionIndexerState* self, VersionIndexerCommitRequest req) {
	self->committedVersion = std::max(self->committedVersion, req.committedVersion);
	req.reply.send(Void());
	++self->stats.reqIn;
	self->stats.lastCommittedVersion = std::max(self->stats.lastCommittedVersion, req.committedVersion);
	wait(self->version.whenAtLeast(req.previousVersion));
	if (self->version.get() < req.version) {
		ASSERT(self->version.get() == req.previousVersion);
		VersionIndexerState::VersionEntry entry;
		entry.version = req.version;
		entry.tags = std::move(req.tags);
		std::sort(entry.tags.begin(), entry.tags.end());
		self->versionWindow.emplace_back(std::move(entry));
		self->version.set(req.version);
		self->stats.windowEnd = req.version;
	}
	return Void();
}

ACTOR Future<Void> windowTruncator(VersionIndexerState* self) {
	wait(self->version.whenAtLeast(1)); // wait for first commit
	loop {
		wait(self->version.whenAtLeast(self->versionWindow.front().version +
		                               4 * SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS));
		auto truncateTo = self->version.get() - SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
		self->truncate(truncateTo);
		self->stats.windowBegin = truncateTo;
	}
}

ACTOR Future<Void> versionIndexer(VersionIndexerInterface interface) {
	state VersionIndexerState self(interface.id());
	state ActorCollection actors(false);
	actors.add(windowTruncator(&self));
	loop {
		choose {
			when(VersionIndexerCommitRequest req = waitNext(interface.commit.getFuture())) {
				actors.add(addVersion(&self, req));
			}
			when(wait(actors.getResult())) { UNSTOPPABLE_ASSERT(false); }
		}
	}
}
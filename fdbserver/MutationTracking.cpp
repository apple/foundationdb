/*
 * MutationTracking.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <algorithm>
#include <vector>
#include "fdbclient/FDBTypes.h"
#include "fdbserver/MutationTracking.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/SpanContextMessage.h"
#include "fdbserver/OTELSpanContextMessage.h"
#include "fdbclient/SystemData.h"
#if defined(FDB_CLEAN_BUILD) && MUTATION_TRACKING_ENABLED
#error "You cannot use mutation tracking in a clean/release build."
#endif

// If MUTATION_TRACKING_ENABLED is set, MutationTracking events will be logged for the
// keys in debugKeys and the ranges in debugRanges.
// Each entry is a pair of (label, keyOrRange) and the Label will be attached to the
// MutationTracking TraceEvent for easier searching/recognition.
std::vector<std::pair<const char*, KeyRef>> debugKeys = { { "SomeKey", "foo"_sr } };
std::vector<std::pair<const char*, KeyRangeRef>> debugRanges = { { "Everything", { ""_sr, "\xff\xff\xff\xff"_sr } } };

TraceEvent debugMutationEnabled(const char* context, Version version, MutationRef const& mutation, UID id) {
	const char* label = nullptr;

	for (auto& labelKey : debugKeys) {
		if (((mutation.type == mutation.ClearRange || mutation.type == mutation.DebugKeyRange) &&
		     KeyRangeRef(mutation.param1, mutation.param2).contains(labelKey.second)) ||
		    mutation.param1 == labelKey.second) {
			label = labelKey.first;
			break;
		}
	}

	for (auto& labelRange : debugRanges) {
		if (((mutation.type == mutation.ClearRange || mutation.type == mutation.DebugKeyRange) &&
		     KeyRangeRef(mutation.param1, mutation.param2).intersects(labelRange.second)) ||
		    labelRange.second.contains(mutation.param1)) {
			label = labelRange.first;
			break;
		}
	}

	if (label != nullptr) {
		TraceEvent event("MutationTracking", id);
		event.detail("Label", label).detail("At", context).detail("Version", version).detail("Mutation", mutation);
		return event;
	}

	return TraceEvent();
}

TraceEvent debugEncrptedMutationEnabled(const char* context, Version version, MutationRef const& mutation, UID id) {
	ASSERT(mutation.type == mutation.Encrypted);
	MutationRef fmutation = Standalone(mutation);
	Arena tempArena;
	ArenaReader reader(tempArena, mutation.param2, AssumeVersion(ProtocolVersion::withEncryptionAtRest()));
	reader >> fmutation;
	return debugMutationEnabled(context, version, fmutation, id);
}

TraceEvent debugKeyRangeEnabled(const char* context, Version version, KeyRangeRef const& keys, UID id) {
	return debugMutation(context, version, MutationRef(MutationRef::DebugKeyRange, keys.begin, keys.end), id);
}

TraceEvent debugTagsAndMessageEnabled(const char* context, Version version, StringRef commitBlob, UID id) {
	BinaryReader rdr(commitBlob, AssumeVersion(g_network->protocolVersion()));
	while (!rdr.empty()) {
		if (*(int32_t*)rdr.peekBytes(4) == VERSION_HEADER) {
			int32_t dummy;
			rdr >> dummy >> version;
			continue;
		}
		TagsAndMessage msg;
		msg.loadFromArena(&rdr, nullptr);
		bool logAdapterMessage =
		    std::any_of(msg.tags.begin(), msg.tags.end(), [](const Tag& t) { return t.locality == tagLocalityTxs; });
		StringRef mutationData = msg.getMessageWithoutTags();
		uint8_t mutationType = *mutationData.begin();
		if (logAdapterMessage) {
			// Skip the message, as there will always be an identical non-logAdapterMessage mutation
			// that we can match against in the same commit.
		} else if (LogProtocolMessage::startsLogProtocolMessage(mutationType)) {
			BinaryReader br(mutationData, AssumeVersion(rdr.protocolVersion()));
			LogProtocolMessage lpm;
			br >> lpm;
			rdr.setProtocolVersion(br.protocolVersion());
		} else if (SpanContextMessage::startsSpanContextMessage(mutationType)) {
			BinaryReader br(mutationData, AssumeVersion(rdr.protocolVersion()));
			SpanContextMessage scm;
			br >> scm;
		} else if (OTELSpanContextMessage::startsOTELSpanContextMessage(mutationType)) {
			BinaryReader br(mutationData, AssumeVersion(rdr.protocolVersion()));
			OTELSpanContextMessage scm;
			br >> scm;
		} else {
			MutationRef m;
			BinaryReader br(mutationData, AssumeVersion(rdr.protocolVersion()));
			br >> m;
			TraceEvent event = debugMutation(context, version, m, id);
			if (event.isEnabled()) {
				event.detail("MessageTags", msg.tags);
				return event;
			}
		}
	}
	return TraceEvent();
}

#if MUTATION_TRACKING_ENABLED
TraceEvent debugMutation(const char* context, Version version, MutationRef const& mutation, UID id) {
	if (mutation.type == mutation.Encrypted) {
		return debugEncrptedMutationEnabled(context, version, mutation, id);
	} else {
		return debugMutationEnabled(context, version, mutation, id);
	}
}
TraceEvent debugKeyRange(const char* context, Version version, KeyRangeRef const& keys, UID id) {
	return debugKeyRangeEnabled(context, version, keys, id);
}
TraceEvent debugTagsAndMessage(const char* context, Version version, StringRef commitBlob, UID id) {
	return debugTagsAndMessageEnabled(context, version, commitBlob, id);
}
#else
TraceEvent debugMutation(const char* context, Version version, MutationRef const& mutation, UID id) {
	return TraceEvent();
}
TraceEvent debugKeyRange(const char* context, Version version, KeyRangeRef const& keys, UID id) {
	return TraceEvent();
}
TraceEvent debugTagsAndMessage(const char* context, Version version, StringRef commitBlob, UID id) {
	return TraceEvent();
}
#endif

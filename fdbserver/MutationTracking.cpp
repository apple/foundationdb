/*
 * MutationTracking.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include <vector>
#include "fdbserver/MutationTracking.h"
#include "fdbserver/LogProtocolMessage.h"

#if defined(FDB_CLEAN_BUILD) && MUTATION_TRACKING_ENABLED
#error "You cannot use mutation tracking in a clean/release build."
#endif

// Track up to 2 keys in simulation via enabling MUTATION_TRACKING_ENABLED and setting the keys here.
StringRef debugKey = LiteralStringRef( "" );
StringRef debugKey2 = LiteralStringRef( "\xff\xff\xff\xff" );

TraceEvent debugMutationEnabled( const char* context, Version version, MutationRef const& mutation ) {
	if ((mutation.type == mutation.ClearRange || mutation.type == mutation.DebugKeyRange) &&
			((mutation.param1<=debugKey && mutation.param2>debugKey) || (mutation.param1<=debugKey2 && mutation.param2>debugKey2))) {
		return std::move(TraceEvent("MutationTracking").detail("At", context).detail("Version", version).detail("MutationType", typeString[mutation.type]).detail("KeyBegin", mutation.param1).detail("KeyEnd", mutation.param2));
	} else if (mutation.param1 == debugKey || mutation.param1 == debugKey2) {
		return std::move(TraceEvent("MutationTracking").detail("At", context).detail("Version", version).detail("MutationType", typeString[mutation.type]).detail("Key", mutation.param1).detail("Value", mutation.param2));
	} else {
		return TraceEvent();
	}
}

TraceEvent debugKeyRangeEnabled( const char* context, Version version, KeyRangeRef const& keys ) {
	if (keys.contains(debugKey) || keys.contains(debugKey2)) {
		return debugMutation(context, version, MutationRef(MutationRef::DebugKeyRange, keys.begin, keys.end));
	} else {
		return TraceEvent();
	}
}

TraceEvent debugTagsAndMessageEnabled( const char* context, Version version, StringRef commitBlob ) {
	BinaryReader rdr(commitBlob, AssumeVersion(currentProtocolVersion));
	while (!rdr.empty()) {
		if (*(int32_t*)rdr.peekBytes(4) == VERSION_HEADER) {
			int32_t dummy;
			rdr >> dummy >> version;
			continue;
		}
		TagsAndMessage msg;
		msg.loadFromArena(&rdr, nullptr);
		bool logAdapterMessage = std::any_of(
				msg.tags.begin(), msg.tags.end(), [](const Tag& t) { return t == txsTag || t.locality == tagLocalityTxs; });
		StringRef mutationData = msg.getMessageWithoutTags();
		uint8_t mutationType = *mutationData.begin();
		if (logAdapterMessage) {
			// Skip the message, as there will always be an idential non-logAdapterMessage mutation
			// that we can match against in the same commit.
		} else if (LogProtocolMessage::startsLogProtocolMessage(mutationType)) {
			BinaryReader br(mutationData, AssumeVersion(rdr.protocolVersion()));
			LogProtocolMessage lpm;
			br >> lpm;
			rdr.setProtocolVersion(br.protocolVersion());
		} else {
			MutationRef m;
			BinaryReader br(mutationData, AssumeVersion(rdr.protocolVersion()));
			br >> m;
			TraceEvent&& event = debugMutation(context, version, m);
			if (event.isEnabled()) {
				return std::move(event.detail("MessageTags", msg.tags));
			}
		}
	}
	return TraceEvent();
}

#if MUTATION_TRACKING_ENABLED
TraceEvent debugMutation( const char* context, Version version, MutationRef const& mutation ) {
	return debugMutationEnabled( context, version, mutation );
}
TraceEvent debugKeyRange( const char* context, Version version, KeyRangeRef const& keys ) {
	return debugKeyRangeEnabled( context, version, keys );
}
TraceEvent debugTagsAndMessage( const char* context, Version version, StringRef commitBlob ) {
	return debugTagsAndMessageEnabled( context, version, commitBlob );
}
#else
TraceEvent debugMutation(const char* context, Version version, MutationRef const& mutation) {
	return TraceEvent();
}
TraceEvent debugKeyRange(const char* context, Version version, KeyRangeRef const& keys) {
	return TraceEvent();
}
TraceEvent debugTagsAndMessage(const char* context, Version version, StringRef commitBlob) {
	return TraceEvent();
}
#endif

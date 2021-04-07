/*
 * ProxyTLogPushMessageSerializer.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ptxn/ProxyTLogPushMessageSerializer.h"

namespace ptxn {

void ProxyTLogPushMessageSerializer::writeMessage(const MutationRef& mutation, const TeamID& teamID) {
	writers[teamID].writeItem(SubsequenceMutationItem{ currentSubsequence++, mutation });
}

void ProxyTLogPushMessageSerializer::completeMessageWriting(const TeamID& teamID) {
	writers[teamID].completeItemWriting();

	ProxyTLogMessageHeader header;
	header.numItems = writers[teamID].getNumItems();
	header.length = writers[teamID].getItemsBytes();

	writers[teamID].writeHeader(header);
}

Standalone<StringRef> ProxyTLogPushMessageSerializer::getSerialized(const TeamID& teamID) {
	ASSERT(writers[teamID].isWritingCompleted());
	return writers[teamID].getSerialized();
}

bool proxyTLogPushMessageDeserializer(const Arena& arena,
                                      StringRef serialized,
                                      ProxyTLogMessageHeader& header,
                                      std::vector<SubsequenceMutationItem>& messages) {
	if (!headeredItemDeserializerBase(arena, serialized, header, messages)) {
		return false;
	}

	return true;
}

} // namespace ptxn

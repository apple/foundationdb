/*
 * MockPeekCursor.h
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

#ifndef FDBSERVER_MOCK_PEEK_CURSOR
#define FDBSERVER_MOCK_PEEK_CURSOR

#include "fdbserver/LogSystem.h"

struct MockPeekCursor final : ILogSystem::IPeekCursor, ReferenceCounted<MockPeekCursor> {

	struct MessageAndTags {
		MessageAndTags(const std::string& message, const vector<Tag>& tags) : message(message), tags(tags) {}
		std::string message;
		std::vector<Tag> tags;
		// We don't handle subversion in this mock because storage server doesn't care about it.
	};
	typedef std::pair<Version, std::vector<MessageAndTags>> OneVersionMessages;

	// Input: The messages that need to be feed into the mock cursor.
	const std::vector<OneVersionMessages> allVersionMessages;

	// Iteration statuses of the cursor
	std::size_t pVersion = 0;
	std::size_t pMessage = 0;

	// _version could be derived from pVersion. We have an additional field so version() can return a reference.
	LogMessageVersion cursorVersion;
	// _reader could be derived too. We have an additional field so reader() can return a reference.
	ArenaReader cursorReader;
	Arena cursorArena;

	MockPeekCursor(const std::vector<OneVersionMessages>& allVersionMessages, const Arena& arena);
	MockPeekCursor(const std::vector<OneVersionMessages>& allVersionMessages,
	               size_t pVersion,
	               size_t pMessage,
	               const LogMessageVersion& version,
	               const Arena& arena);

	void describe();

	Reference<IPeekCursor> cloneNoMore() override;
	void setProtocolVersion(ProtocolVersion version) override;
	bool hasMessage() const override;
	VectorRef<Tag> getTags() const override;
	Arena& arena() override;
	ArenaReader* reader() override;
	StringRef getMessage() override;
	StringRef getMessageWithTags() override;
	void nextMessage() override;
	void advanceTo(LogMessageVersion n) override;
	Future<Void> getMore(TaskPriority taskID) override;
	Future<Void> onFailed() override;
	bool isActive() const override;
	bool isExhausted() const override;
	const LogMessageVersion& version() const override;
	Version popped() const override;
	Version getMinKnownCommittedVersion() const override;
	Optional<UID> getPrimaryPeekLocation() const override;
	void addref() override;
	void delref() override;
};

#endif // FDBSERVER_MOCK_PEEK_CURSOR

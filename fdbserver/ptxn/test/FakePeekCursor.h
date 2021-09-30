/*
 * FakePeekCursor.h
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

#ifndef FDBSERVER_PTXN_TEST_MOCK_PEEK_CURSOR
#define FDBSERVER_PTXN_TEST_MOCK_PEEK_CURSOR

#include <iosfwd>
#include "fdbserver/LogSystem.h"

namespace ptxn::test {
struct MockPeekCursor final : ILogSystem::IPeekCursor, ReferenceCounted<MockPeekCursor> {

	struct VersionedMessage {
		// Every message should be able to have independent arenas so that they can be freed separately after consumed.
		Arena arena;
		Version version;
		// Subversion is not used for now.
		uint32_t sub = 0;
		StringRef message;
		VectorRef<Tag> tags;
		VersionedMessage() = default;
		VersionedMessage(const Arena& arena, Version version, const StringRef& message, const VectorRef<Tag>& tags)
		  : arena(arena), version(version), message(message), tags(tags) {}

		friend std::ostream& operator<<(std::ostream& os, const VersionedMessage& versionedMessage);
	};

	struct VersionedMessageSupplier final : ReferenceCounted<VersionedMessageSupplier> {
		int i = 0;
		int end;
		Standalone<VectorRef<Tag>> tags;
		int advanceVersionsPerMutation;

		Optional<VersionedMessage> get() {
			// std::cout << "OnDemandVersionedMessageSupplier get " << i << ", end " << end << std::endl;
			ASSERT(i <= end);
			if (i == end) {
				return Optional<VersionedMessage>();
			}
			Arena arena;
			// TODO: Support keys in random order.
			MutationRef mutation(arena,
			                     MutationRef::SetValue,
			                     StringRef(arena, "Key-" + std::to_string(i)),
			                     StringRef(arena, "Value-" + std::to_string(i)));
			StringRef str = StringRef(arena, BinaryWriter::toValue(mutation, AssumeVersion(currentProtocolVersion)));
			Version version = i * advanceVersionsPerMutation + 1;
			VersionedMessage message(arena, version, str, tags);

			i++;
			return Optional<VersionedMessage>(message);
		}

		static Version commitVersion(int id, int advanceVersionsPerMutation) {
			return id * advanceVersionsPerMutation + 1;
		}

		explicit VersionedMessageSupplier(const int end,
		                                  const Standalone<VectorRef<Tag>> tags,
		                                  const int advanceVersionsPerMutation)
		  : end(end), tags(tags), advanceVersionsPerMutation(advanceVersionsPerMutation) {}

		VersionedMessageSupplier(const VersionedMessageSupplier& that)
		  : i(that.i), end(that.end), tags(that.tags), advanceVersionsPerMutation(that.advanceVersionsPerMutation) {}
		VersionedMessageSupplier& operator=(const VersionedMessageSupplier& that) {
			i = that.i;
			end = that.end;
			tags = that.tags;
			advanceVersionsPerMutation = that.advanceVersionsPerMutation;
			return *this;
		}

		using ReferenceCounted<VersionedMessageSupplier>::addref;
		using ReferenceCounted<VersionedMessageSupplier>::delref;
	};

	// Every time when getMore() is called, the end of supplier is extended by nMutationsPerMore until it reaches
	// maxMutations().
	int nMutationsPerMore = 0;
	Optional<int> maxMutations = 0;

	VersionedMessageSupplier supplier;
	Arena cursorArena;

	// The following 2 variables keeps the information of the next message after calling nextMessage()
	Optional<VersionedMessage> curVersionedMessage;
	LogMessageVersion curVersion;
	// This can be updated every time reader() is called, so we do not need to keep its status. It's here just for
	// managing the pointer returned by reader().
	std::unique_ptr<ArenaReader> curReader = nullptr;

	MockPeekCursor(int nMutationsPerMore,
	               Optional<int> maxMutations,
	               const VersionedMessageSupplier& supplier,
	               const Arena& cursorArena);

	// When cloneNoMore().
	MockPeekCursor(const VersionedMessageSupplier& supplier,
	               const Arena& cursorArena,
	               const Optional<VersionedMessage>& curVersionedMessage,
	               const LogMessageVersion& curVersion);

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
	Optional<UID> getCurrentPeekLocation() const override;
	void addref() override;
	void delref() override;
};
} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_MOCK_PEEK_CURSOR
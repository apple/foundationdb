/*
 * FakePeekCursor.cpp
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

#include <ostream>

#include "fdbserver/ptxn/test/FakePeekCursor.h"

namespace ptxn::test {

const static bool LOG_METHOD_CALLS = false;
const static bool LOG_MESSAGES = false;

std::ostream& operator<<(std::ostream& os, const MockPeekCursor::VersionedMessage& versionedMessage) {
	os << "VersionedMessage [version: " << versionedMessage.version << " sub: " << versionedMessage.sub
	   << " message: " << StringRef(versionedMessage.message).toHexString() << " tags: ";
	for (auto tag : versionedMessage.tags) {
		os << tag.toString() << " ";
	}
	os << "]";
	return os;
}

static void logMethodName(std::string methodName) {
	if (LOG_METHOD_CALLS) {
		std::cout << "FakeLogSystem::" << methodName << std::endl;
	}
}

MockPeekCursor::MockPeekCursor(int nMutationsPerMore,
                               Optional<int> maxMutations,
                               const VersionedMessageSupplier& supplier,
                               const Arena& cursorArena)
  : nMutationsPerMore(nMutationsPerMore), maxMutations(maxMutations), supplier(supplier), cursorArena(cursorArena) {
	logMethodName(__func__);
}

MockPeekCursor::MockPeekCursor(const VersionedMessageSupplier& supplier,
                               const Arena& cursorArena,
                               const Optional<VersionedMessage>& curVersionedMessage,
                               const LogMessageVersion& curVersion)
  : supplier(supplier), cursorArena(cursorArena), curVersionedMessage(curVersionedMessage), curVersion(curVersion) {
	// nMutationsPerMore is set to 0.
	logMethodName(__func__);
}

Reference<ILogSystem::IPeekCursor> MockPeekCursor::cloneNoMore() {
	logMethodName(__func__);
	return makeReference<MockPeekCursor>(supplier, cursorArena, curVersionedMessage, curVersion);
}

// Ignore setProtocolVersion. Always use IncludeVersion(). TODO: It hits segfault when copy construct if I use
// IncludeVersion().
void MockPeekCursor::setProtocolVersion(ProtocolVersion version) {
	logMethodName(__func__);
}

bool MockPeekCursor::hasMessage() const {
	logMethodName(__func__);
	return curVersionedMessage.present();
}

VectorRef<Tag> MockPeekCursor::getTags() const {
	logMethodName(__func__);
	// It should return something like allVersionMessages[pVersion].second[pMessage].tags;
	UNREACHABLE() // Not yet supported
}

Arena& MockPeekCursor::arena() {
	logMethodName(__func__);
	UNREACHABLE() // Not yet supported
}

ArenaReader* MockPeekCursor::reader() {
	logMethodName(__func__);
	curReader.reset(new ArenaReader(cursorArena, getMessage(), AssumeVersion(currentProtocolVersion)));
	return curReader.get();
}

StringRef MockPeekCursor::getMessage() {
	logMethodName(__func__);
	ASSERT(curVersionedMessage.present());
	StringRef message = curVersionedMessage.get().message;
	if (LOG_MESSAGES) {
		std::cout << "MockPeekCursor gets message from: " << curVersionedMessage.get() << std::endl;
	}
	return message;
}

StringRef MockPeekCursor::getMessageWithTags() {
	logMethodName(__func__);
	return StringRef();
}

void MockPeekCursor::nextMessage() {
	logMethodName(__func__);
	curVersionedMessage = supplier.get();
	if (curVersionedMessage.present()) {
		// We are not interested in sub-version yet.
		curVersion.reset(curVersionedMessage.get().version);
		if (LOG_MESSAGES) {
			std::cout << "Next message is " << curVersionedMessage.get() << ", curVersion reset to "
			          << curVersion.toString() << std::endl;
		}

	} else {
		// If there is no next message, use the last version + 1.
		curVersion.reset(curVersion.version + 1);
		if (LOG_MESSAGES) {
			std::cout << "No next message, curVersion reset to " << curVersion.toString() << std::endl;
		}
	}
}

void MockPeekCursor::advanceTo(LogMessageVersion n) {
	logMethodName(__func__);
	while (hasMessage() && version() < n) {
		nextMessage();
	}
}

Future<Void> MockPeekCursor::getMore(TaskPriority taskID) {
	logMethodName(__func__);

	int nextEnd = supplier.end + nMutationsPerMore;
	if (maxMutations.present()) {
		nextEnd = std::min(nextEnd, maxMutations.get());
	}
	supplier.end = nextEnd;
	nextMessage();

	return Void();
}

Future<Void> MockPeekCursor::onFailed() {
	logMethodName(__func__);
	return Future<Void>();
}

bool MockPeekCursor::isActive() const {
	logMethodName(__func__);
	return false;
}

bool MockPeekCursor::isExhausted() const {
	logMethodName(__func__);
	return false;
}

const LogMessageVersion& MockPeekCursor::version() const {
	logMethodName(__func__);
	return curVersion;
}

Version MockPeekCursor::popped() const {
	logMethodName(__func__);
	return 0;
}

Version MockPeekCursor::getMinKnownCommittedVersion() const {
	logMethodName(__func__);
	return 0;
}

Optional<UID> MockPeekCursor::getPrimaryPeekLocation() const {
	logMethodName(__func__);
	return Optional<UID>();
}

Optional<UID> MockPeekCursor::getCurrentPeekLocation() const {
	logMethodName(__func__);
	return Optional<UID>();
}

void MockPeekCursor::addref() {
	logMethodName(__func__);
	ReferenceCounted<MockPeekCursor>::addref();
}

void MockPeekCursor::delref() {
	logMethodName(__func__);
	ReferenceCounted<MockPeekCursor>::delref();
}

} // namespace ptxn::test

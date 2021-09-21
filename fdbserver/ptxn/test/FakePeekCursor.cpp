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

std::ostream& operator<<(std::ostream& os, const FakePeekCursor::VersionedMessage& versionedMessage) {
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

FakePeekCursor::FakePeekCursor(int nMutationsPerMore,
                               Optional<int> maxMutations,
                               const VersionedMessageSupplier& supplier,
                               const Arena& cursorArena)
  : nMutationsPerMore(nMutationsPerMore), maxMutations(maxMutations), supplier(supplier), cursorArena(cursorArena) {
	logMethodName(__func__);
}

FakePeekCursor::FakePeekCursor(const VersionedMessageSupplier& supplier,
                               const Arena& cursorArena,
                               const Optional<VersionedMessage>& curVersionedMessage,
                               const LogMessageVersion& curVersion)
  : supplier(supplier), cursorArena(cursorArena), curVersionedMessage(curVersionedMessage), curVersion(curVersion) {
	// nMutationsPerMore is set to 0.
	logMethodName(__func__);
}

Reference<ILogSystem::IPeekCursor> FakePeekCursor::cloneNoMore() {
	logMethodName(__func__);
	return makeReference<FakePeekCursor>(supplier, cursorArena, curVersionedMessage, curVersion);
}

// Ignore setProtocolVersion. Always use IncludeVersion(). TODO: It hits segfault when copy construct if I use
// IncludeVersion().
void FakePeekCursor::setProtocolVersion(ProtocolVersion version) {
	logMethodName(__func__);
}

bool FakePeekCursor::hasMessage() const {
	logMethodName(__func__);
	return curVersionedMessage.present();
}

VectorRef<Tag> FakePeekCursor::getTags() const {
	logMethodName(__func__);
	// It should return something like allVersionMessages[pVersion].second[pMessage].tags;
	UNREACHABLE() // Not yet supported
}

Arena& FakePeekCursor::arena() {
	logMethodName(__func__);
	UNREACHABLE() // Not yet supported
}

ArenaReader* FakePeekCursor::reader() {
	logMethodName(__func__);
	curReader.reset(new ArenaReader(cursorArena, getMessage(), AssumeVersion(currentProtocolVersion)));
	return curReader.get();
}

StringRef FakePeekCursor::getMessage() {
	logMethodName(__func__);
	ASSERT(curVersionedMessage.present());
	StringRef message = curVersionedMessage.get().message;
	if (LOG_MESSAGES) {
		std::cout << "FakePeekCursor gets message from: " << curVersionedMessage.get() << std::endl;
	}
	return message;
}

StringRef FakePeekCursor::getMessageWithTags() {
	logMethodName(__func__);
	return StringRef();
}

void FakePeekCursor::nextMessage() {
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

void FakePeekCursor::advanceTo(LogMessageVersion n) {
	logMethodName(__func__);
	while (hasMessage() && version() < n) {
		nextMessage();
	}
}

Future<Void> FakePeekCursor::getMore(TaskPriority taskID) {
	logMethodName(__func__);

	int nextEnd = supplier.end + nMutationsPerMore;
	if (maxMutations.present()) {
		nextEnd = std::min(nextEnd, maxMutations.get());
	}
	supplier.end = nextEnd;
	nextMessage();

	return Void();
}

Future<Void> FakePeekCursor::onFailed() {
	logMethodName(__func__);
	return Future<Void>();
}

bool FakePeekCursor::isActive() const {
	logMethodName(__func__);
	return false;
}

bool FakePeekCursor::isExhausted() const {
	logMethodName(__func__);
	return false;
}

const LogMessageVersion& FakePeekCursor::version() const {
	logMethodName(__func__);
	return curVersion;
}

Version FakePeekCursor::popped() const {
	logMethodName(__func__);
	return 0;
}

Version FakePeekCursor::getMinKnownCommittedVersion() const {
	logMethodName(__func__);
	return 0;
}

Optional<UID> FakePeekCursor::getPrimaryPeekLocation() const {
	logMethodName(__func__);
	return Optional<UID>();
}

Optional<UID> FakePeekCursor::getCurrentPeekLocation() const {
	logMethodName(__func__);
	return Optional<UID>();
}

void FakePeekCursor::addref() {
	logMethodName(__func__);
	ReferenceCounted<FakePeekCursor>::addref();
}

void FakePeekCursor::delref() {
	logMethodName(__func__);
	ReferenceCounted<FakePeekCursor>::delref();
}

} // namespace ptxn::test

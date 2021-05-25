/*
 * MockPeekCursor.cpp
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

#include "MockPeekCursor.h"

const static bool LOG_METHOD_CALLS = false;
const static bool LOG_MESSAGES = true;

static void logMethodName(std::string methodName) {
	if (LOG_METHOD_CALLS) {
		std::cout << "MockLogSystem::" << methodName << std::endl;
	}
}

// Assume allVersionMessages is not empty.
MockPeekCursor::MockPeekCursor(const std::vector<OneVersionMessages>& allVersionMessages, const Arena& arena)
  : allVersionMessages(allVersionMessages), cursorVersion(allVersionMessages[0].first),
    cursorReader(arena, allVersionMessages[0].second[0].message, AssumeVersion(currentProtocolVersion)),
    cursorArena(arena) {
	logMethodName(__func__);
}

MockPeekCursor::MockPeekCursor(const std::vector<OneVersionMessages>& allVersionMessages,
                               size_t pVersion,
                               size_t pMessage,
                               const LogMessageVersion& version,
                               const Arena& arena)
  : allVersionMessages(allVersionMessages), pVersion(pVersion), pMessage(pMessage), cursorVersion(version),
    cursorReader(arena, allVersionMessages[0].second[0].message, AssumeVersion(currentProtocolVersion)),
    cursorArena(arena) {
	logMethodName(__func__);
}

Reference<ILogSystem::IPeekCursor> MockPeekCursor::cloneNoMore() {
	logMethodName(__func__);
	// Simply clone, ignore "no more"
	return makeReference<MockPeekCursor>(allVersionMessages, pVersion, pMessage, cursorVersion, cursorArena);
}

// Ignore setProtocolVersion. Always use IncludeVersion(). TODO: It hits segfault when copy construct if I use
// IncludeVersion().
void MockPeekCursor::setProtocolVersion(ProtocolVersion version) {
	logMethodName(__func__);
}

bool MockPeekCursor::hasMessage() const {
	logMethodName(__func__);
	return pVersion < allVersionMessages.size();
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
	cursorReader = ArenaReader(cursorArena, getMessage(), AssumeVersion(currentProtocolVersion));
	return &cursorReader;
}

StringRef MockPeekCursor::getMessage() {
	logMethodName(__func__);
	std::string message = allVersionMessages[pVersion].second[pMessage].message;
	if (LOG_MESSAGES) {
		std::cout << "MockPeekCursor gets message: " << StringRef(message).toHexString() << std::endl;
	}
	return message;
}

StringRef MockPeekCursor::getMessageWithTags() {
	logMethodName(__func__);
	return StringRef();
}

void MockPeekCursor::nextMessage() {
	logMethodName(__func__);
	if (pVersion < allVersionMessages.size()) {
		auto& curVersionMessages = allVersionMessages[pVersion].second;
		if (pMessage + 1 < curVersionMessages.size()) {
			pMessage++;
		} else {
			pVersion++;
			pMessage = 0;
			if (pVersion < allVersionMessages.size()) {
				cursorVersion.reset(allVersionMessages[pVersion].first);
			} else {
				ASSERT(pVersion == allVersionMessages.size());
				// ~If there is no next message, just use the last version, because it is the "smallest possible message
				// version which a subsequent message might have".~
				// If there is no next message, use the last version + 1.
				cursorVersion.reset(allVersionMessages[pVersion - 1].first + 1);
			}
		}
	}
	if (LOG_MESSAGES) {
		std::cout << "Next message pVersion: " << pVersion << ", pMessage: " << pMessage << std::endl;
	}
}

void MockPeekCursor::advanceTo(LogMessageVersion n) {
	logMethodName(__func__);
	// Just move to the end.
	// TODO: This works in our use case, but in the future we could change allVersionMessages to ordered map and keep
	//  track of the actual version rather than pVersion.
	pVersion = allVersionMessages.size();
}

Future<Void> MockPeekCursor::getMore(TaskPriority taskID) {
	logMethodName(__func__);
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
	return cursorVersion;
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

void MockPeekCursor::addref() {
	logMethodName(__func__);
	ReferenceCounted<MockPeekCursor>::addref();
}

void MockPeekCursor::delref() {
	logMethodName(__func__);
	ReferenceCounted<MockPeekCursor>::delref();
}

void MockPeekCursor::describe() {
	std::cout << "Describing MockPeekCursor at " << (void*)this << std::endl;
	std::cout << "allVersionMessages:" << std::endl;
	for (auto& oneVersionMessages : allVersionMessages) {
		auto& version = oneVersionMessages.first;
		auto& messages = oneVersionMessages.second;
		std::cout << "\tversion: " << version << std::endl;
		for (auto& message : messages) {
			std::cout << "\t\tmessage: " << StringRef(message.message).toHexString() << "\ttags: ";
			for (auto tag : message.tags) {
				std::cout << tag.toString() << " ";
			}
			std::cout << std::endl;
		}
	}
	std::cout << "pVersion: " << pVersion << std::endl;
	std::cout << "pMessage: " << pMessage << std::endl;
	std::cout << "LogMessageVersion: " << cursorVersion.toString() << std::endl;
	std::cout << "_arena: " << (void*)&cursorArena << std::endl;
}

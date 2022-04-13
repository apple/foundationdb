/*
 * LogSystemDiskQueueAdapter.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/IDiskQueue.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/LogSystemDiskQueueAdapter.h"
#include "fdbserver/Knobs.h"
#include "flow/actorcompiler.h" // has to be last include

class LogSystemDiskQueueAdapterImpl {
public:
	ACTOR static Future<Standalone<StringRef>> readNext(LogSystemDiskQueueAdapter* self, int bytes) {
		while (self->recoveryQueueDataSize < bytes) {
			if (self->recoveryLoc == self->logSystem->getEnd()) {
				// Recovery will be complete once the current recoveryQueue is consumed, so we no longer need
				// self->logSystem
				TraceEvent("PeekNextEnd")
				    .detail("Queue", self->recoveryQueue.size())
				    .detail("Bytes", bytes)
				    .detail("Loc", self->recoveryLoc)
				    .detail("End", self->logSystem->getEnd());
				self->logSystem.clear();
				break;
			}

			if (!self->cursor->hasMessage()) {
				loop {
					choose {
						when(wait(self->cursor->getMore())) { break; }
						when(wait(self->localityChanged)) {
							self->cursor = self->logSystem->peekTxs(
							    UID(),
							    self->recoveryLoc,
							    self->peekLocality ? self->peekLocality->get().primaryLocality : tagLocalityInvalid,
							    self->peekLocality ? self->peekLocality->get().knownCommittedVersion : invalidVersion,
							    self->totalRecoveredBytes == 0);
							self->localityChanged = self->peekLocality->onChange();
						}
						when(wait(delay(self->peekTypeSwitches == 0
						                    ? SERVER_KNOBS->DISK_QUEUE_ADAPTER_MIN_SWITCH_TIME
						                    : SERVER_KNOBS->DISK_QUEUE_ADAPTER_MAX_SWITCH_TIME))) {
							self->peekTypeSwitches++;
							if (self->peekTypeSwitches % 3 == 1) {
								self->cursor = self->logSystem->peekTxs(UID(),
								                                        self->recoveryLoc,
								                                        tagLocalityInvalid,
								                                        invalidVersion,
								                                        self->totalRecoveredBytes == 0);
								self->localityChanged = Never();
							} else if (self->peekTypeSwitches % 3 == 2) {
								self->cursor = self->logSystem->peekTxs(
								    UID(),
								    self->recoveryLoc,
								    self->peekLocality ? self->peekLocality->get().secondaryLocality
								                       : tagLocalityInvalid,
								    self->peekLocality ? self->peekLocality->get().knownCommittedVersion
								                       : invalidVersion,
								    self->totalRecoveredBytes == 0);
								self->localityChanged = self->peekLocality->onChange();
							} else {
								self->cursor = self->logSystem->peekTxs(
								    UID(),
								    self->recoveryLoc,
								    self->peekLocality ? self->peekLocality->get().primaryLocality : tagLocalityInvalid,
								    self->peekLocality ? self->peekLocality->get().knownCommittedVersion
								                       : invalidVersion,
								    self->totalRecoveredBytes == 0);
								self->localityChanged = self->peekLocality->onChange();
							}
						}
					}
				}
				TraceEvent("PeekNextGetMore")
				    .detail("Total", self->totalRecoveredBytes)
				    .detail("Queue", self->recoveryQueue.size())
				    .detail("Bytes", bytes)
				    .detail("Loc", self->recoveryLoc)
				    .detail("End", self->logSystem->getEnd())
				    .detail("HasMessage", self->cursor->hasMessage())
				    .detail("Version", self->cursor->version().version);

				if (self->cursor->popped() != 0 || (!self->hasDiscardedData && BUGGIFY_WITH_PROB(0.01))) {
					TEST(true); // disk adapter reset
					TraceEvent(SevWarnAlways, "DiskQueueAdapterReset").detail("Version", self->cursor->popped());
					self->recoveryQueue.clear();
					self->recoveryQueueDataSize = 0;
					self->recoveryLoc = self->cursor->popped();
					self->recoveryQueueLoc = self->recoveryLoc;
					self->totalRecoveredBytes = 0;
					if (self->peekTypeSwitches % 3 == 1) {
						self->cursor = self->logSystem->peekTxs(
						    UID(), self->recoveryLoc, tagLocalityInvalid, invalidVersion, true);
						self->localityChanged = Never();
					} else if (self->peekTypeSwitches % 3 == 2) {
						self->cursor = self->logSystem->peekTxs(
						    UID(),
						    self->recoveryLoc,
						    self->peekLocality ? self->peekLocality->get().secondaryLocality : tagLocalityInvalid,
						    self->peekLocality ? self->peekLocality->get().knownCommittedVersion : invalidVersion,
						    true);
						self->localityChanged = self->peekLocality->onChange();
					} else {
						self->cursor = self->logSystem->peekTxs(
						    UID(),
						    self->recoveryLoc,
						    self->peekLocality ? self->peekLocality->get().primaryLocality : tagLocalityInvalid,
						    self->peekLocality ? self->peekLocality->get().knownCommittedVersion : invalidVersion,
						    true);
						self->localityChanged = self->peekLocality->onChange();
					}
					self->hasDiscardedData = true;
					throw disk_adapter_reset();
				}

				if (self->recoveryQueueDataSize == 0) {
					self->recoveryQueueLoc = self->recoveryLoc;
				}
				if (!self->cursor->hasMessage()) {
					self->recoveryLoc = self->cursor->version().version;
					wait(yield());
					continue;
				}
			}

			self->recoveryQueue.push_back(Standalone<StringRef>(self->cursor->getMessage(), self->cursor->arena()));
			self->recoveryQueueDataSize += self->recoveryQueue.back().size();
			self->totalRecoveredBytes += self->recoveryQueue.back().size();
			self->cursor->nextMessage();
			if (!self->cursor->hasMessage())
				self->recoveryLoc = self->cursor->version().version;

			//TraceEvent("PeekNextResults").detail("From", self->recoveryLoc).detail("Queue", self->recoveryQueue.size()).detail("Bytes", bytes).detail("Has", self->cursor->hasMessage()).detail("End", self->logSystem->getEnd());
		}
		if (self->recoveryQueue.size() > 1) {
			self->recoveryQueue[0] = concatenate(self->recoveryQueue.begin(), self->recoveryQueue.end());
			self->recoveryQueue.resize(1);
		}

		if (self->recoveryQueueDataSize == 0)
			return Standalone<StringRef>();

		ASSERT(self->recoveryQueue[0].size() == self->recoveryQueueDataSize);

		//TraceEvent("PeekNextReturn").detail("Bytes", bytes).detail("QueueSize", self->recoveryQueue.size());
		bytes = std::min(bytes, self->recoveryQueue[0].size());
		Standalone<StringRef> result(self->recoveryQueue[0].substr(0, bytes), self->recoveryQueue[0].arena());
		self->recoveryQueue[0].contents() = self->recoveryQueue[0].substr(bytes);
		self->recoveryQueueDataSize = self->recoveryQueue[0].size();
		if (self->recoveryQueue[0].size() == 0) {
			self->recoveryQueue.clear();
		}
		return result;
	}
};

Future<Standalone<StringRef>> LogSystemDiskQueueAdapter::readNext(int bytes) {
	if (!enableRecovery)
		return Standalone<StringRef>();
	return LogSystemDiskQueueAdapterImpl::readNext(this, bytes);
}

IDiskQueue::location LogSystemDiskQueueAdapter::getNextReadLocation() const {
	return IDiskQueue::location(0, recoveryQueueLoc);
}

IDiskQueue::location LogSystemDiskQueueAdapter::push(StringRef contents) {
	while (contents.size()) {
		int remainder = pushedData.size() == 0 ? 0 : pushedData.back().capacity() - pushedData.back().size();

		if (remainder == 0) {
			VectorRef<uint8_t> block;
			block.reserve(pushedData.arena(), SERVER_KNOBS->LOG_SYSTEM_PUSHED_DATA_BLOCK_SIZE);
			pushedData.push_back(pushedData.arena(), block);
			remainder = block.capacity();
		}

		pushedData.back().append(pushedData.arena(), contents.begin(), std::min(remainder, contents.size()));
		contents = contents.substr(std::min(remainder, contents.size()));
	}

	return IDiskQueue::location(0, nextCommit);
}

void LogSystemDiskQueueAdapter::pop(location upTo) {
	ASSERT(upTo.hi == 0);
	poppedUpTo = std::max(upTo.lo, poppedUpTo);
}

Future<Void> LogSystemDiskQueueAdapter::commit() {
	ASSERT(!commitMessages.empty());

	auto promise = commitMessages.front();
	commitMessages.pop_front();

	CommitMessage cm;
	cm.messages = this->pushedData;
	this->pushedData = Standalone<VectorRef<VectorRef<uint8_t>>>();
	cm.popTo = poppedUpTo;
	promise.send(cm);

	return cm.acknowledge.getFuture();
}

Future<Void> LogSystemDiskQueueAdapter::getError() const {
	return Void();
}

Future<Void> LogSystemDiskQueueAdapter::onClosed() const {
	return Void();
}

void LogSystemDiskQueueAdapter::dispose() {
	delete this;
}

void LogSystemDiskQueueAdapter::close() {
	delete this;
}

Future<LogSystemDiskQueueAdapter::CommitMessage> LogSystemDiskQueueAdapter::getCommitMessage() {
	Promise<CommitMessage> pcm;
	commitMessages.push_back(pcm);
	return pcm.getFuture();
}

LogSystemDiskQueueAdapter* openDiskQueueAdapter(Reference<ILogSystem> logSystem,
                                                Reference<AsyncVar<PeekTxsInfo>> peekLocality,
                                                Version txsPoppedVersion) {
	return new LogSystemDiskQueueAdapter(logSystem, peekLocality, txsPoppedVersion, true);
}

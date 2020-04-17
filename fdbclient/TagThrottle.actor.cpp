/*
 * TagThrottle.actor.cpp
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

#include "fdbclient/TagThrottle.h"
#include "fdbclient/MasterProxyInterface.h"
#include "fdbclient/DatabaseContext.h"

#include "flow/actorcompiler.h" // has to be last include

void TagSet::addTag(TransactionTagRef tag) {
	ASSERT(CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH < 256); // Tag length is encoded with a single byte

	if(tag.size() > CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH) {
		throw tag_too_long();
	}
	if(tags.size() >= CLIENT_KNOBS->MAX_TAGS_PER_TRANSACTION) {
		throw too_many_tags();
	}

	tags.insert(TransactionTagRef(arena, tag));
	bytes += tag.size();
}

size_t TagSet::size() {
	return tags.size();
}

namespace ThrottleApi {
	Priority priorityFromReadVersionFlags(int flags) {
		if((flags & GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE) == GetReadVersionRequest::PRIORITY_SYSTEM_IMMEDIATE) {
			return Priority::IMMEDIATE;
		}
		else if((flags & GetReadVersionRequest::PRIORITY_DEFAULT) == GetReadVersionRequest::PRIORITY_DEFAULT) {
			return Priority::DEFAULT;
		}
		else if((flags & GetReadVersionRequest::PRIORITY_BATCH) == GetReadVersionRequest::PRIORITY_BATCH) {
			return Priority::BATCH;
		}

		ASSERT(false);
		throw internal_error();
	}

	const char* priorityToString(Priority priority, bool capitalize) {
		switch(priority) {
			case Priority::BATCH:
				return capitalize ? "Batch" : "batch";
			case Priority::DEFAULT:
				return capitalize ? "Default" : "default";
			case Priority::IMMEDIATE:
				return capitalize ? "Immediate" : "immediate";
		}

		ASSERT(false);
		throw internal_error();
	}

	void signalThrottleChange(Transaction &tr) {
		tr.atomicOp(tagThrottleSignalKey, LiteralStringRef("XXXXXXXXXX\x00\x00\x00\x00"), MutationRef::SetVersionstampedValue);
	}

	// Format for throttle key:
	//
	// tagThrottleKeysPrefix + [tag list]
	// tag list consists of 1 or more consecutive tags, each encoded as:
	// tag.size() (1 byte) + tag's bytes. For example, tag 'foo' is: \x03foo
	// The tags are listed in sorted order
	//
	// Currently, the throttle API supports only 1 tag per throttle
	Key throttleKeyForTags(std::set<TransactionTagRef> const& tags) {
		ASSERT(CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH < 256);
		ASSERT(tags.size() > 0);

		ASSERT(tags.size() == 1); // TODO: support multiple tags per throttle

		int size = tagThrottleKeysPrefix.size() + tags.size();
		for(auto tag : tags) {
			ASSERT(tag.size() <= CLIENT_KNOBS->MAX_TRANSACTION_TAG_LENGTH);
			size += tag.size();
		}

		Key result;

		uint8_t* str = new (result.arena()) uint8_t[size];
		result.contents() = StringRef(str, size);

		memcpy(str, tagThrottleKeysPrefix.begin(), tagThrottleKeysPrefix.size());
		str += tagThrottleKeysPrefix.size();

		for(auto tag : tags) {
			*(str++) = (uint8_t)tag.size();
			if(tag.size() > 0) {
				memcpy(str, tag.begin(), tag.size());
				str += tag.size();
			}
		}

		return result;
	}

	TransactionTagRef tagFromThrottleKey(KeyRef key) {
		TransactionTagRef tag = key.substr(tagThrottleKeysPrefix.size()+1);
		ASSERT(tag.size() == key.begin()[tagThrottleKeysPrefix.size()]); // TODO: support multiple tags per throttle
		return tag;
	}

	ACTOR Future<std::map<TransactionTag, TagThrottleInfo>> getTags(Database db, int limit) {
		state Transaction tr(db);

		loop {
			try {
				Standalone<RangeResultRef> tags = wait(tr.getRange(tagThrottleKeys, limit));
				std::map<TransactionTag, TagThrottleInfo> results;
				for(auto tag : tags) {
					results[tagFromThrottleKey(tag.key)] = decodeTagThrottleValue(tag.value);
				}
				return results;
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> throttleTag(Database db, TransactionTagRef tag, double tpsRate, double expiration, bool serializeExpirationAsDuration, bool autoThrottled) {
		state Transaction tr(db);
		state Key key = throttleKeyForTags(std::set<TransactionTagRef>{ tag });

		TagThrottleInfo throttle(tpsRate, expiration, autoThrottled, ThrottleApi::Priority::DEFAULT, serializeExpirationAsDuration);
		BinaryWriter wr(IncludeVersion());
		wr << throttle;
		state Value value = wr.toValue();

		loop {
			try {
				tr.set(key, value);

				if(!autoThrottled) {
					signalThrottleChange(tr);
				}

				wait(tr.commit());
				return Void();
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<bool> unthrottleTag(Database db, TransactionTagRef tag) {
		state Transaction tr(db);
		state Key key = throttleKeyForTags(std::set<TransactionTagRef>{ tag });

		loop {
			try {
				state Optional<Value> value = wait(tr.get(key));
				if(value.present()) {
					tr.clear(key);
					signalThrottleChange(tr);

					wait(tr.commit());
				}

				return value.present();
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<uint64_t> unthrottleTags(Database db, bool disableAuto, bool disableManual) {
		state Transaction tr(db);

		state KeySelector begin = firstGreaterOrEqual(tagThrottleKeys.begin);
		state KeySelectorRef end = firstGreaterOrEqual(tagThrottleKeys.end);

		state uint64_t unthrottledTags = 0;

		loop {
			try {
				state Standalone<RangeResultRef> tags = wait(tr.getRange(begin, end, 1000));
				state uint64_t localUnthrottledTags = 0;
				for(auto tag : tags) {
					bool autoThrottled = decodeTagThrottleValue(tag.value).autoThrottled;
					if(autoThrottled && disableAuto) {
						tr.clear(tag.key);
						++localUnthrottledTags;
					}
					else if(!autoThrottled && disableManual) {
						tr.clear(tag.key);
						++localUnthrottledTags;
					}
				}

				if(localUnthrottledTags > 0) {
					signalThrottleChange(tr);
				}

				wait(tr.commit());

				unthrottledTags += localUnthrottledTags;

				if(!tags.more) {
					return unthrottledTags;
				}

				ASSERT(tags.size() > 0);
				begin = KeySelector(firstGreaterThan(tags[tags.size()-1].key), tags.arena());
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}	

	Future<uint64_t> unthrottleManual(Database db) {
		return unthrottleTags(db, false, true);	
	}

	Future<uint64_t> unthrottleAuto(Database db) {
		return unthrottleTags(db, true, false);	
	}

	Future<uint64_t> unthrottleAll(Database db) {
		return unthrottleTags(db, true, true);	
	}

	ACTOR Future<Void> enableAuto(Database db, bool enabled) {
		state Transaction tr(db);

		loop {
			try {
				Optional<Value> value = wait(tr.get(tagThrottleAutoEnabledKey));
				if(!value.present() || (enabled && value.get() != LiteralStringRef("1") || (!enabled && value.get() != LiteralStringRef("0")))) {
					tr.set(tagThrottleAutoEnabledKey, LiteralStringRef(enabled ? "1" : "0"));
					signalThrottleChange(tr);

					wait(tr.commit());
				}
				return Void();
			}
			catch(Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	TagThrottleInfo decodeTagThrottleValue(const ValueRef& value) {
		TagThrottleInfo throttleInfo;
		BinaryReader reader(value, IncludeVersion());
		reader >> throttleInfo;
		return throttleInfo;
	}
}
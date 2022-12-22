/*
  * KeyBackedConfig.actor.h
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
#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_KEYBACKEDCONFIG_G_H)
#define FDBCLIENT_KEYBACKEDCONFIG_G_H
#include "fdbclient/KeyBackedConfig.actor.g.h"
#elif !defined(FDBCLIENT_KEYBACKEDCONFIG_ACTOR_H)
#define FDBCLIENT_KEYBACKEDCONFIG_ACTOR_H
#include "fdbclient/TaskBucket.h"
#include "fdbclient/KeyBackedTypes.h"
#include "flow/actorcompiler.h" // has to be last include

FDB_DECLARE_BOOLEAN_PARAM(SetValidation);

// Key backed tags are a single-key slice of the TagUidMap, defined below.
// The Value type of the key is a UidAndAbortedFlagT which is a pair of {UID, aborted_flag}
// All tasks on the UID will have a validation key/value that requires aborted_flag to be
// false, so changing that value, such as changing the UID or setting aborted_flag to true,
// will kill all of the active tasks on that backup/restore UID.
typedef std::pair<UID, bool> UidAndAbortedFlagT;
class KeyBackedTag : public KeyBackedProperty<UidAndAbortedFlagT> {
public:
	KeyBackedTag() : KeyBackedProperty(StringRef()) {}
	KeyBackedTag(std::string tagName, StringRef tagMapPrefix);

	Future<Void> cancel(Reference<ReadYourWritesTransaction> tr) {
		std::string tag = tagName;
		Key _tagMapPrefix = tagMapPrefix;
		return map(get(tr), [tag, _tagMapPrefix, tr](Optional<UidAndAbortedFlagT> up) -> Void {
			if (up.present()) {
				// Set aborted flag to true
				up.get().second = true;
				KeyBackedTag(tag, _tagMapPrefix).set(tr, up.get());
			}
			return Void();
		});
	}

	std::string tagName;
	Key tagMapPrefix;
};

typedef KeyBackedMap<std::string, UidAndAbortedFlagT> TagMap;
// Map of tagName to {UID, aborted_flag} located in the fileRestorePrefixRange keyspace.
class TagUidMap : public KeyBackedMap<std::string, UidAndAbortedFlagT> {
	ACTOR static Future<std::vector<KeyBackedTag>> getAll_impl(TagUidMap* tagsMap,
	                                                           Reference<ReadYourWritesTransaction> tr,
	                                                           Snapshot snapshot);

public:
	TagUidMap(const StringRef& prefix) : TagMap("tag->uid/"_sr.withPrefix(prefix)), prefix(prefix) {}

	Future<std::vector<KeyBackedTag>> getAll(Reference<ReadYourWritesTransaction> tr,
	                                         Snapshot snapshot = Snapshot::False) {
		return getAll_impl(this, tr, snapshot);
	}

	Key prefix;
};

class KeyBackedConfig {
public:
	static struct {
		static TaskParam<UID> uid() { return __FUNCTION__sr; }
	} TaskParams;

	KeyBackedConfig(StringRef prefix, UID uid = UID())
	  : uid(uid), prefix(prefix), configSpace(uidPrefixKey("uid->config/"_sr.withPrefix(prefix), uid)) {}

	KeyBackedConfig(StringRef prefix, Reference<Task> task) : KeyBackedConfig(prefix, TaskParams.uid().get(task)) {}

	Future<Void> toTask(Reference<ReadYourWritesTransaction> tr,
	                    Reference<Task> task,
	                    SetValidation setValidation = SetValidation::True) {
		// Set the uid task parameter
		TaskParams.uid().set(task, uid);

		if (!setValidation) {
			return Void();
		}

		// Set the validation condition for the task which is that the restore uid's tag's uid is the same as the
		// restore uid. Get this uid's tag, then get the KEY for the tag's uid but don't read it.  That becomes the
		// validation key which TaskBucket will check, and its value must be this restore config's uid.
		UID u = uid; // 'this' could be invalid in lambda
		Key p = prefix;
		return map(tag().get(tr), [u, p, task](Optional<std::string> const& tag) -> Void {
			if (!tag.present())
				throw restore_error();
			// Validation contition is that the uidPair key must be exactly {u, false}
			TaskBucket::setValidationCondition(
			    task, KeyBackedTag(tag.get(), p).key, TupleCodec<UidAndAbortedFlagT>::pack({ u, false }));
			return Void();
		});
	}

	KeyBackedProperty<std::string> tag() { return configSpace.pack(__FUNCTION__sr); }

	UID getUid() { return uid; }

	Key getUidAsKey() { return BinaryWriter::toValue(uid, Unversioned()); }

	void clear(Reference<ReadYourWritesTransaction> tr) { tr->clear(configSpace.range()); }

	// lastError is a pair of error message and timestamp expressed as an int64_t
	KeyBackedProperty<std::pair<std::string, Version>> lastError() { return configSpace.pack(__FUNCTION__sr); }

	KeyBackedMap<int64_t, std::pair<std::string, Version>> lastErrorPerType() {
		return configSpace.pack(__FUNCTION__sr);
	}

	// Updates the error per type map and the last error property
	Future<Void> updateErrorInfo(Database cx, Error e, std::string message) {
		// Avoid capture of this ptr
		auto& copy = *this;

		return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) mutable {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return map(tr->getReadVersion(), [=](Version v) mutable {
				copy.lastError().set(tr, { message, v });
				copy.lastErrorPerType().set(tr, e.code(), { message, v });
				return Void();
			});
		});
	}

protected:
	UID uid;
	Key prefix;
	Subspace configSpace;
};

#include "flow/unactorcompiler.h"
#endif //FOUNDATIONDB_KEYBACKEDCONFIG_H

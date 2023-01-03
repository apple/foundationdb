/*
 * UpdateWorkerList.actor.cpp
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

#include "fdbclient/SystemData.h"
#include "fdbserver/UpdateWorkerList.h"
#include "flow/actorcompiler.h" // must be last include

class UpdateWorkerListImpl {
public:
	ACTOR static Future<Void> update(UpdateWorkerList* self, Database db) {
		// The Database we are using is based on worker registrations to this cluster controller, which come only
		// from master servers that we started, so it shouldn't be possible for multiple cluster controllers to
		// fight.
		state Transaction tr(db);
		loop {
			try {
				tr.clear(workerListKeys);
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		loop {
			tr.reset();

			// Wait for some changes
			while (!self->anyDelta.get())
				wait(self->anyDelta.onChange());
			self->anyDelta.set(false);

			state std::map<Optional<Standalone<StringRef>>, Optional<ProcessData>> delta;
			delta.swap(self->delta);

			TraceEvent("UpdateWorkerList").detail("DeltaCount", delta.size());

			// Do a transaction to write the changes
			loop {
				try {
					for (auto w = delta.begin(); w != delta.end(); ++w) {
						if (w->second.present()) {
							tr.set(workerListKeyFor(w->first.get()), workerListValue(w->second.get()));
						} else
							tr.clear(workerListKeyFor(w->first.get()));
					}
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}
};

Future<Void> UpdateWorkerList::init(Database const& db) {
	return UpdateWorkerListImpl::update(this, db);
}

void UpdateWorkerList::set(Optional<Standalone<StringRef>> processID, Optional<ProcessData> data) {
	delta[processID] = data;
	anyDelta.set(true);
}

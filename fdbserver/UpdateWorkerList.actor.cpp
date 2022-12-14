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

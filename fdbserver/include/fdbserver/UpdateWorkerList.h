#pragma once

#include "fdbclient/NativeAPI.actor.h"
#include "fdbrpc/Locality.h"
#include "flow/genericactors.actor.h"

class UpdateWorkerList {
	friend class UpdateWorkerListImpl;
	std::map<Optional<Standalone<StringRef>>, Optional<ProcessData>> delta;
	AsyncVar<bool> anyDelta;

public:
	Future<Void> init(Database const& db);
	void set(Optional<Standalone<StringRef>> processID, Optional<ProcessData> data);
};

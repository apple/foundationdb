#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"

#include "flow/actorcompiler.h" // has to be last include

ICheckpointReader* newCheckpointReader(const CheckpointMetaData& checkpoint, UID logID) {
	if (checkpoint.getFormat() == RocksDBColumnFamily) {
		return newRocksDBCheckpointReader(checkpoint, logID);
	} else if (checkpoint.getFormat() == RocksDB) {
		throw not_implemented();
	} else {
		ASSERT(false);
	}

	return nullptr;
}

ACTOR Future<Void> deleteCheckpoint(CheckpointMetaData checkpoint) {
	wait(delay(0, TaskPriority::FetchKeys));

	if (checkpoint.getFormat() == RocksDBColumnFamily) {
		wait(deleteRocksCFCheckpoint(checkpoint));
	} else if (checkpoint.getFormat() == RocksDB) {
		throw not_implemented();
	} else {
		ASSERT(false);
	}

	return Void();
}

ACTOR Future<CheckpointMetaData> fetchCheckpoint(Database cx,
                                                 CheckpointMetaData initialState,
                                                 std::string dir,
                                                 std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	state CheckpointMetaData result;
	if (initialState.getFormat() == RocksDBColumnFamily) {
		CheckpointMetaData _result = wait(fetchRocksDBCheckpoint(cx, initialState, dir, cFun));
		result = _result;
	} else if (initialState.getFormat() == RocksDB) {
		throw not_implemented();
	} else {
		ASSERT(false);
	}

	return result;
}
#include "fdbserver/ServerCheckpoint.actor.h"
#include "fdbserver/RocksDBCheckpointUtils.actor.h"

#include "flow/actorcompiler.h" // has to be last include

ICheckpointReader* newCheckpointReader(const CheckpointMetaData& checkpoint, UID logID) {
	if (checkpoint.getFormat() == RocksDBColumnFamily) {
		return newRocksDBCheckpointReader(checkpoint, logID);
	} else if (checkpoint.getFormat() == RocksDB) {
		return newRocksDBCheckpointReader(checkpoint, logID);
	} else {
		throw not_implemented();
	}

	return nullptr;
}

ACTOR Future<Void> deleteCheckpoint(CheckpointMetaData checkpoint) {
	wait(delay(0, TaskPriority::FetchKeys));
	state CheckpointFormat format = checkpoint.getFormat();
	if (format == RocksDBColumnFamily || format == RocksDB) {
		wait(deleteRocksCFCheckpoint(checkpoint));
	} else {
		throw not_implemented();
	}

	return Void();
}

ACTOR Future<CheckpointMetaData> fetchCheckpoint(Database cx,
                                                 CheckpointMetaData initialState,
                                                 std::string dir,
                                                 std::function<Future<Void>(const CheckpointMetaData&)> cFun) {
	state CheckpointMetaData result;
	const CheckpointFormat format = initialState.getFormat();
	if (format == RocksDBColumnFamily || format == RocksDB) {
		CheckpointMetaData _result = wait(fetchRocksDBCheckpoint(cx, initialState, dir, cFun));
		result = _result;
	} else {
		throw not_implemented();
	}

	return result;
}
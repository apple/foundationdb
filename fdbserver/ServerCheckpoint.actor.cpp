#include "fdbserver/ServerCheckpoint.actor.h"

#include "flow/actorcompiler.h" // has to be last include

namespace {
ACTOR Future<Void> deleteRocksCFCheckpoint(CheckpointMetaData checkpoint) {
	ASSERT_EQ(checkpoint.getFormat(), RocksDBColumnFamily);
	ObjectReader reader(checkpoint.serializedCheckpoint.begin(), IncludeVersion());
	RocksDBColumnFamilyCheckpoint rocksCF;
	reader.deserialize(rocksCF);

	state std::unordered_set<std::string> dirs;
	for (const LiveFileMetaData& file : rocksCF.sstFiles) {
		dirs.insert(file.db_path);
	}

    state std::unordered_set<std::string>::iterator it = dirs.begin();
	for (; it != dirs.end(); ++it) {
		const std::string dir = *it;
		platform::eraseDirectoryRecursive(dir);
		TraceEvent("DeleteCheckpointRemovedDir", checkpoint.checkpointID)
		    .detail("CheckpointID", checkpoint.checkpointID)
		    .detail("Dir", dir);
		wait(delay(0, TaskPriority::FetchKeys));
	}
	return Void();
}

} // namespace

ACTOR Future<Void> deleteCheckpoint(CheckpointMetaData checkpoint) {
	wait(delay(0, TaskPriority::FetchKeys));
	if (checkpoint.getFormat() == RocksDBColumnFamily) {
		wait(deleteRocksCFCheckpoint(checkpoint));
	} else {
		throw not_implemented();
	}
    return Void();
}
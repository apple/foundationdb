#include "fdbserver/logsystem/LogSystemFactory.h"
#include "fdbserver/logsystem/TagPartitionedLogSystem.h"

Future<Void> recoverAndEndLogSystemEpoch(Reference<AsyncVar<Reference<TagPartitionedLogSystem>>> const& outLogSystem,
                                         UID const& dbgid,
                                         DBCoreState const& oldState,
                                         FutureStream<TLogRejoinRequest> const& rejoins,
                                         LocalityData const& locality,
                                         bool* forceRecovery) {
	return TagPartitionedLogSystem::recoverAndEndEpoch(outLogSystem, dbgid, oldState, rejoins, locality, forceRecovery);
}

Reference<TagPartitionedLogSystem> makeLogSystemFromLogSystemConfig(UID const& dbgid,
                                                                    LocalityData const& locality,
                                                                    LogSystemConfig const& conf,
                                                                    bool excludeRemote,
                                                                    bool useRecoveredAt,
                                                                    Optional<PromiseStream<Future<Void>>> addActor) {
	if (conf.logSystemType == LogSystemType::empty) {
		return Reference<TagPartitionedLogSystem>();
	} else if (conf.logSystemType == LogSystemType::tagPartitioned) {
		return TagPartitionedLogSystem::fromLogSystemConfig(
		    dbgid, locality, conf, excludeRemote, useRecoveredAt, addActor);
	}
	throw internal_error();
}

Reference<TagPartitionedLogSystem> makeOldLogSystemFromLogSystemConfig(UID const& dbgid,
                                                                       LocalityData const& locality,
                                                                       LogSystemConfig const& conf) {
	if (conf.logSystemType == LogSystemType::empty) {
		return Reference<TagPartitionedLogSystem>();
	} else if (conf.logSystemType == LogSystemType::tagPartitioned) {
		return TagPartitionedLogSystem::fromOldLogSystemConfig(dbgid, locality, conf);
	}
	throw internal_error();
}

Reference<TagPartitionedLogSystem> makeLogSystemFromServerDBInfo(UID const& dbgid,
                                                                 ServerDBInfo const& dbInfo,
                                                                 bool useRecoveredAt,
                                                                 Optional<PromiseStream<Future<Void>>> addActor) {
	return makeLogSystemFromLogSystemConfig(
	    dbgid, dbInfo.myLocality, dbInfo.logSystemConfig, false, useRecoveredAt, addActor);
}

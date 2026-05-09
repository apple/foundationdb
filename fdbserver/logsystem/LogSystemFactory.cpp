#include "fdbserver/logsystem/LogSystemFactory.h"
#include "fdbserver/logsystem/LogSystem.h"

Future<Void> recoverAndEndLogSystemEpoch(Reference<AsyncVar<Reference<LogSystem>>> const& outLogSystem,
                                         UID const& dbgid,
                                         DBCoreState const& oldState,
                                         FutureStream<TLogRejoinRequest> const& rejoins,
                                         LocalityData const& locality,
                                         bool* forceRecovery) {
	return LogSystem::recoverAndEndEpoch(outLogSystem, dbgid, oldState, rejoins, locality, forceRecovery);
}

Reference<LogSystem> makeLogSystemFromLogSystemConfig(UID const& dbgid,
                                                      LocalityData const& locality,
                                                      LogSystemConfig const& conf,
                                                      bool excludeRemote,
                                                      bool useRecoveredAt,
                                                      Optional<PromiseStream<Future<Void>>> addActor) {
	if (conf.logSystemType == LogSystemType::empty) {
		return Reference<LogSystem>();
	} else if (conf.logSystemType == LogSystemType::tagPartitioned) {
		return LogSystem::fromLogSystemConfig(dbgid, locality, conf, excludeRemote, useRecoveredAt, addActor);
	}
	throw internal_error();
}

Reference<LogSystem> makeOldLogSystemFromLogSystemConfig(UID const& dbgid,
                                                         LocalityData const& locality,
                                                         LogSystemConfig const& conf) {
	if (conf.logSystemType == LogSystemType::empty) {
		return Reference<LogSystem>();
	} else if (conf.logSystemType == LogSystemType::tagPartitioned) {
		return LogSystem::fromOldLogSystemConfig(dbgid, locality, conf);
	}
	throw internal_error();
}

Reference<LogSystem> makeLogSystemFromServerDBInfo(UID const& dbgid,
                                                   ServerDBInfo const& dbInfo,
                                                   bool useRecoveredAt,
                                                   Optional<PromiseStream<Future<Void>>> addActor) {
	return makeLogSystemFromLogSystemConfig(
	    dbgid, dbInfo.myLocality, dbInfo.logSystemConfig, false, useRecoveredAt, addActor);
}

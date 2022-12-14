#pragma once

class ClusterControllerSingletonRecruiter {
	// recruitX is used to signal when role X needs to be (re)recruited.
	// recruitingXID is used to track the ID of X's interface which is being recruited.
	// We use AsyncVars to kill (i.e. halt) singletons that have been replaced.
	double lastRecruitTime = 0;
	AsyncVar<bool> recruitDistributor;
	Optional<UID> recruitingDistributorID;
	AsyncVar<bool> recruitRatekeeper;
	Optional<UID> recruitingRatekeeperID;
	AsyncVar<bool> recruitBlobManager;
	Optional<UID> recruitingBlobManagerID;
	AsyncVar<bool> recruitBlobMigrator;
	Optional<UID> recruitingBlobMigratorID;
	AsyncVar<bool> recruitEncryptKeyProxy;
	Optional<UID> recruitingEncryptKeyProxyID;
	AsyncVar<bool> recruitConsistencyScan;
	Optional<UID> recruitingConsistencyScanID;

public:
	setRatekeepeInterface(ClusterControllerRatekeeperInterface interface) {
		TraceEvent("CCRK_SetInf", cc.getId()).detail("Id", interface.get().id());
		cc.db.setRatekeeper(interface.get());
	}
};

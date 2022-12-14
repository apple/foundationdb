#pragma once

#include "fdbrpc/Locality.h"

// Allows the comparison of two different recruitments to determine which one is better
// Tlog recruitment is different from all the other roles, in that it avoids degraded processes
// And tried to avoid recruitment in the same DC as the cluster controller
struct RoleFitness {
	ProcessClass::Fitness bestFit;
	ProcessClass::Fitness worstFit;
	ProcessClass::ClusterRole role;
	int count;
	int worstUsed = 1;
	bool degraded = false;

	RoleFitness(int bestFit, int worstFit, int count, ProcessClass::ClusterRole role)
	  : bestFit((ProcessClass::Fitness)bestFit), worstFit((ProcessClass::Fitness)worstFit), role(role), count(count) {}

	RoleFitness(int fitness, int count, ProcessClass::ClusterRole role)
	  : bestFit((ProcessClass::Fitness)fitness), worstFit((ProcessClass::Fitness)fitness), role(role), count(count) {}

	RoleFitness()
	  : bestFit(ProcessClass::NeverAssign), worstFit(ProcessClass::NeverAssign), role(ProcessClass::NoRole), count(0) {}

	RoleFitness(const std::vector<WorkerDetails>& workers,
	            ProcessClass::ClusterRole role,
	            const std::map<Optional<Standalone<StringRef>>, int>& id_used)
	  : role(role) {
		// Every recruitment will attempt to recruit the preferred amount through GoodFit,
		// So a recruitment which only has BestFit is not better than one that has a GoodFit process
		worstFit = ProcessClass::GoodFit;
		degraded = false;
		bestFit = ProcessClass::NeverAssign;
		worstUsed = 1;
		for (auto& it : workers) {
			auto thisFit = it.processClass.machineClassFitness(role);
			auto thisUsed = id_used.find(it.interf.locality.processId());

			if (thisUsed == id_used.end()) {
				TraceEvent(SevError, "UsedNotFound").detail("ProcessId", it.interf.locality.processId().get());
				ASSERT(false);
			}
			if (thisUsed->second == 0) {
				TraceEvent(SevError, "UsedIsZero").detail("ProcessId", it.interf.locality.processId().get());
				ASSERT(false);
			}

			bestFit = std::min(bestFit, thisFit);

			if (thisFit > worstFit) {
				worstFit = thisFit;
				worstUsed = thisUsed->second;
			} else if (thisFit == worstFit) {
				worstUsed = std::max(worstUsed, thisUsed->second);
			}
			degraded = degraded || it.degraded;
		}

		count = workers.size();

		// degraded is only used for recruitment of tlogs
		if (role != ProcessClass::TLog) {
			degraded = false;
		}
	}

	bool operator<(RoleFitness const& r) const {
		if (worstFit != r.worstFit)
			return worstFit < r.worstFit;
		if (worstUsed != r.worstUsed)
			return worstUsed < r.worstUsed;
		if (count != r.count)
			return count > r.count;
		if (degraded != r.degraded)
			return r.degraded;
		// FIXME: TLog recruitment process does not guarantee the best fit is not worsened.
		if (role != ProcessClass::TLog && role != ProcessClass::LogRouter && bestFit != r.bestFit)
			return bestFit < r.bestFit;
		return false;
	}
	bool operator>(RoleFitness const& r) const { return r < *this; }
	bool operator<=(RoleFitness const& r) const { return !(*this > r); }
	bool operator>=(RoleFitness const& r) const { return !(*this < r); }

	bool betterCount(RoleFitness const& r) const {
		if (count > r.count)
			return true;
		if (worstFit != r.worstFit)
			return worstFit < r.worstFit;
		if (worstUsed != r.worstUsed)
			return worstUsed < r.worstUsed;
		if (degraded != r.degraded)
			return r.degraded;
		return false;
	}

	bool operator==(RoleFitness const& r) const {
		return worstFit == r.worstFit && worstUsed == r.worstUsed && bestFit == r.bestFit && count == r.count &&
		       degraded == r.degraded;
	}

	std::string toString() const { return format("%d %d %d %d %d", worstFit, worstUsed, count, degraded, bestFit); }
};

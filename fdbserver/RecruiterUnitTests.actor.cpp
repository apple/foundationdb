/*
 * RecruiterUnitTests.actor.cpp
 */

#include "fdbserver/ClusterController.actor.h"
#include "fdbserver/CoordinationInterface.h"
#include "fdbserver/Recruiter.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // must be last include

namespace {

static int workers = 0;

// Adds a worker with the specified process class to the given
// ClusterControllerData's list of workers. This function simulates the
// behavior of `registerWorker`, and is used only for testing purposes.
void addWorker(ClusterControllerData& data, ProcessClass::ClassType classType) {
	Standalone<StringRef> id = StringRef(std::to_string(workers++));

	data.id_worker[id] = WorkerInfo();
	data.id_worker[id].details = WorkerDetails();
	data.id_worker[id].details.recoveredDiskFiles = true;

	data.id_worker[id].details.interf.locality.set(LocalityData::keyDcId, "1"_sr);
	// data.id_worker[id].details.interf.locality.set("data_hall"_sr, "1"_sr);
	// data.id_worker[id].details.interf.locality.set("rack"_sr, "1"_sr);
	// data.id_worker[id].details.interf.locality.set("zoneid"_sr, "1"_sr);
	// data.id_worker[id].details.interf.locality.set(LocalityData::keyMachineId, "1"_sr);
	data.id_worker[id].details.interf.locality.set(LocalityData::keyProcessId, id);

	data.id_worker[id].details.processClass = ProcessClass(classType, ProcessClass::ClassSource::CommandLineSource);
}

void addWorkers(ClusterControllerData& data, ProcessClass::ClassType classType, int count) {
	for (int i = 0; i < count; ++i) {
		addWorker(data, classType);
	}
}

// Returns the number of unique stateless processes the workers in `workers`
// are running on.
int uniqueStatelessProcesses(WorkerRecruitment const& workers) {
	std::unordered_set<UID> ids;
	std::transform(workers.grvProxies.begin(),
	               workers.grvProxies.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerInterface const& w) {
		               printf("id: %s\n", w.id().toString().c_str());
		               return w.id();
	               });
	std::transform(workers.commitProxies.begin(),
	               workers.commitProxies.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerInterface const& w) {
		               printf("id: %s\n", w.id().toString().c_str());
		               return w.id();
	               });
	std::transform(workers.resolvers.begin(),
	               workers.resolvers.end(),
	               std::inserter(ids, ids.end()),
	               [](WorkerInterface const& w) {
		               printf("id: %s\n", w.id().toString().c_str());
		               return w.id();
	               });
	return ids.size();
}

} // namespace

TEST_CASE("/fdbserver/Recruiter/Stateless/GrvProxies") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "2"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(workers.grvProxies.size(), 2);
	ASSERT_EQ(workers.commitProxies.size(), 1);
	ASSERT_EQ(workers.resolvers.size(), 1);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/Stateless/CommitProxies") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "2"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 2);
	ASSERT_EQ(workers.resolvers.size(), 1);

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/Stateless/Resolvers") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "2"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 1);
	ASSERT_EQ(workers.resolvers.size(), 2);

	return Void();
}

// Run the cluster controller without explicitly setting its datacenter ID.
// This should result in a `no_more_servers` error when the recruited stateless
// processes (which do have a datacenter ID set) do not match the DC ID of the
// cluster controller.
TEST_CASE("/fdbserver/Recruiter/Stateless/CCNoDc") {
	DatabaseConfiguration conf;
	// conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	// conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "1"_sr));
	// conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "1"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData(), // don't set a dcID for the CC
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	try {
		auto workers = recruiter.findWorkers(&data, info, false);
		ASSERT(false);
	} catch (Error& e) {
		ASSERT_EQ(e.code(), error_code_no_more_servers);
	}

	return Void();
}

// Ensure we can recruit multiple unique stateless roles on a single process.
// Also checks that only one role of each type can be recruited on a process.
TEST_CASE("/fdbserver/Recruiter/Stateless/SingleProcessMultipleRoles") {
	// Configure the database to prefer two of each stateless role.
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "2"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "2"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "2"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, 1);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	// Check that only one of each role was actually recruited on the single
	// available process.
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 1);
	ASSERT_EQ(workers.resolvers.size(), 1);

	// Check that all roles are running on the same process.
	ASSERT_EQ(workers.grvProxies.front().id(), workers.commitProxies.front().id());
	ASSERT_EQ(workers.grvProxies.front().id(), workers.resolvers.front().id());

	return Void();
}

TEST_CASE("/fdbserver/Recruiter/Stateless/All") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "1"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "3"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "2"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, 6);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 3);
	ASSERT_EQ(workers.resolvers.size(), 2);

	// Make sure all roles were recruited on separate processes.
	ASSERT_EQ(uniqueStatelessProcesses(workers), 6);

	return Void();
}

// Even when asking for zero stateless processes, we should always get at least
// one.
TEST_CASE("/fdbserver/Recruiter/Stateless/Zero") {
	DatabaseConfiguration conf;
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/grv_proxies"_sr, "0"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/commit_proxies"_sr, "0"_sr));
	conf.applyMutation(MutationRef(MutationRef::SetValue, "\xff/conf/resolvers"_sr, "0"_sr));
	conf.test_setDefaultReplicationPolicy();

	ClusterControllerData data(ClusterControllerFullInterface(),
	                           LocalityData({}, {}, {}, "1"_sr),
	                           ServerCoordinators(),
	                           makeReference<AsyncVar<Optional<UID>>>());
	addWorkers(data, ProcessClass::ClassType::StatelessClass, 5);

	auto recruiter = Recruiter(UID());
	RecruitmentInfo info(conf, false, 0);
	auto workers = recruiter.findWorkers(&data, info, false);
	ASSERT_EQ(workers.grvProxies.size(), 1);
	ASSERT_EQ(workers.commitProxies.size(), 1);
	ASSERT_EQ(workers.resolvers.size(), 1);

	return Void();
}

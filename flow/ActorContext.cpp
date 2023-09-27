#include "flow/ActorContext.h"

#ifdef WITH_ACAC

#include <iomanip>
#include <iostream>
#include <mutex>

#include "flow/flow.h"
#include "libb64/encode.h"
#include "libb64/decode.h"

namespace {
std::vector<ActorExecutionContext> g_currentExecutionContext;
std::unordered_map<ActorID, ActiveActor> g_activeActors;

ActorID getActorID() {
	static thread_local ActorID actorID = INIT_ACTOR_ID;
	return ++actorID;
}

inline ActorID getActorSpawnerID() {
	if (g_currentExecutionContext.empty()) {
		return INIT_ACTOR_ID;
	}
	return g_currentExecutionContext.back().actorID;
}

inline bool isActorOnMainThread() {
	// The INetwork framework behaves differently in Net2 and Sim2.
	// For Net2, when Net2::run() is called, the N2::thread_network is set to be the current Net2 instance.
	// For Sim2, it tests if on main thread by calling the underlying Net2 instance, however, since Net2::run()
	// is never called, the N2::thread_network will always be nullptr. In this case, Sim2::isOnMainThread will always
	// return false and not reliable.
	if (g_network) [[likely]] {
		return g_network->isSimulated() ? true : g_network->isOnMainThread();
	} else {
		return false;
	}
}

} // anonymous namespace

using ActiveActorsCount_t = uint32_t;

ActiveActor::ActiveActor() : identifier(), id(), spawnTime(0.0), spawner(INVALID_ACTOR_ID) {}

ActiveActor::ActiveActor(const ActorIdentifier& identifier_, const ActorID& id_, const ActorID& spawnerID_)
  : identifier(identifier_), id(id_), spawnTime(g_network != nullptr ? g_network->now() : 0.0), spawner(spawnerID_) {}

ActiveActorHelper::ActiveActorHelper(const ActorIdentifier& actorIdentifier) {
	if (!isActorOnMainThread()) [[unlikely]] {
		return;
	}
	const auto actorID_ = getActorID();
	const auto spawnerActorID = getActorSpawnerID();
	actorID = actorID_;
	g_activeActors[actorID] = ActiveActor(actorIdentifier, actorID, spawnerActorID);
}

ActiveActorHelper::~ActiveActorHelper() {
	if (!isActorOnMainThread()) [[unlikely]] {
		return;
	}
	g_activeActors.erase(actorID);
}

ActorExecutionContextHelper::ActorExecutionContextHelper(const ActorID& actorID_,
                                                         const ActorBlockIdentifier& blockIdentifier_) {
	if (!isActorOnMainThread()) [[unlikely]] {
		return;
	}
	g_currentExecutionContext.emplace_back(actorID_, blockIdentifier_);
}

ActorExecutionContextHelper::~ActorExecutionContextHelper() {
	if (!isActorOnMainThread()) [[unlikely]] {
		return;
	}
	if (g_currentExecutionContext.empty()) [[unlikely]] {
		// This should not happen, abort the program if it happens.
		std::abort();
	}
	g_currentExecutionContext.pop_back();
}

// TODO: Rewrite this function for better display
void dumpActors(std::ostream& stream) {
	stream << "Current active ACTORs:" << std::endl;
	for (const auto& [actorID, activeActor] : g_activeActors) {
		stream << std::setw(10) << actorID << "  " << activeActor.identifier.toString() << std::endl;
		if (activeActor.spawner != INVALID_ACTOR_ID) {
			stream << "        Spawn by " << std::setw(10) << activeActor.spawner << std::endl;
		}
	}
}

namespace {

std::vector<ActiveActor> getCallBacktraceOfActor(const ActorID& actorID) {
	std::vector<ActiveActor> actorBacktrace;
	auto currentActorID = actorID;
	for (;;) {
		if (currentActorID == INIT_ACTOR_ID) {
			// Reaching the root
			break;
		}
		if (g_activeActors.count(currentActorID) == 0) {
			// TODO: Understand why this happens and react properly
			break;
		}
		actorBacktrace.push_back(g_activeActors.at(currentActorID));
		if (g_activeActors.at(currentActorID).spawner != INVALID_ACTOR_ID) {
			currentActorID = g_activeActors.at(currentActorID).spawner;
		} else {
			// TODO: Understand why the actor has no spawner ID
			break;
		}
	}
	return actorBacktrace;
}

} // anonymous namespace

void dumpActorCallBacktrace() {
	std::string backtrace = encodeActorContext(ActorContextDumpType::CURRENT_CALL_BACKTRACE);
	std::cout << backtrace << std::endl;
}

std::string encodeActorContext(const ActorContextDumpType dumpType) {
	BinaryWriter writer(Unversioned());
	auto writeActorInfo = [&writer](const ActiveActor& actor) {
		writer << actor.id << actor.identifier << actor.spawner;
	};

	writer << static_cast<uint8_t>(dumpType)
	       << (g_currentExecutionContext.empty() ? INVALID_ACTOR_ID : g_currentExecutionContext.back().actorID);

	switch (dumpType) {
	case ActorContextDumpType::FULL_CONTEXT:
		writer << static_cast<ActiveActorsCount_t>(g_activeActors.size());
		for (const auto& [actorID, activeActor] : g_activeActors) {
			writeActorInfo(activeActor);
		}
		break;
	case ActorContextDumpType::CURRENT_STACK:
		// Only current call stack
		{
			if (g_currentExecutionContext.empty()) {
				writer << static_cast<ActiveActorsCount_t>(0);
				break;
			}
			writer << static_cast<ActiveActorsCount_t>(g_currentExecutionContext.size());
			for (const auto& context : g_currentExecutionContext) {
				writeActorInfo(g_activeActors.at(context.actorID));
			}
		}
		break;
	case ActorContextDumpType::CURRENT_CALL_BACKTRACE:
		// The call backtrace of current active actor
		{
			if (g_currentExecutionContext.empty()) {
				writer << static_cast<ActiveActorsCount_t>(0);
				break;
			}
			const auto actors = getCallBacktraceOfActor(g_currentExecutionContext.back().actorID);
			writer << static_cast<ActiveActorsCount_t>(actors.size());
			for (const auto& item : actors) {
				writeActorInfo(item);
			}
		}
		break;
	default:
		UNREACHABLE();
	}

	const std::string data = writer.toValue().toString();
	return base64::encoder::from_string(data);
}

DecodedActorContext decodeActorContext(const std::string& caller) {
	DecodedActorContext result;
	const auto decoded = base64::decoder::from_string(caller);
	BinaryReader reader(decoded, Unversioned());

	std::underlying_type_t<ActorContextDumpType> dumpTypeRaw;
	reader >> dumpTypeRaw;
	result.dumpType = static_cast<ActorContextDumpType>(dumpTypeRaw);

	reader >> result.currentRunningActor;

	ActiveActorsCount_t actorCount;
	reader >> actorCount;

	std::unordered_map<ActorID, std::tuple<ActorID, ActorIdentifier, ActorID>> actors;
	for (ActiveActorsCount_t i = 0; i < actorCount; ++i) {
		ActorID id;
		ActorID spawner;
		ActorIdentifier identifier;
		reader >> id >> identifier >> spawner;
		result.context.emplace_back(id, identifier, spawner);
	}

	return result;
}

#endif // WITH_ACAC

#include "fdbclient/IdempotencyId.h"
#include "flow/UnitTest.h"

void forceLinkIdempotencyIdTests() {}

TEST_CASE("/fdbclient/IdempotencyId/basic") {
	Arena arena;
	std::unordered_set<IdempotencyId> idSet;
	std::vector<IdempotencyId> idVector;
	for (int i = 0; i < 5; ++i) {
		UID id = deterministicRandom()->randomUniqueID();
		idVector.emplace_back(id);
		idSet.emplace(id);
	}
	for (int i = 0; i < 5; ++i) {
		int length = deterministicRandom()->randomInt(16, 256);
		StringRef id = makeString(length, arena);
		deterministicRandom()->randomBytes(mutateString(id), length);
		idVector.emplace_back(id);
		idSet.emplace(id);
	}

	ASSERT(idSet.size() == idVector.size());
	for (const auto& id : idVector) {
		ASSERT(idSet.find(id) != idSet.end());
		idSet.erase(id);
		ASSERT(idSet.find(id) == idSet.end());
	}
	ASSERT(idSet.size() == 0);

	return Void();
}
/*
 * Buggify.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FLOW_BUGGIFY_H
#define FLOW_BUGGIFY_H

#pragma once

#include <map>
#include <unordered_map>

#include "flow/DeterministicRandom.h"
#include "flow/Trace.h"

extern class INetwork* g_network;
extern TraceBatch g_traceBatch;

inline double P_EXPENSIVE_VALIDATION{ 0.05 };

struct BuggifySection {
	const char* file;
	int line;

	bool operator==(const BuggifySection& rhs) const noexcept { return line == rhs.line && file == rhs.file; }
};

struct BuggifySectionHash {
	size_t operator()(const BuggifySection& section) const noexcept {
		return std::hash<const char*>{}(section.file) ^ std::hash<int>{}(section.line);
	}
};

#define __GENERATE_BUGGIFY_VARIABLES(TYPE, Type, type)                                                                 \
	inline double P_##TYPE##_BUGGIFIED_SECTION_ACTIVATED{ 0.25 };                                                      \
	inline double P_##TYPE##_BUGGIFIED_SECTION_FIRES{ 0.25 };                                                          \
	inline double P_##TYPE##_ENABLED{ false };                                                                         \
	inline std::unordered_map<BuggifySection, bool, BuggifySectionHash> Type##_SBVars;                                 \
	inline bool is##Type##BuggifyEnabled() noexcept {                                                                  \
		return P_##TYPE##_ENABLED;                                                                                     \
	}                                                                                                                  \
	inline void enable##Type##Buggify() noexcept {                                                                     \
		P_##TYPE##_ENABLED = true;                                                                                     \
	}                                                                                                                  \
	inline void disable##Type##Buggify() noexcept {                                                                    \
		P_##TYPE##_ENABLED = false;                                                                                    \
	}                                                                                                                  \
	inline void clear##Type##BuggifySections() {                                                                       \
		Type##_SBVars.clear();                                                                                         \
	}                                                                                                                  \
	inline bool get##Type##SBVar(const char* file, const int line) {                                                   \
		const BuggifySection section{ file, line };                                                                    \
		const auto sectionItr = Type##_SBVars.find(section);                                                           \
		if (sectionItr != Type##_SBVars.end()) [[likely]] {                                                            \
			return sectionItr->second;                                                                                 \
		}                                                                                                              \
                                                                                                                       \
		const double rand = deterministicRandom()->random01();                                                         \
		const bool activated = rand < P_##TYPE##_BUGGIFIED_SECTION_ACTIVATED;                                          \
		Type##_SBVars.emplace(section, activated);                                                                     \
		g_traceBatch.addBuggify(activated, line, file);                                                                \
		if (g_network) [[likely]] {                                                                                    \
			g_traceBatch.dump();                                                                                       \
		}                                                                                                              \
                                                                                                                       \
		return activated;                                                                                              \
	}

__GENERATE_BUGGIFY_VARIABLES(GENERAL, General, general)

__GENERATE_BUGGIFY_VARIABLES(CLIENT, Client, client)

#undef __GENERATE_BUGGIFY_VARIABLES

/* Disabled due to <source_location> not available on clang-14 on macOS 13. Use macro for a bit longer.
inline bool buggify(double probability = P_GENERAL_BUGGIFIED_SECTION_FIRES,
                    const std::source_location location = std::source_location::current()) {
	return isGeneralBuggifyEnabled() && getGeneralSBVar(location.file_name(), static_cast<int>(location.line())) &&
	       deterministicRandom()->random01() < probability;
}
*/

inline bool _buggify(const char *file, const int line, double probability = P_GENERAL_BUGGIFIED_SECTION_FIRES) {
	return isGeneralBuggifyEnabled() && getGeneralSBVar(file, line) &&
	       deterministicRandom()->random01() < probability;
}
// buggify() macro defined at the end to avoid affecting swift namespace

#define EXPENSIVE_VALIDATION (isGeneralBuggifyEnabled() && deterministicRandom()->random01() < P_EXPENSIVE_VALIDATION)

#define CLIENT_BUGGIFY_WITH_PROB(x)                                                                                    \
	(isClientBuggifyEnabled() && getClientSBVar(__FILE__, __LINE__) && deterministicRandom()->random01() < (x))

#define CLIENT_BUGGIFY CLIENT_BUGGIFY_WITH_PROB(P_CLIENT_BUGGIFIED_SECTION_FIRES)

namespace SwiftBridging {

inline std::map<std::pair<std::string, int>, bool> SwiftGeneralSBVar;

inline bool getGeneralSBVar(const char* file, const int line) {
	const auto paired = std::make_pair(std::string(file), line);
	if (SwiftGeneralSBVar.count(paired)) [[likely]] {
		return SwiftGeneralSBVar[paired];
	}

	const double rand = deterministicRandom()->random01();
	const bool activated = rand < P_GENERAL_BUGGIFIED_SECTION_ACTIVATED;
	SwiftGeneralSBVar[paired] = activated;
	g_traceBatch.addBuggify(activated, line, file);
	if (g_network) [[likely]] {
		g_traceBatch.dump();
	}

	return activated;
}

inline bool buggify(const char* _Nonnull filename, int line) {
	// SEE: the global buggify function above.
	return isGeneralBuggifyEnabled() && getGeneralSBVar(filename, line) &&
	       deterministicRandom()->random01() < P_GENERAL_BUGGIFIED_SECTION_FIRES;
}

} // namespace SwiftBridging

// TODO: go back to buggify() function using <source_location>, above
#define buggify(...) _buggify(__FILE__, __LINE__ __VA_OPT__(,) __VA_ARGS__)

#endif // FLOW_BUGGIFY_H

/*
 * BackupRangeSpec.h
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

#pragma once

#ifndef FDBSERVER_BACKUP_RANGE_SPEC_H
#define FDBSERVER_BACKUP_RANGE_SPEC_H

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

#include "fdbclient/BackupAgent.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

// Test-option encoding of backup and restore key ranges. Workload options are plain strings, so keys use the same
// escaping as printable()/unprintable() (and fdbcli): printable ASCII is literal, a backslash is "\\", and any other
// byte is "\xNN".
//
//   ranges: "<begin>:<end>;<begin>:<end>;..."   e.g. "\x02perf\x00:\x02perf\x01;d/:e"
//   prefix: "<prefix>"                           shorthand for the single range [prefix, strinc(prefix))
//
// ';' and ':' separate fields, so a key containing either must write it as "\x3b" or "\x3a". An empty begin is the
// start of the keyspace. Ranges must be non-empty, not overlap each other and lie within normalKeys. The only system
// keys a backup may cover are the standard system backup ranges (see getSystemBackupRanges): mutation logs capture
// system-key mutations only inside those ranges, so any other system range would get a snapshot with no matching logs.
//
// Other range sources (random, named scenarios) can produce the same Standalone<VectorRef<KeyRangeRef>>.
namespace BackupRangeSpec {

namespace detail {

[[noreturn]] inline void invalid(StringRef option, std::string const& spec, const char* reason) {
	TraceEvent(SevWarnAlways, "InvalidBackupRangeSpec")
	    .detail("Option", option)
	    .detail("Spec", spec.substr(0, 200))
	    .detail("Reason", reason);
	throw test_specification_invalid();
}

inline bool isSystemBackupRange(KeyRangeRef const& range) {
	return std::any_of(getSystemBackupRanges().begin(), getSystemBackupRanges().end(), [&](KeyRangeRef const& r) {
		return r == range;
	});
}

// unprintable() asserts on malformed escapes, so check them first.
inline Key decodeKey(StringRef option, std::string const& spec, std::string const& escaped) {
	for (size_t i = 0; i < escaped.size(); ++i) {
		if (escaped[i] != '\\') {
			continue;
		}
		if (i + 1 < escaped.size() && escaped[i + 1] == '\\') {
			++i;
		} else if (i + 3 < escaped.size() && escaped[i + 1] == 'x' && isxdigit(escaped[i + 2]) &&
		           isxdigit(escaped[i + 3])) {
			i += 3;
		} else {
			invalid(option, spec, "bad escape sequence");
		}
	}
	return Key(unprintable(escaped));
}

} // namespace detail

// `option` names the option being parsed, for error reporting.
inline Standalone<VectorRef<KeyRangeRef>> parseRanges(StringRef option, std::string const& spec) {
	Standalone<VectorRef<KeyRangeRef>> ranges;
	size_t pos = 0;
	while (true) {
		size_t semi = spec.find(';', pos);
		std::string item = spec.substr(pos, semi == std::string::npos ? std::string::npos : semi - pos);
		size_t colon = item.find(':');
		if (colon == std::string::npos || item.find(':', colon + 1) != std::string::npos) {
			detail::invalid(option, spec, "each range must be <begin>:<end>");
		}
		Key begin = detail::decodeKey(option, spec, item.substr(0, colon));
		Key end = detail::decodeKey(option, spec, item.substr(colon + 1));
		if (begin >= end) {
			detail::invalid(option, spec, "range is empty or inverted");
		}
		if (end > allKeys.end) {
			detail::invalid(option, spec, "range extends past allKeys");
		}
		if (end > normalKeys.end && !detail::isSystemBackupRange(KeyRangeRef(begin, end))) {
			detail::invalid(option, spec, "range extends into system keys other than the system backup ranges");
		}
		ranges.push_back_deep(ranges.arena(), KeyRangeRef(begin, end));
		if (semi == std::string::npos) {
			break;
		}
		pos = semi + 1;
	}

	std::vector<KeyRangeRef> sorted(ranges.begin(), ranges.end());
	std::sort(
	    sorted.begin(), sorted.end(), [](KeyRangeRef const& a, KeyRangeRef const& b) { return a.begin < b.begin; });
	for (size_t i = 1; i < sorted.size(); ++i) {
		if (sorted[i].begin < sorted[i - 1].end) {
			detail::invalid(option, spec, "ranges overlap");
		}
	}
	return ranges;
}

// Returns [prefix, strinc(prefix)) for an escaped, non-empty prefix.
inline KeyRange parsePrefixRange(StringRef option, std::string const& spec) {
	Key prefix = detail::decodeKey(option, spec, spec);
	if (prefix.empty() || prefix >= normalKeys.end) {
		detail::invalid(option, spec, "prefix must be non-empty and below \\xff");
	}
	return prefixRange(prefix);
}

// Returns the ranges given by `rangesOption` or `prefixOption`, or nothing when neither is set. Setting both is an
// error. getOption consumes an option, so call this once per option pair per workload instance.
inline Optional<Standalone<VectorRef<KeyRangeRef>>> readOptionalRanges(VectorRef<KeyValueRef> options,
                                                                       StringRef rangesOption,
                                                                       StringRef prefixOption) {
	std::string ranges = getOption(options, Key(rangesOption), Value()).toString();
	std::string prefix = getOption(options, Key(prefixOption), Value()).toString();
	if (!ranges.empty() && !prefix.empty()) {
		detail::invalid(rangesOption, ranges, "set only one of the ranges and prefix options");
	}
	if (!ranges.empty()) {
		return parseRanges(rangesOption, ranges);
	}
	if (!prefix.empty()) {
		Standalone<VectorRef<KeyRangeRef>> result;
		result.push_back_deep(result.arena(), parsePrefixRange(prefixOption, prefix));
		return result;
	}
	return Optional<Standalone<VectorRef<KeyRangeRef>>>();
}

// An unset option yields the empty key.
inline Key readKeyOption(VectorRef<KeyValueRef> options, StringRef name) {
	std::string spec = getOption(options, Key(name), Value()).toString();
	return detail::decodeKey(name, spec, spec);
}

} // namespace BackupRangeSpec

#endif

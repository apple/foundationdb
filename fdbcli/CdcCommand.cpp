/*
 * CdcCommand.cpp
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

#include <algorithm>
#include <charconv>

#include "fdbcli/fdbcli.h"
#include "fdbclient/NativeCdc.h"
#include "fdbclient/json_spirit/json_spirit_writer_template.h"

namespace {

json_spirit::mValue versionJson(Version version) {
	return version == invalidVersion ? json_spirit::mValue() : json_spirit::mValue(version);
}

std::string versionText(Version version) {
	return version == invalidVersion ? "unknown" : std::to_string(version);
}

Version acknowledgementLag(const NativeCdcStatus& status, const NativeCdcStreamStatus& stream) {
	if (status.readVersion == invalidVersion || stream.info.minVersion == invalidVersion) {
		return invalidVersion;
	}
	return std::max<Version>(0, status.readVersion - stream.info.minVersion);
}

int pendingRetiredTags(const NativeCdcStatus& status) {
	return static_cast<int>(
	    std::count_if(status.tags.begin(), status.tags.end(), [](const auto& tag) { return tag.pendingRetiredPop; }));
}

json_spirit::mObject statusJson(const NativeCdcStatus& status) {
	json_spirit::mObject result;
	result["read_version"] = versionJson(status.readVersion);
	result["admission_enabled"] = status.admissionEnabled;
	result["tag_count"] = status.tagCount;
	result["metadata_complete"] = status.metadataComplete;
	result["metadata_drained"] = status.metadataComplete
	                                 ? json_spirit::mValue(status.streams.empty() && pendingRetiredTags(status) == 0)
	                                 : json_spirit::mValue();

	json_spirit::mArray streams;
	for (const auto& stream : status.streams) {
		json_spirit::mObject entry;
		entry["stream_id"] = std::to_string(stream.info.streamId);
		entry["name"] = stream.info.name.printable();
		entry["range_begin"] = stream.info.keys.begin.printable();
		entry["range_end"] = stream.info.keys.end.printable();
		entry["min_version"] = versionJson(stream.info.minVersion);
		entry["acknowledgement_lag_versions"] = versionJson(acknowledgementLag(status, stream));
		entry["owner_proxy_id"] =
		    stream.owner.present() ? json_spirit::mValue(stream.owner.get().toString()) : json_spirit::mValue();
		entry["owner_published"] = stream.ownerPublished;
		json_spirit::mArray tags;
		for (const auto& tag : stream.tags) {
			tags.push_back(tag.toString());
		}
		entry["tags"] = tags;
		streams.push_back(entry);
	}
	result["streams"] = streams;

	json_spirit::mArray tags;
	for (const auto& tag : status.tags) {
		json_spirit::mObject entry;
		entry["tag"] = tag.tag.toString();
		entry["safe_pop_version"] = versionJson(tag.safePopVersion);
		entry["pending_retired_pop"] = tag.pendingRetiredPop;
		entry["retired_pop_version"] = versionJson(tag.retiredPopVersion);
		json_spirit::mArray blockers;
		for (CDCStreamId streamId : tag.blockingStreams) {
			blockers.push_back(std::to_string(streamId));
		}
		entry["blocking_stream_ids"] = blockers;
		tags.push_back(entry);
	}
	result["tags"] = tags;

	json_spirit::mArray proxies;
	for (const auto& proxy : status.proxies) {
		json_spirit::mObject entry;
		entry["id"] = proxy.id.toString();
		entry["address"] = proxy.address.toString();
		entry["sample"] = json_spirit::mValue();
		entry["error"] = json_spirit::mValue();
		if (proxy.error.present()) {
			json_spirit::mObject error;
			error["code"] = proxy.error.get().code();
			error["message"] = proxy.error.get().what();
			entry["error"] = error;
		}
		if (proxy.sample.present()) {
			const auto& sample = proxy.sample.get();
			json_spirit::mObject metrics;
			metrics["latest_committed_version"] = versionJson(sample.latestCommittedVersion);
			metrics["buffered_memory_bytes"] = sample.bufferedBytes;
			metrics["active_permits"] = sample.activePermits;
			metrics["buffer_limit"] = sample.bufferLimit;
			metrics["waiters"] = sample.waiters;
			metrics["pop_attempts"] = sample.popAttempts;
			metrics["pop_completions"] = sample.popCompletions;
			metrics["recovery_state"] = sample.recoveryState;
			metrics["recovery_count"] = sample.recoveryCount;
			metrics["recovered_at"] = versionJson(sample.recoveredAt);
			metrics["old_log_generations"] = sample.oldLogGenerations;
			json_spirit::mArray streamSamples;
			for (const auto& stream : sample.streams) {
				json_spirit::mObject streamSample;
				streamSample["stream_id"] = std::to_string(stream.streamId);
				streamSample["present"] = stream.present;
				if (stream.present) {
					streamSample["initialized"] = stream.initialized;
					streamSample["min_version"] = versionJson(stream.minVersion);
					streamSample["buffered_through"] = versionJson(stream.bufferedThrough);
					streamSample["buffered_memory_bytes"] = stream.bufferedBytes;
					streamSample["read_demand"] = stream.readDemand;
					streamSample["active_consume_requests"] = stream.activeConsumeRequests;
					streamSample["too_old"] = stream.tooOld;
					streamSample["buffer_limit_exceeded"] = stream.bufferLimitExceeded;
				}
				streamSamples.push_back(streamSample);
			}
			metrics["streams"] = streamSamples;
			entry["sample"] = metrics;
		}
		proxies.push_back(entry);
	}
	result["proxies"] = proxies;
	return result;
}

void printCdcStatus(const NativeCdcStatus& status) {
	fmt::println("Native CDC admission: {}", status.admissionEnabled ? "enabled" : "disabled");
	fmt::println("Durable retention metadata at version {} ({}):",
	             versionText(status.readVersion),
	             status.metadataComplete ? "complete" : "INCOMPLETE");
	fmt::println(
	    "  {} active streams; {} tags pending retired cleanup", status.streams.size(), pendingRetiredTags(status));
	if (!status.metadataComplete) {
		fmt::println("  WARNING: Incomplete metadata; drain completion and retention blockers cannot be certified.");
	} else if (status.streams.empty() && pendingRetiredTags(status) == 0) {
		fmt::println("  Retention metadata is drained. Physical TLog disk reclamation is not certified.");
	}
	for (const auto& stream : status.streams) {
		fmt::println("  Stream {}: name=\"{}\", range={}",
		             stream.info.streamId,
		             stream.info.name.printable(),
		             stream.info.keys.toString());
		fmt::println("    minimum version={}, acknowledgement lag={} versions, owner={} ({})",
		             versionText(stream.info.minVersion),
		             versionText(acknowledgementLag(status, stream)),
		             stream.owner.present() ? stream.owner.get().toString() : "unknown",
		             stream.ownerPublished ? "published" : "not published");
		fmt::print("    tags:");
		for (const auto& tag : stream.tags) {
			fmt::print(" {}", tag.toString());
		}
		fmt::println("");
	}
	for (const auto& tag : status.tags) {
		fmt::print("  Tag {}: allowed safe-pop version={}, blocking stream IDs:",
		           tag.tag.toString(),
		           versionText(tag.safePopVersion));
		for (CDCStreamId streamId : tag.blockingStreams) {
			fmt::print(" {}", streamId);
		}
		fmt::println("; retired cleanup={}{}",
		             tag.pendingRetiredPop ? "pending through version " : "none",
		             tag.pendingRetiredPop ? versionText(tag.retiredPopVersion) : "");
	}
	fmt::println("Advisory proxy samples (not atomic with the durable metadata snapshot):");
	for (const auto& proxy : status.proxies) {
		fmt::println("  Proxy {} at {}:", proxy.id.toString(), proxy.address.toString());
		if (!proxy.sample.present()) {
			fmt::println("    unavailable: {}", proxy.error.present() ? proxy.error.get().what() : "no sample");
			continue;
		}
		const auto& sample = proxy.sample.get();
		fmt::println("    memory: {} buffered bytes; {} active permits / {} limit; {} waiters",
		             sample.bufferedBytes,
		             sample.activePermits,
		             sample.bufferLimit,
		             sample.waiters);
		fmt::println("    pop requests: {} attempts, {} completions; latest committed version={}",
		             sample.popAttempts,
		             sample.popCompletions,
		             versionText(sample.latestCommittedVersion));
		fmt::println("    recovery state={}, recovery count={}, recovered at version={}, old log generations={}",
		             sample.recoveryState,
		             sample.recoveryCount,
		             versionText(sample.recoveredAt),
		             sample.oldLogGenerations);
		for (const auto& stream : sample.streams) {
			if (!stream.present) {
				fmt::println("    Stream {}: not present in proxy sample", stream.streamId);
				continue;
			}
			fmt::println("    Stream {}: initialized={}, minimum version={}, buffered through={}, buffered bytes={}",
			             stream.streamId,
			             stream.initialized,
			             versionText(stream.minVersion),
			             versionText(stream.bufferedThrough),
			             stream.bufferedBytes);
			fmt::println("      read demand={}, active consumes={}, too old={}, buffer limit exceeded={}",
			             stream.readDemand,
			             stream.activeConsumeRequests,
			             stream.tooOld,
			             stream.bufferLimitExceeded);
		}
	}
	fmt::println("Version distances are not elapsed time. Proxy memory is not retained TLog disk.");
	fmt::println("Allowed pops and completed pop requests do not certify reclaimed disk; shared tags retain history");
	fmt::println("for every blocking stream. Old log generations alone do not prove CDC is blocking recovery.");
}

Optional<CDCStreamId> parseStreamId(StringRef token) {
	const std::string text = token.toString();
	CDCStreamId streamId = 0;
	const auto result = std::from_chars(text.data(), text.data() + text.size(), streamId);
	if (result.ec != std::errc() || result.ptr != text.data() + text.size() || streamId == 0) {
		return {};
	}
	return streamId;
}

} // namespace

namespace fdb_cli {

Future<bool> cdcCommandActor(Database cx, std::vector<StringRef> tokens) {
	if (tokens.size() >= 2 && tokencmp(tokens[1], "status")) {
		if (tokens.size() != 2 && !(tokens.size() == 3 && tokencmp(tokens[2], "json"))) {
			printUsage(tokens[0]);
			co_return false;
		}
		NativeCdcStatus status = co_await getNativeCdcStatus(cx);
		if (tokens.size() == 3) {
			fmt::println("{}",
			             json_spirit::write_string(json_spirit::mValue(statusJson(status)), json_spirit::pretty_print));
		} else {
			printCdcStatus(status);
		}
		co_return true;
	}
	if (tokens.size() < 2 || !tokencmp(tokens[1], "remove") || tokens.size() != 5) {
		printUsage(tokens[0]);
		co_return false;
	}
	if (tokens[2].empty()) {
		fmt::println(stderr, "ERROR: CDC stream name must be non-empty.");
		co_return false;
	}
	Optional<CDCStreamId> expectedStreamId = parseStreamId(tokens[3]);
	if (!expectedStreamId.present()) {
		fmt::println(stderr, "ERROR: EXPECTED_STREAM_ID must be a nonzero unsigned 64-bit decimal integer.");
		co_return false;
	}
	if (!tokencmp(tokens[4], "CONFIRM-DATA-LOSS")) {
		fmt::println(stderr,
		             "ERROR: Removal relinquishes unread CDC history; the final argument must be CONFIRM-DATA-LOSS.");
		co_return false;
	}

	Key name = tokens[2];
	const UID auditId = deterministicRandom()->randomUniqueID();
	TraceEvent("NativeCdcRemoveCommand", auditId).detail("Phase", "Attempt").detail("StreamId", expectedStreamId.get());
	NativeCdcRemoveResult result;
	try {
		result = co_await removeNativeCdcStreamGuarded(cx, name, expectedStreamId.get());
	} catch (Error& e) {
		TraceEvent("NativeCdcRemoveCommand", auditId)
		    .errorUnsuppressed(e)
		    .detail("Phase", "Error")
		    .detail("StreamId", expectedStreamId.get());
		throw;
	}
	const char* outcome = result == NativeCdcRemoveResult::Removed         ? "Removed"
	                      : result == NativeCdcRemoveResult::AlreadyAbsent ? "AlreadyAbsent"
	                                                                       : "StreamReplaced";
	TraceEvent("NativeCdcRemoveCommand", auditId).detail("Phase", outcome).detail("StreamId", expectedStreamId.get());
	if (result == NativeCdcRemoveResult::StreamReplaced) {
		fmt::println(
		    stderr,
		    "ERROR: CDC stream name \"{}\" has a different stream ID than {}; the replacement was not removed.",
		    name.printable(),
		    expectedStreamId.get());
		fmt::println(stderr, "Review 'cdc status' and confirm the current stream identity before retrying.");
		co_return false;
	}
	fmt::println("CDC stream \"{}\" (ID {}) {}.",
	             name.printable(),
	             expectedStreamId.get(),
	             result == NativeCdcRemoveResult::Removed ? "removed; unread CDC history relinquished"
	                                                      : "already absent");
	fmt::println("Retired cleanup may still be pending. Use 'cdc status' to monitor it; removal does not certify");
	fmt::println("physical TLog disk reclamation.");
	co_return true;
}

CommandFactory cdcFactory(
    "cdc",
    CommandHelp("cdc status [json] | cdc remove <NAME> <EXPECTED_STREAM_ID> CONFIRM-DATA-LOSS",
                "inspect native CDC retention and remove an exact stream identity",
                "'cdc status [json]' reports durable stream/tag retention metadata and advisory proxy samples. "
                "Missing samples remain unknown. Admission disabled is not a completed drain. Version distances "
                "are not elapsed time, and proxy buffered bytes are not retained TLog disk.\n\n"
                "'cdc remove <NAME> <EXPECTED_STREAM_ID> CONFIRM-DATA-LOSS' relinquishes unread history for that "
                "exact stream identity. Obtain its nonzero decimal ID from 'cdc status'; a same-name replacement "
                "is never removed. Removal schedules cleanup but does not certify reclaimed TLog disk. Repair the "
                "consumer instead when its unread history is still required.\n\n"
                "These cluster-wide management commands require trusted access and use the native client. "
                "Use a compatible fdbcli version. Names and range keys in status use printable escaping; see "
                "'help escaping' for name arguments. JSON stream IDs are decimal strings.\n"));

} // namespace fdb_cli

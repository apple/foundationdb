/*
 * DatabaseMaintenance.cpp
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
#include <cinttypes>
#include <cstdio>
#include <map>
#include <string>
#include <vector>

#include "flow/DeterministicRandom.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "fdbrpc/sim_validation.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/QuietDatabase.h"
#include "DatabaseMaintenance.h"

Future<Void> clearData(Database cx) {
	TraceEvent("ClearData_Start").detail("Phase", "Enter");

	Transaction tr(cx);
	TraceEvent("ClearData_TrCreated").detail("Phase", "Loop1_Start");

	while (true) {
		TraceEvent("ClearData_Loop1_Iteration").detail("Phase", "TryBlock");

		Error err;
		try {
			tr.debugTransaction(debugRandom()->randomUniqueID());
			TraceEvent("ClearData_DebugSet").detail("Phase", "Loop1_Debug");

			ASSERT(tr.trState->readOptions.present() && tr.trState->readOptions.get().debugID.present());
			TraceEvent("TesterClearingDatabaseStart", tr.trState->readOptions.get().debugID.get()).log();

			// This transaction needs to be self-conflicting, but not conflict consistently with
			// any other transactions
			tr.clear(normalKeys);
			tr.makeSelfConflicting();
			TraceEvent("ClearData_PreGetReadVersion").detail("Phase", "Loop1_AboutToAwait");

			Version rv = co_await tr.getReadVersion(); // required since we use addReadConflictRange but not get
			TraceEvent("TesterClearingDatabaseRV", tr.trState->readOptions.get().debugID.get()).detail("RV", rv);
			TraceEvent("ClearData_PreCommit").detail("Phase", "Loop1_AboutToCommit");

			co_await tr.commit();
			TraceEvent("TesterClearingDatabase", tr.trState->readOptions.get().debugID.get())
			    .detail("AtVersion", tr.getCommittedVersion());
			TraceEvent("ClearData_Loop1_Success").detail("Phase", "Loop1_Complete");
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "TesterClearingDatabaseError", tr.trState->readOptions.get().debugID.get()).error(e);
			TraceEvent("ClearData_Loop1_Catch").detail("Phase", "Loop1_Error").detail("ErrorCode", e.code());
			err = e;
		}

		TraceEvent("ClearData_Loop1_ErrorHandling").detail("Phase", "Loop1_OnError");
		co_await tr.onError(err);
	}

	TraceEvent("ClearData_Loop1_Done").detail("Phase", "Loop2_Start");
	tr = Transaction(cx);
	TraceEvent("ClearData_NewTr").detail("Phase", "Loop2_TrCreated");

	while (true) {
		bool needsErrorHandling = false;
		Error caughtError;

		TraceEvent("ClearData_Loop2_Iteration").detail("Phase", "Loop2_TryBlock");

		try {
			tr.debugTransaction(debugRandom()->randomUniqueID());
			tr.setOption(FDBTransactionOptions::RAW_ACCESS);
			TraceEvent("ClearData_Loop2_PreGetRange").detail("Phase", "Loop2_AboutToAwait");

			RangeResult rangeResult = co_await tr.getRange(normalKeys, 1);
			TraceEvent("ClearData_Loop2_PostGetRange")
			    .detail("Phase", "Loop2_RangeGot")
			    .detail("ResultSize", rangeResult.size());

			// If the result is non-empty, it is possible that there is some bug.
			if (!rangeResult.empty()) {
				TraceEvent(SevError, "TesterClearFailure").detail("FirstKey", rangeResult[0].key);
				ASSERT(false);
			}
			// TODO(gglass): the assert below is leftover from some tenant logic.  If it fails, take it out.
			// If not, leave it in and take out this comment.
			ASSERT(tr.trState->readOptions.present() && tr.trState->readOptions.get().debugID.present());
			TraceEvent("TesterCheckDatabaseClearedDone", tr.trState->readOptions.get().debugID.get());
			TraceEvent("ClearData_Loop2_Success").detail("Phase", "Loop2_Complete");
			break;
		} catch (Error& e) {
			TraceEvent(SevWarn, "TesterCheckDatabaseClearedError", tr.trState->readOptions.get().debugID.get())
			    .error(e);
			TraceEvent("ClearData_Loop2_Catch").detail("Phase", "Loop2_Error").detail("ErrorCode", e.code());
			caughtError = e;
			needsErrorHandling = true;
		}

		if (needsErrorHandling) {
			TraceEvent("ClearData_Loop2_ErrorHandling").detail("Phase", "Loop2_OnError");
			co_await tr.onError(caughtError);
		}
	}

	TraceEvent("ClearData_End").detail("Phase", "Success");
	co_return;
}

namespace {

std::string toHTML(const StringRef& binaryString) {
	std::string s;

	for (int i = 0; i < binaryString.size(); i++) {
		uint8_t c = binaryString[i];
		if (c == '<') {
			s += "&lt;";
		} else if (c == '>') {
			s += "&gt;";
		} else if (c == '&') {
			s += "&amp;";
		} else if (c == '"') {
			s += "&quot;";
		} else if (c == ' ') {
			s += "&nbsp;";
		} else if (c > 32 && c < 127) {
			s += c;
		} else {
			s += format("<span class=\"binary\">[%02x]</span>", c);
		}
	}

	return s;
}

} // namespace

Future<Void> dumpDatabase(Database const& cx, std::string const& outputFilename, KeyRange const& range) {
	try {
		Transaction tr(cx);
		while (true) {
			FILE* output = fopen(outputFilename.c_str(), "wt");
			Error err;
			bool success = false;
			try {
				KeySelectorRef iter = firstGreaterOrEqual(range.begin);
				Arena arena;
				fprintf(output, "<html><head><style type=\"text/css\">.binary {color:red}</style></head><body>\n");
				Version ver = co_await tr.getReadVersion();
				fprintf(output, "<h3>Database version: %" PRId64 "</h3>", ver);

				while (true) {
					RangeResult results = co_await tr.getRange(iter, firstGreaterOrEqual(range.end), 1000);
					for (int r = 0; r < results.size(); r++) {
						std::string key = toHTML(results[r].key), value = toHTML(results[r].value);
						fprintf(output, "<p>%s <b>:=</b> %s</p>\n", key.c_str(), value.c_str());
					}
					if (results.size() < 1000) {
						break;
					}
					iter = firstGreaterThan(KeyRef(arena, results[results.size() - 1].key));
				}
				fprintf(output, "</body></html>");
				success = true;
			} catch (Error& e) {
				err = e;
			}
			fclose(output);
			if (success) {
				TraceEvent("DatabaseDumped").detail("Filename", outputFilename);
				co_return;
			}
			co_await tr.onError(err);
		}
	} catch (Error& e) {
		TraceEvent(SevError, "DumpDatabaseError").error(e).detail("Filename", outputFilename);
		throw;
	}
}

std::vector<PerfMetric> aggregateMetrics(std::vector<std::vector<PerfMetric>> metrics) {
	std::map<std::string, std::vector<PerfMetric>> metricMap;
	for (int i = 0; i < metrics.size(); i++) {
		std::vector<PerfMetric> workloadMetrics = metrics[i];
		TraceEvent("MetricsReturned").detail("Count", workloadMetrics.size());
		for (int m = 0; m < workloadMetrics.size(); m++) {
			printf("Metric (%d, %d): %s, %f, %s\n",
			       i,
			       m,
			       workloadMetrics[m].name().c_str(),
			       workloadMetrics[m].value(),
			       workloadMetrics[m].formatted().c_str());
			metricMap[workloadMetrics[m].name()].push_back(workloadMetrics[m]);
		}
	}
	TraceEvent("Metric")
	    .detail("Name", "Reporting Clients")
	    .detail("Value", (double)metrics.size())
	    .detail("Formatted", format("%d", metrics.size()).c_str());

	std::vector<PerfMetric> result;
	std::map<std::string, std::vector<PerfMetric>>::iterator it;
	for (it = metricMap.begin(); it != metricMap.end(); it++) {
		auto& vec = it->second;
		if (vec.empty())
			continue;
		double sum = 0;
		for (int i = 0; i < vec.size(); i++)
			sum += vec[i].value();
		if (vec[0].averaged() && !vec.empty())
			sum /= vec.size();
		result.emplace_back(vec[0].name(), sum, Averaged::False, vec[0].format_code());
	}
	return result;
}

void logMetrics(std::vector<PerfMetric> metrics) {
	for (int idx = 0; idx < metrics.size(); idx++)
		TraceEvent("Metric")
		    .detail("Name", metrics[idx].name())
		    .detail("Value", metrics[idx].value())
		    .detail("Formatted", format(metrics[idx].format_code().c_str(), metrics[idx].value()));
}

Future<Void> checkConsistencyScanAfterTest(Database cx, TesterConsistencyScanState* csState) {
	if (!csState->enabled) {
		co_return;
	}

	// mark it as done so later so this does not repeat
	csState->enabled = false;

	if (csState->enableAfter || csState->waitForComplete) {
		printf("Enabling consistency scan after test ...\n");
		TraceEvent("TestProgress").log("checkConsistencyScanAfterTest: calling enableConsistencyScanInSim()");
		co_await enableConsistencyScanInSim(cx);
		printf("Enabled consistency scan after test.\n");
		TraceEvent("TestProgress").log("checkConsistencyScanAfterTest: enableConsistencyScanInSim() returned.");
	}

	TraceEvent("TestProgress").log("checkConsistencyScanAfterTest: calling disableConsistencyScanInSim()");
	co_await disableConsistencyScanInSim(cx, csState->waitForComplete);
	TraceEvent("TestProgress").log("checkConsistencyScanAfterTest: disableConsistencyScanInSim() returned.");

	co_return;
}

/*
 * RestoreValidation.cpp
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

// RestoreValidationWorkload triggers and monitors a ValidateRestore audit to verify that
// backup/restore operations correctly restored all data.
//
// This workload is designed to work with BackupAndRestoreValidation workload:
// 1. BackupAndRestoreValidation performs backup and restore with a prefix (e.g., \xff\x02/rlog/)
// 2. BackupAndRestoreValidation sets a completion marker when fully done
// 3. RestoreValidationWorkload waits for the completion marker
// 4. RestoreValidationWorkload triggers a ValidateRestore audit via the audit_storage API
// 5. The audit compares source keys (normalKeys) with restored keys (prefix + normalKeys)
// 6. RestoreValidationWorkload monitors audit progress and reports success/failure
//
// The workload includes:
// - Synchronization: Waits for restore completion marker to avoid racing with restore
// - Retry logic: Retries audit scheduling on transient failures (up to 5 times)
// - Timeout handling: 60s timeout on audit scheduling to detect cluster recovery issues
// - Progress monitoring: Polls audit status every checkInterval seconds
// - Error detection: Fails the test if audit finds missing or mismatched keys

#include "fdbclient/Audit.h"
#include "fdbclient/AuditUtils.h"
#include "fdbclient/ClusterConnectionFile.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/tester/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RestoreValidationWorkload : TestWorkload {
	static constexpr auto NAME = "RestoreValidation";

	double validateAfter;
	KeyRange validationRange;
	int expectedPhase; // Expected AuditPhase (2 = Complete)
	bool expectSuccess;
	double checkInterval;
	double maxWaitTime;

	RestoreValidationWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		validateAfter = getOption(options, "validateAfter"_sr, 50.0);
		validationRange = normalKeys;
		expectedPhase = getOption(options, "expectedPhase"_sr, (int)AuditPhase::Complete);
		expectSuccess = getOption(options, "expectSuccess"_sr, true);
		checkInterval = getOption(options, "checkInterval"_sr, 5.0);
		maxWaitTime = getOption(options, "maxWaitTime"_sr, 300.0);

		TraceEvent("RestoreValidationWorkloadInit")
		    .detail("ValidateAfter", validateAfter)
		    .detail("ExpectedPhase", expectedPhase)
		    .detail("ExpectSuccess", expectSuccess)
		    .detail("MaxWaitTime", maxWaitTime);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId == 0) {
			return _start(cx);
		}
		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> _start(Database cx) {
		// Only run on client 0 to avoid conflicts (backup/restore runs on client 0)
		if (clientId != 0) {
			co_return;
		}

		// Wait for the specified time before starting validation
		TraceEvent("RestoreValidationWorkloadWaiting").detail("WaitTime", validateAfter);
		co_await delay(validateAfter);

		// Wait for restore completion marker
		// BackupAndRestoreValidation sets this key when restore is fully complete
		Key completionMarker = "\xff\x02/restoreValidationComplete"_sr;
		int checkAttempts = 0;

		TraceEvent("RestoreValidationWaitingForRestoreCompletion")
		    .detail("CompletionMarker", printable(completionMarker));

		while (true) {
			Error err;
			try {
				Transaction tr(cx);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);

				Optional<Value> markerValue = co_await tr.get(completionMarker);
				if (markerValue.present()) {
					TraceEvent("RestoreValidationRestoreComplete").detail("CheckAttempts", checkAttempts);
					break;
				}

				checkAttempts++;
				// No max check limit - keep waiting until test timeout or marker appears
				// This is necessary because buggify can make operations arbitrarily slow
				if (checkAttempts % 12 == 0) { // Log every minute
					TraceEvent("RestoreValidationStillWaitingForRestore")
					    .detail("CheckAttempts", checkAttempts)
					    .detail("WaitTimeSeconds", checkAttempts * 5);
				}
				co_await delay(5.0);
			} catch (Error& e) {
				err = e;
			}
			if (!err.isValid()) {
				continue;
			}
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}
			// Retry on transient errors from buggify chaos injection
			if (err.code() == error_code_grv_proxy_memory_limit_exceeded ||
			    err.code() == error_code_commit_proxy_memory_limit_exceeded ||
			    err.code() == error_code_database_locked || err.code() == error_code_transaction_too_old ||
			    err.code() == error_code_future_version || err.code() == error_code_audit_storage_failed ||
			    err.code() == error_code_tag_throttled) {
				TraceEvent(SevWarn, "RestoreValidationRetryableError")
				    .error(err)
				    .detail("CheckAttempts", checkAttempts);
				co_await delay(1.0); // Backoff before retry
				// Loop will retry
			} else {
				throw err;
			}
		}

		TraceEvent("RestoreValidationWorkloadStarting").detail("Range", validationRange);

		int auditRetryCount = 0;
		int maxAuditRetries = 5;

		while (true) {
			Error err;
			try {
				// Trigger the audit_storage validate_restore command
				AuditType auditType = AuditType::ValidateRestore;
				Reference<IClusterConnectionRecord> clusterFile = cx->getConnectionRecord();

				TraceEvent("RestoreValidationTriggeringAudit")
				    .detail("AuditType", (int)auditType)
				    .detail("Range", validationRange)
				    .detail("RetryCount", auditRetryCount);

				// Trigger the audit using ManagementAPI with timeout
				// Use shorter timeout for scheduling (60s) to detect cluster issues early
				UID auditId;
				try {
					UID scheduleResult = co_await timeoutError(
					    auditStorage(clusterFile, validationRange, auditType, KeyValueStoreType::END, maxWaitTime),
					    60.0);
					auditId = scheduleResult;
				} catch (Error& e) {
					if (e.code() == error_code_timed_out) {
						TraceEvent(SevWarn, "RestoreValidationAuditScheduleTimeout")
						    .detail("RetryCount", auditRetryCount)
						    .detail("MaxRetries", maxAuditRetries);
						// Treat as retryable - cluster might be recovering
						if (auditRetryCount < maxAuditRetries) {
							throw audit_storage_failed();
						} else {
							throw;
						}
					}
					throw;
				}

				TraceEvent("RestoreValidationAuditScheduled")
				    .detail("AuditID", auditId)
				    .detail("RetryCount", auditRetryCount);

				// Monitor audit progress
				double startTime = now();
				double lastReportTime = startTime;
				AuditPhase finalPhase = AuditPhase::Invalid;
				std::string errorMessage;

				while (true) {
					co_await delay(checkInterval);

					// Get audit status (newFirst=true to get latest states first)
					// Add timeout to handle cluster recovery/instability
					std::vector<AuditStorageState> auditStates;
					try {
						std::vector<AuditStorageState> states =
						    co_await timeoutError(getAuditStates(cx, auditType, true), 60.0);
						auditStates = states;
					} catch (Error& e) {
						if (e.code() == error_code_timed_out) {
							// Cluster is likely recovering - check overall timeout and continue
							if (now() - startTime > maxWaitTime) {
								TraceEvent(SevError, "RestoreValidationTimeout")
								    .detail("AuditID", auditId)
								    .detail("ElapsedTime", now() - startTime)
								    .detail("MaxWaitTime", maxWaitTime)
								    .detail("Reason", "getAuditStates timed out");
								throw timed_out();
							}
							continue; // Skip this iteration, try again
						}
						throw;
					}

					// Filter for our audit ID
					bool foundOurAudit = false;
					bool allComplete = true;
					bool anyError = false;

					for (const auto& auditState : auditStates) {
						if (auditState.id == auditId) {
							foundOurAudit = true;

							if (auditState.getPhase() == AuditPhase::Running) {
								allComplete = false;
							} else if (auditState.getPhase() == AuditPhase::Error ||
							           auditState.getPhase() == AuditPhase::Failed) {
								anyError = true;
								finalPhase = auditState.getPhase();
								if (!auditState.error.empty()) {
									errorMessage = auditState.error;
								} else {
									errorMessage = "Unknown error";
								}
							} else if (auditState.getPhase() == AuditPhase::Complete) {
								finalPhase = AuditPhase::Complete;
							}
						}
					}

					if (!foundOurAudit) {
						TraceEvent(SevWarn, "RestoreValidationNoAuditStates")
						    .detail("AuditID", auditId)
						    .detail("ElapsedTime", now() - startTime);
					} else {
						// Report progress periodically
						if (now() - lastReportTime >= 10.0) {
							TraceEvent("RestoreValidationProgress")
							    .detail("AuditID", auditId)
							    .detail("AllComplete", allComplete)
							    .detail("AnyError", anyError)
							    .detail("FinalPhase", (int)finalPhase)
							    .detail("ElapsedTime", now() - startTime);
							lastReportTime = now();
						}

						if (allComplete || anyError) {
							break;
						}
					}

					// Check timeout
					if (now() - startTime > maxWaitTime) {
						TraceEvent(SevError, "RestoreValidationTimeout")
						    .detail("AuditID", auditId)
						    .detail("ElapsedTime", now() - startTime)
						    .detail("MaxWaitTime", maxWaitTime);
						throw timed_out();
					}
				}

				// Verify the results
				TraceEvent("RestoreValidationComplete")
				    .detail("AuditID", auditId)
				    .detail("FinalPhase", (int)finalPhase)
				    .detail("ExpectedPhase", expectedPhase)
				    .detail("ErrorMessage", errorMessage)
				    .detail("ElapsedTime", now() - startTime);

				if (expectSuccess) {
					if (finalPhase != AuditPhase::Complete) {
						// Log as warning since we may retry - only becomes error if all retries fail
						TraceEvent(SevWarn, "RestoreValidationUnexpectedPhase")
						    .detail("AuditID", auditId)
						    .detail("FinalPhase", (int)finalPhase)
						    .detail("ExpectedPhase", expectedPhase)
						    .detail("ErrorMessage", errorMessage);
						throw audit_storage_failed();
					}
					if (!errorMessage.empty()) {
						TraceEvent(SevError, "RestoreValidationUnexpectedError")
						    .detail("AuditID", auditId)
						    .detail("ErrorMessage", errorMessage);
						throw audit_storage_error();
					}
				} else {
					if (finalPhase == AuditPhase::Complete) {
						TraceEvent(SevError, "RestoreValidationUnexpectedSuccess")
						    .detail("AuditID", auditId)
						    .detail("ExpectedPhase", expectedPhase);
						throw audit_storage_task_outdated();
					}
				}

				TraceEvent("RestoreValidationSuccess").detail("AuditID", auditId);
				break; // Success!

			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}
			// Retry audit on failures caused by cluster instability during buggify
			if (err.code() == error_code_audit_storage_failed && auditRetryCount < maxAuditRetries) {
				auditRetryCount++;
				double backoff = std::min(10.0, 2.0 * auditRetryCount);
				TraceEvent(SevWarn, "RestoreValidationAuditRetry")
				    .error(err)
				    .detail("RetryCount", auditRetryCount)
				    .detail("MaxRetries", maxAuditRetries)
				    .detail("BackoffSeconds", backoff);
				co_await delay(backoff);
				// Loop will retry the entire audit
			} else {
				TraceEvent(SevError, "RestoreValidationError")
				    .errorUnsuppressed(err)
				    .detail("RetryCount", auditRetryCount);
				throw err;
			}
		}
	}
};

WorkloadFactory<RestoreValidationWorkload> RestoreValidationWorkloadFactory;

/*
 * WaitFailure.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef WAIT_FAILURE_SERVER_H
#define WAIT_FAILURE_SERVER_H
#pragma once

#include "flow/flow.h"
#include "fdbrpc/fdbrpc.h"

Future<Void> waitFailureServer(const FutureStream<ReplyPromise<Void>>& waitFailure);

// talks to a wait failure server, returns Void on failure
Future<Void> waitFailureClient(const RequestStream<ReplyPromise<Void>>& waitFailure,
                               double const& failureReactionTime = 0,
                               double const& failureReactionSlope = 0,
                               bool const& trace = false,
                               TaskPriority const& taskID = TaskPriority::DefaultEndpoint);

// talks to a wait failure server, returns Void on failure, reaction time is always waited
Future<Void> waitFailureClientStrict(const RequestStream<ReplyPromise<Void>>& waitFailure,
                                     double const& failureReactionTime = 0,
                                     TaskPriority const& taskID = TaskPriority::DefaultEndpoint);

// talks to a wait failure server, updates failed to be true or false based on failure status.
Future<Void> waitFailureTracker(const RequestStream<ReplyPromise<Void>>& waitFailure,
                                Reference<AsyncVar<bool>> const& failed,
                                double const& failureReactionTime = 0,
                                double const& failureReactionSlope = 0,
                                TaskPriority const& taskID = TaskPriority::DefaultEndpoint);

#endif

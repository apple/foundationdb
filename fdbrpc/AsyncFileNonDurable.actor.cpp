/*
 * AsyncFileNonDurable.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/AsyncFileNonDurable.actor.h"
#include "flow/actorcompiler.h" // has to be last include

std::map<std::string, Future<Void>> AsyncFileNonDurable::filesBeingDeleted;

ACTOR Future<Void> sendOnProcess(ISimulator::ProcessInfo* process, Promise<Void> promise, TaskPriority taskID) {
	wait(g_simulator.onProcess(process, taskID));
	promise.send(Void());
	return Void();
}

ACTOR Future<Void> sendErrorOnProcess(ISimulator::ProcessInfo* process,
                                      Promise<Void> promise,
                                      Error e,
                                      TaskPriority taskID) {
	wait(g_simulator.onProcess(process, taskID));
	promise.sendError(e);
	return Void();
}

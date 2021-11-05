/*
 * FlowProcess.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
#include "fdbrpc/FlowProcess.actor.h"

#include "flow/actorcompiler.h" // has to be last include

ACTOR Future<int> spawnProcess(std::string binPath,
                               std::vector<std::string> paramList,
                               double maxWaitTime,
                               bool isSync,
                               double maxSimDelayTime);

namespace {

ACTOR Future<int> flowProcessRunner(FlowProcess* self, Promise<Void> ready) {
  state FlowProcessInterface processInterface;
  state Future<int> process;
  
  auto path = abspath(getExecPath());
  auto endpoint = processInterface.registerProcess.getEndpoint();
  auto address = endpoint.addresses.address.toString();
  auto token = endpoint.token;

  std::vector<std::string> args = {"-r", "flowprocess", "--process_name"};
  args.emplace_back(self->name().toString());
  args.emplace_back("--process_endpoint");
  args.emplace_back(format("%s,%lu,%lu", address.c_str(), token.first(), token.second()));

  process = spawnProcess(path, args, 5000.0, false, 0.01);
  choose {
    when (FlowProcessRegistrationRequest req = waitNext(processInterface.registerProcess.getFuture())) {
      self->consumeInterface(req.flowProcessInterface);
      ready.send(Void());
    }
    when(int res = wait(process)) {
      ready.sendError(operation_failed());
      return res;
    }
  }
  int res = wait(process);
  return res;
}

}

FlowProcess::~FlowProcess() {}

void FlowProcess::start() {
  returnCodePromise = flowProcessRunner(this, readyPromise);
}


Future<Void> runFlowProcess(std::string name, Endpoint endpoint) {
  FlowProcess* self = IProcessFactory::create(name.c_str());
  RequestStream<FlowProcessRegistrationRequest> registerProcess(endpoint);
  FlowProcessRegistrationRequest req;
  req.flowProcessInterface = self->serializedInterface();
  registerProcess.send(req);
  return self->run();
}
/*
 * TLogInterface.cpp
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

#include "fdbserver/ptxn/TLogInterface.h"

namespace ptxn {

MessageTransferModel TLogInterfaceBase::getMessageTransferModel() const {
	return messageTransferModel;
}

void TLogInterfaceBase::initEndpointsImpl(std::vector<ReceiverPriorityPair>&& receivers) {
	receivers.push_back(commit.getReceiver(TaskPriority::TLogCommit));
	receivers.push_back(lock.getReceiver());
	receivers.push_back(getQueuingMetrics.getReceiver(TaskPriority::TLogQueuingMetrics));
	receivers.push_back(confirmRunning.getReceiver(TaskPriority::TLogConfirmRunning));
	receivers.push_back(waitFailure.getReceiver());
	receivers.push_back(recoveryFinished.getReceiver());
	receivers.push_back(snapRequest.getReceiver());
	FlowTransport::transport().addEndpoints(receivers);
}

void TLogInterface_ActivelyPush::initEndpoints() {
	TLogInterfaceBase::initEndpointsImpl({});
}

void TLogInterface_PassivelyPull::initEndpoints() {
	TLogInterfaceBase::initEndpointsImpl({ peekMessages.getReceiver(TaskPriority::TLogPeek),
	                                       popMessages.getReceiver(TaskPriority::TLogPop),
	                                       disablePopRequest.getReceiver(),
	                                       enablePopRequest.getReceiver() });
}

std::shared_ptr<TLogInterfaceBase> getNewTLogInterface(const MessageTransferModel model,
                                                       UID id_,
                                                       UID sharedTLogID_,
                                                       LocalityData locality) {
	switch (model) {
	case MessageTransferModel::TLogActivelyPush:
		return std::make_shared<TLogInterface_ActivelyPush>();
	case MessageTransferModel::StorageServerActivelyPull:
		return std::make_shared<TLogInterface_PassivelyPull>(id_, sharedTLogID_, locality);
	default:
		throw internal_error_msg("Unsupported TLog Interface");
	}
}

} // namespace ptxn

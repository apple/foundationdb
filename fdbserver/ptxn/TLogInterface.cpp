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

void TLogInterfaceBase::initEndpointsImpl(std::vector<ReceiverPriorityPair>&& more) {
	std::vector<ReceiverPriorityPair> receivers;

	// The order here should be the same as declared in the TLogInterfaceBase
	receivers.push_back(commit.getReceiver(TaskPriority::TLogCommit));
	receivers.push_back(peek.getReceiver(TaskPriority::TLogCommit));
	receivers.push_back(pop.getReceiver(TaskPriority::TLogCommit));
	receivers.push_back(lock.getReceiver());
	receivers.push_back(getQueuingMetrics.getReceiver(TaskPriority::TLogQueuingMetrics));
	receivers.push_back(confirmRunning.getReceiver(TaskPriority::TLogConfirmRunning));
	receivers.push_back(waitFailure.getReceiver());
	receivers.push_back(recoveryFinished.getReceiver());
	receivers.push_back(snapRequest.getReceiver());

	receivers.insert(receivers.end(), more.begin(), more.end());
	FlowTransport::transport().addEndpoints(receivers);
}

void TLogInterface_ActivelyPush::initEndpoints() {
	TLogInterfaceBase::initEndpointsImpl({});
}

void TLogInterface_PassivelyPull::initEndpoints() {
	// RequestSteam order must be the same as declared in TLogInterface_PassivelyPull
	TLogInterfaceBase::initEndpointsImpl({ disablePopRequest.getReceiver(), enablePopRequest.getReceiver() });
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

namespace details {

namespace {
template <typename T>
std::shared_ptr<T> TLogInterfaceCastHelper(std::shared_ptr<TLogInterfaceBase> ptr) {
	std::shared_ptr<T> result = std::dynamic_pointer_cast<T>(ptr);
	if (!result) {
		// std::bad_cast might be a better choice if not using flow
		throw internal_error_msg("Unable to downcast TLogInterface");
	}
	return result;
}
} // namespace

TLogInterfaceSharedPtrWrapper::operator std::shared_ptr<TLogInterface_PassivelyPull>() {
	return TLogInterfaceCastHelper<TLogInterface_PassivelyPull>(ptr);
}

TLogInterfaceSharedPtrWrapper::operator std::shared_ptr<TLogInterface_ActivelyPush>() {
	return TLogInterfaceCastHelper<TLogInterface_ActivelyPush>(ptr);
}

} // namespace details

} // namespace ptxn

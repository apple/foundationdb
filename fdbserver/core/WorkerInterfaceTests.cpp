/*
 * WorkerInterfaceTests.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "fdbserver/core/WorkerInterface.h"

#include "flow/ObjectSerializer.h"
#include "flow/UnitTest.h"

TEST_CASE("/NativeCDC/InternalInterfaceFlatBufferRoundTrip") {
	const NetworkAddress workerAddress(IPAddress(0x01020304), 4500);
	const NetworkAddress cdcAddress(IPAddress(0x05060708), 4501);

	WorkerInterface worker;
	worker.tLog = RequestStream<InitializeTLogRequest>(Endpoint({ workerAddress }, UID(1, 2)));
	worker.cdcProxy = RequestStream<InitializeCDCProxyRequest>(Endpoint({ cdcAddress }, UID(3, 4)));

	const Standalone<StringRef> serializedWorker = ObjectWriter::toValue(worker, Unversioned());
	const auto decodedWorker = ObjectReader::fromStringRef<WorkerInterface>(serializedWorker, Unversioned());
	ASSERT_EQ(decodedWorker.tLog.getEndpoint().token, worker.tLog.getEndpoint().token);
	ASSERT_EQ(decodedWorker.cdcProxy.getEndpoint().token, worker.cdcProxy.getEndpoint().token);
	ASSERT_EQ(decodedWorker.cdcProxy.getEndpoint().getPrimaryAddress(), cdcAddress);

	CDCProxyInterface cdcProxy;
	cdcProxy.consume = PublicRequestStream<CDCConsumeRequest>(Endpoint({ cdcAddress }, UID(5, 6)));

	RegisterMasterRequest request;
	request.id = UID(7, 8);
	request.cdcProxies.push_back(cdcProxy);
	request.recoveryCount = 1;
	request.registrationCount = 2;
	request.recoveryState = RecoveryState::UNINITIALIZED;
	request.recoveryStalled = false;

	const Standalone<StringRef> serializedRequest = ObjectWriter::toValue(request, Unversioned());
	const auto decodedRequest = ObjectReader::fromStringRef<RegisterMasterRequest>(serializedRequest, Unversioned());
	ASSERT_EQ(decodedRequest.id, request.id);
	ASSERT_EQ(decodedRequest.cdcProxies.size(), 1);
	ASSERT_EQ(decodedRequest.cdcProxies.front().id(), cdcProxy.id());

	return Void();
}

TEST_CASE("/NativeCDC/TLogPeekRequestFlatBufferRoundTrip") {
	TLogPeekRequest request(100,
	                        Tag(tagLocalityCDC, 0),
	                        false,
	                        false,
	                        Optional<std::pair<UID, int>>(),
	                        Optional<Version>(200),
	                        Optional<bool>(true),
	                        4096);

	const Standalone<StringRef> serializedRequest = ObjectWriter::toValue(request, Unversioned());
	const auto decodedRequest = ObjectReader::fromStringRef<TLogPeekRequest>(serializedRequest, Unversioned());
	ASSERT_EQ(decodedRequest.begin, request.begin);
	ASSERT_EQ(decodedRequest.tag, request.tag);
	ASSERT_EQ(decodedRequest.end, request.end);
	ASSERT_EQ(decodedRequest.returnEmptyIfStopped, request.returnEmptyIfStopped);
	ASSERT_EQ(decodedRequest.replyByteLimit, request.replyByteLimit);

	return Void();
}

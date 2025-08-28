
/*
 * FDBAWSCredentialsProvider.cpp
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

#include "fdbclient/FDBAWSCredentialsProvider.h"
#include "fdbclient/FDBAWSCredentialsProviderChain.h"
#include "fdbclient/Tracing.h"

#ifdef WITH_AWS_BACKUP

// You're supposed to call AWS::ShutdownAPI(options); once done
// But we want this to live for the lifetime of the process, so we don't do that
namespace FDBAWSCredentialsProvider {
Aws::Auth::AWSCredentials getAwsCredentials() {
	static bool doneInit = false;
	if (!doneInit) {
		doneInit = true;
		Aws::SDKOptions options;
		Aws::InitAPI(options);
		TraceEvent("AWSSDKInitSuccessful");
	}
	FDBAWSCredentialsProviderChain credProvider;
	Aws::Auth::AWSCredentials creds = credProvider.GetAWSCredentials();
	return creds;
}
} // namespace FDBAWSCredentialsProvider

#endif

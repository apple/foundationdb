/*
 * FDBAWSCredentialsProviderChain.cpp
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

#include "fdbclient/FDBAWSCredentialsProviderChain.h"
#include "fdbclient/Tracing.h"

#ifdef WITH_AWS_BACKUP

#include "aws/core/auth/STSCredentialsProvider.h"
#include "aws/core/auth/SSOCredentialsProvider.h"
#include "aws/identity-management/auth/STSProfileCredentialsProvider.h"
#include "aws/core/platform/Environment.h"
#include "aws/core/utils/memory/AWSMemory.h"
#include "aws/core/utils/StringUtils.h"
#include "aws/core/utils/logging/LogMacros.h"

static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";
static const char DefaultCredentialsProviderChainTag[] = "DefaultAWSCredentialsProviderChain";

// this method is copied from
// https://github.com/aws/aws-sdk-cpp/blob/98a87de4e2e532cc88e527a27fbdf110d9375e6b/src/aws-cpp-sdk-core/source/auth/AWSCredentialsProviderChain.cpp#L36

FDBAWSCredentialsProviderChain::FDBAWSCredentialsProviderChain() : Aws::Auth::AWSCredentialsProviderChain() {
	AddProvider(Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>(DefaultCredentialsProviderChainTag));
	AddProvider(
	    Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(DefaultCredentialsProviderChainTag));
	AddProvider(Aws::MakeShared<Aws::Auth::ProcessCredentialsProvider>(DefaultCredentialsProviderChainTag));
	// added line below to support assume role and profile
	AddProvider(Aws::MakeShared<Aws::Auth::STSProfileCredentialsProvider>(DefaultCredentialsProviderChainTag));
	AddProvider(
	    Aws::MakeShared<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(DefaultCredentialsProviderChainTag));
	AddProvider(Aws::MakeShared<Aws::Auth::SSOCredentialsProvider>(DefaultCredentialsProviderChainTag));

	// ECS TaskRole Credentials only available when ENVIRONMENT VARIABLE is set
	const auto relativeUri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI);
	AWS_LOGSTREAM_DEBUG(DefaultCredentialsProviderChainTag,
	                    "The environment variable value " << AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI << " is "
	                                                      << relativeUri);

	const auto absoluteUri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI);
	AWS_LOGSTREAM_DEBUG(DefaultCredentialsProviderChainTag,
	                    "The environment variable value " << AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI << " is "
	                                                      << absoluteUri);

	const auto ec2MetadataDisabled = Aws::Environment::GetEnv(AWS_EC2_METADATA_DISABLED);
	AWS_LOGSTREAM_DEBUG(DefaultCredentialsProviderChainTag,
	                    "The environment variable value " << AWS_EC2_METADATA_DISABLED << " is "
	                                                      << ec2MetadataDisabled);

	if (!relativeUri.empty()) {
		AddProvider(Aws::MakeShared<Aws::Auth::TaskRoleCredentialsProvider>(DefaultCredentialsProviderChainTag,
		                                                                    relativeUri.c_str()));
		AWS_LOGSTREAM_INFO(DefaultCredentialsProviderChainTag,
		                   "Added ECS metadata service credentials provider with relative path: ["
		                       << relativeUri << "] to the provider chain.");
	} else if (!absoluteUri.empty()) {
		const auto token = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN);
		AddProvider(Aws::MakeShared<Aws::Auth::TaskRoleCredentialsProvider>(
		    DefaultCredentialsProviderChainTag, absoluteUri.c_str(), token.c_str()));

		// DO NOT log the value of the authorization token for security purposes.
		AWS_LOGSTREAM_INFO(DefaultCredentialsProviderChainTag,
		                   "Added ECS credentials provider with URI: ["
		                       << absoluteUri << "] to the provider chain with a"
		                       << (token.empty() ? "n empty " : " non-empty ") << "authorization token.");
	} else if (Aws::Utils::StringUtils::ToLower(ec2MetadataDisabled.c_str()) != "true") {
		AddProvider(Aws::MakeShared<Aws::Auth::InstanceProfileCredentialsProvider>(DefaultCredentialsProviderChainTag));
		AWS_LOGSTREAM_INFO(DefaultCredentialsProviderChainTag,
		                   "Added EC2 metadata service credentials provider to the provider chain.");
	}
}
#endif

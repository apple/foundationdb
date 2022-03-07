/*
 * BackupContainerAWSS3BlobStore.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#if (!defined FDBCLIENT_BACKUP_CONTAINER_AWSS3_BLOBSTORE_H) && (defined BUILD_AWS_BACKUP)
#define FDBCLIENT_BACKUP_CONTAINER_AWSS3_BLOBSTORE_H
#pragma once

#include "aws/core/Aws.h"
#include "aws/core/http/HttpTypes.h"
#include "aws/core/auth/AWSCredentialsProviderChain.h"
#include "aws/s3-crt/S3CrtClient.h"
#include "aws/s3-crt/model/GetObjectRequest.h"

// struct AwsApi {};

void do_aws_test_stuff() {
	printf("hell world\n");

	printf("Testing sdk code import\n");
	Aws::SDKOptions options;
	printf("Got AWS SDK options\n");

	printf("Testing init api\n");
	Aws::InitAPI(options);
	printf("API Init successful!\n");

	// printf("Testing client configuration\n");

	Aws::String region = "us-west-2";

	Aws::S3Crt::ClientConfiguration config;
	config.region = region;
	printf("Set region in client configuration\n");

	// creds
	printf("Constructing cred provider\n");
	Aws::Auth::DefaultAWSCredentialsProviderChain credProvider;
	printf("Asking cred provider for creds\n");
	Aws::Auth::AWSCredentials creds = credProvider.GetAWSCredentials();
	printf("Got creds provider for creds\n");
	// TODO DO NOT do this normally, just ensuring it works!!
	printf("  Empty: %s\n", creds.IsEmpty() ? "T" : "F");
	printf("  Expired: %s\n", creds.IsExpired() ? "T" : "F");
	printf("  Access key: %s\n", creds.GetAWSAccessKeyId().c_str());
	printf("  Secret key: %s\n", creds.GetAWSSecretKey().c_str());
	printf("  Session token: %s\n", creds.GetSessionToken().c_str());
	double expireTime = creds.GetExpiration().SecondsWithMSPrecision();
	printf("  Expiration: %.3f. Now=%.3f, ExpireTime=%.3f\n", expireTime, now(), expireTime - now());

	// s3

	// This segfaults on construction :(
	/*printf("Testing s3 construction\n");
	Aws::S3Crt::S3CrtClient s3Client(config);
	printf("s3 construction complete\n");

	Aws::String myBucket = "mybucket";
	Aws::String objectName = "myobject";
	printf("Testing s3 get object\n");
	Aws::S3Crt::Model::GetObjectRequest request;
	request.SetBucket(myBucket);
	request.SetKey(objectName);

	Aws::S3Crt::Model::GetObjectOutcome outcome = s3Client.GetObject(request);

	if (outcome.IsSuccess()) {
	    printf("Success:\n%s\n", outcome.GetResult().GetBody().rdbuf());
	} else {
	    printf("Error: %s\n", outcome.GetError());
	}
	printf("s3 get test complete\n");

	printf("doing presign test\n");

	Aws::String result =
	    s3Client.GeneratePresignedUrl(myBucket, objectName, Aws::Http::HttpMethod::HTTP_GET, 2 * 24 * 60 * 60);

	printf("presign got %s!\n", result);

	printf("Testing API shutdown\n");
	Aws::ShutdownAPI(options);
	printf("Got AWS SDK shutdown\n");*/

	// the presign function just calls a common presign function, so maybe we could do that without segfaulting
}

#endif

/*
 * BackupContainerAzureBlobStore.h
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
// #include <aws/core/Aws.h>

void do_aws_test_stuff() {
	printf("hell world\n");

	printf("Testing sdk code import\n");
	Aws::SDKOptions options;
	printf("Got AWS SDK options\n");

	printf("Testing init api\n");
	Aws::InitAPI(options);
	printf("API Init successful!\n");

	// printf("Testing client configuration\n");
	// Aws::Client::ClientConfiguration config;
	// config.region = "us-west-2";
	// printf("Set region in client configuration\n");

	printf("Testing API shutdown\n");
	Aws::ShutdownAPI(options);
	printf("Got AWS SDK shutdown\n");
}

#endif

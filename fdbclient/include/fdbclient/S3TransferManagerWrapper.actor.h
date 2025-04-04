#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_S3TRANSFERMANAGERWRAPPER_ACTOR_H)
#define FDBCLIENT_S3TRANSFERMANAGERWRAPPER_ACTOR_H
#include "fdbclient/S3TransferManagerWrapper.actor.g.h"
#elif !defined(FDBCLIENT_S3TRANSFERMANAGERWRAPPER_H)
#define FDBCLIENT_S3TRANSFERMANAGERWRAPPER_H

// Standard library includes
#include <string>

// Boost includes
#include <boost/version.hpp>

// FoundationDB includes
#include "flow/flow.h"
#include "fdbclient/FDBAWSCredentialsProvider.h"

// AWS includes
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/transfer/TransferManager.h>

// Forward declarations for actor functions
template <typename T>
class Future;

ACTOR Future<Void> uploadFileWithTransferManager(std::string localFile, std::string bucketName, std::string objectKey);
ACTOR Future<Void> downloadFileWithTransferManager(std::string bucketName,
                                                   std::string objectKey,
                                                   std::string localFile);

class S3TransferManagerWrapper {
public:
	static Future<Void> uploadFile(const std::string& localFile,
	                               const std::string& bucketName,
	                               const std::string& objectKey) {
		return uploadFileWithTransferManager(localFile, bucketName, objectKey);
	}

	static Future<Void> downloadFile(const std::string& bucketName,
	                                 const std::string& objectKey,
	                                 const std::string& localFile) {
		return downloadFileWithTransferManager(bucketName, objectKey, localFile);
	}
};

#include "flow/actorcompiler.h"
#endif
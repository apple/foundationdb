/**
 * FileTransfer.h
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
#ifdef FLOW_GRPC_ENABLED
#ifndef FDBRPC_FILE_TRANSFER_H
#define FDBRPC_FILE_TRANSFER_H
#include <optional>

#undef loop
#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include "fdbrpc/file_transfer/file_transfer.pb.h"
#include "fdbrpc/file_transfer/file_transfer.grpc.pb.h"

class FileTransferServiceImpl final : public fdbrpc::FileTransferService::Service {
	const int DEFAULT_CHUNK_SIZE = 1024 * 1024;

public:
	grpc::Status DownloadFile(grpc::ServerContext* context,
	                          const fdbrpc::DownloadRequest* request,
	                          grpc::ServerWriter<fdbrpc::DownloadChunk>* writer) override;

	grpc::Status GetFileInfo(grpc::ServerContext* context,
	                         const fdbrpc::GetFileInfoRequest* request,
	                         fdbrpc::GetFileInfoReply* response) override;

	//-- Testing --
	enum ErrorInjection {
		NO_ERROR,
		FAIL_RANDOMLY,
		FLIP_BYTE,
	};

	void SetErrorInjection(ErrorInjection error_inject) { error_inject_ = error_inject; }

private:
	ErrorInjection error_inject_ = NO_ERROR;
};

class FileTransferClient {
public:
	FileTransferClient(std::shared_ptr<grpc::Channel> channel) : stub_(fdbrpc::FileTransferService::NewStub(channel)) {}

	std::optional<fdbrpc::GetFileInfoReply> GetFileInfo(const std::string& filename, bool get_crc_checksum = false);

	// Downloads a file from a remote server and saves it locally.
	//
	// This function communicates with a remote server to download a file specified by `filename`
	// and saves it to the local file system with the name `output_filename`. Optionally, it can
	// verify the integrity of the downloaded file using a CRC checksum.
	//
	// Params:
	// - filename: The name of the file to download from the remote server.
	// - output_filename: The name of the file to save the downloaded content locally.
	// - verify: Whether to verify the file's integrity using CRC checksum
	//
	// Returns:
	//  std::optional<size_t> The size of the downloaded file in bytes if successful, or std::nullopt if the download
	//  failed. `output_filename` is deleted on failure.
	std::optional<size_t> DownloadFile(const std::string& filename,
	                                   const std::string& output_filename,
	                                   bool verify = true);

private:
	// If download fails, delete the file from disk.
	// TODO: Add ability to resume.
	const bool delete_on_close_ = true;

	std::unique_ptr<fdbrpc::FileTransferService::Stub> stub_;
};

#endif
#endif // FLOW_GRPC_ENABLED

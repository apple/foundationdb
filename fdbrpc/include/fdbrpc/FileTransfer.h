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

#ifndef FDBRPC_FILE_TRANSFER_H
#define FDBRPC_FILE_TRANSFER_H

#include <fstream>
#include <grpcpp/grpcpp.h>

#include <boost/asio.hpp>

#include "fdbrpc/file_transfer/file_transfer.grpc.pb.h"

using fdbrpc::DownloadChunk;
using fdbrpc::DownloadRequest;
using fdbrpc::FileTransferService;
using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

class FileTransferServiceImpl final : public FileTransferService::Service {
public:
	grpc::Status DownloadFile(grpc::ServerContext* context,
	                          const DownloadRequest* request,
	                          grpc::ServerWriter<DownloadChunk>* writer) override {
		std::ifstream input_file(request->file_name(), std::ios::binary | std::ios::ate);
		if (!input_file.is_open()) {
			return grpc::Status(grpc::StatusCode::NOT_FOUND, "File not found");
		}

		// const size_t file_size = input_file.tellg();
		input_file.seekg(0);

		const size_t buffer_size = 1024 * 1024; // 1MB buffer
		std::vector<char> buffer(buffer_size);

		int64_t offset = 0;
		while (input_file.good()) {
			input_file.read(buffer.data(), buffer_size);
			std::streamsize bytes_read = input_file.gcount();

			DownloadChunk chunk;
			chunk.set_offset(offset);
			chunk.set_data(buffer.data(), bytes_read);
			writer->Write(chunk);

			offset += bytes_read;
		}

		input_file.close();
		return grpc::Status::OK;
	}
};

class FileTransferClient {
public:
	FileTransferClient(std::shared_ptr<Channel> channel) : stub_(FileTransferService::NewStub(channel)) {}

	bool DownloadFile(const std::string& filename, const std::string& output_filename) {
		ClientContext context;
		DownloadRequest request;
		request.set_file_name(filename);

		std::unique_ptr<grpc::ClientReader<DownloadChunk>> reader(stub_->DownloadFile(&context, request));

		std::ofstream output_file(output_filename, std::ios::binary);
		if (!output_file.is_open()) {
			std::cerr << "Failed to open file for writing: " << output_filename << std::endl;
			return false;
		}

		DownloadChunk chunk;
		while (reader->Read(&chunk)) {
			output_file.seekp(chunk.offset());
			output_file.write(chunk.data().data(), chunk.data().size());
		}

		output_file.close();

		Status grpc_status = reader->Finish();
		if (grpc_status.ok()) {
			std::cout << "File downloaded successfully: " << output_filename << std::endl;
			return true;
		} else {
			std::cerr << "File download failed: " << grpc_status.error_message() << std::endl;
			return false;
		}
	}

private:
	std::unique_ptr<FileTransferService::Stub> stub_;
};

#endif
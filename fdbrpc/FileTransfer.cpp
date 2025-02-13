/**
 * FileTransfer.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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
#include <fstream>

#include <fdbrpc/FileTransfer.h>
#include "flow/IRandom.h"
#include "crc32/crc32c.h"

//------ FileTransferServiceImpl ------

uint32_t crc32_checksum_ifstream(std::ifstream* input_file) {
	input_file->seekg(0);
	uint32_t crc = 0;
	std::vector<char> buffer(8192);
	while (input_file->read(buffer.data(), buffer.size()) || input_file->gcount() > 0) {
		crc = crc32c_append(crc, reinterpret_cast<const uint8_t*>(buffer.data()), input_file->gcount());
	}
	return crc;
}

grpc::Status FileTransferServiceImpl::DownloadFile(grpc::ServerContext* context,
                                                   const fdbrpc::DownloadRequest* request,
                                                   grpc::ServerWriter<fdbrpc::DownloadChunk>* writer) {
	std::ifstream input_file(request->file_name(), std::ios::binary | std::ios::ate);
	if (!input_file.is_open()) {
		return grpc::Status(grpc::StatusCode::NOT_FOUND, "File found not");
	}

	const std::streamsize file_size = input_file.tellg();
	const size_t buffer_size = request->chunk_size() > 0 ? request->chunk_size() : DEFAULT_CHUNK_SIZE;
	const size_t start_chunk_index = request->first_chunk_index();
	int64_t offset = buffer_size * start_chunk_index;

	if (offset > file_size) {
		return grpc::Status(grpc::StatusCode::OUT_OF_RANGE, "Offset beyond file size");
	}

	input_file.seekg(offset);
	if (!input_file) {
		return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Failed to seek to the offset");
	}

	std::vector<char> buffer(buffer_size);

	while (input_file.read(buffer.data(), buffer_size) || input_file.gcount() > 0) {
		std::streamsize bytes_read = input_file.gcount();
		if (error_inject_ != NO_ERROR && deterministicRandom()->random01() < 0.1) {
			// Inject error 10% of the times times.
			if (error_inject_ == FAIL_RANDOMLY) {
				return grpc::Status(grpc::StatusCode::INTERNAL, "Random test failure");
			} else if (error_inject_ == FLIP_BYTE) {
				buffer[0] = ~buffer[0];
			}
		}

		fdbrpc::DownloadChunk chunk;
		chunk.set_offset(offset);
		chunk.set_data(buffer.data(), bytes_read);
		writer->Write(chunk);
		offset += bytes_read;
	}

	if (input_file.bad()) {
		return grpc::Status(grpc::StatusCode::DATA_LOSS, "Error while reading the file");
	}

	input_file.close();
	return grpc::Status::OK;
}

grpc::Status FileTransferServiceImpl::GetFileInfo(grpc::ServerContext* context,
                                                  const fdbrpc::GetFileInfoRequest* request,
                                                  fdbrpc::GetFileInfoReply* response) {

	std::ifstream input_file(request->file_name(), std::ios::binary | std::ios::ate);
	if (!input_file.is_open()) {
		return grpc::Status(grpc::StatusCode::NOT_FOUND, "File not found");
	}

	if (request->get_size()) {
		response->set_file_size(input_file.tellg());
	}

	if (request->get_crc_checksum()) {
		auto crc = crc32_checksum_ifstream(&input_file);
		response->set_crc_checksum(crc);
	}

	return grpc::Status::OK;
}

std::optional<fdbrpc::GetFileInfoReply> FileTransferClient::GetFileInfo(const std::string& filename,
                                                                        bool get_crc_checksum) {
	grpc::ClientContext context;
	fdbrpc::GetFileInfoRequest request;
	request.set_file_name(filename);
	request.set_get_size(true);
	request.set_get_crc_checksum(get_crc_checksum);

	fdbrpc::GetFileInfoReply response;
	grpc::Status status = stub_->GetFileInfo(&context, request, &response);
	if (status.ok()) {
		return { response };
	} else {
		return std::nullopt;
	}
}

//------ FileTransferClient ------

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
std::optional<size_t> FileTransferClient::DownloadFile(const std::string& filename,
                                                       const std::string& output_filename,
                                                       bool verify) {

	uint32_t expected_crc = 0;
	uint32_t expected_size = 0;
	{
		fdbrpc::GetFileInfoRequest request;
		grpc::ClientContext context;
		request.set_file_name(filename);
		request.set_get_crc_checksum(verify);
		request.set_get_size(true);
		fdbrpc::GetFileInfoReply response;
		auto res = stub_->GetFileInfo(&context, request, &response);
		if (!res.ok()) {
			return std::nullopt;
		}
		expected_crc = response.crc_checksum();
		expected_size = response.file_size();
	}

	fdbrpc::DownloadRequest request;
	request.set_file_name(filename);

	grpc::ClientContext context;
	std::unique_ptr<grpc::ClientReader<fdbrpc::DownloadChunk>> reader(stub_->DownloadFile(&context, request));

	std::ofstream output_file(output_filename, std::ios::binary | std::ios::trunc);
	if (!output_file.is_open()) {
		return std::nullopt; // Failed to open file
	}

	fdbrpc::DownloadChunk chunk;
	size_t bytes_read = 0;
	bool failed = false;
	while (reader->Read(&chunk)) {
		if (chunk.offset() != bytes_read) {
			// Abort on invalid offset
			failed = true;
			break;
		}

		output_file.write(chunk.data().data(), chunk.data().size());
		bytes_read += chunk.data().size();
	}

	// Close file after writing
	output_file.close();
	failed = failed || (bytes_read != expected_size);

	// Verify checksum
	if (!failed && verify) {
		std::ifstream output_file_reader(output_filename);
		uint32_t actual_crc = crc32_checksum_ifstream(&output_file_reader);
		failed = (actual_crc != expected_crc);
	}

	// Check final gRPC status
	grpc::Status grpc_status = reader->Finish();
	if (failed || !grpc_status.ok()) {
		// TODO: Return error codes/exception to tell caller what happened?
		// std::cerr << "File download failed: " << grpc_status.error_message() << std::endl;
		if (delete_on_close_) {
			output_file.close();
			std::remove(output_filename.c_str());
		}
		return std::nullopt;
	}

	return { bytes_read };
}

#endif // FLOW_GRPC_ENABLED

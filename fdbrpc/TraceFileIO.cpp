/*
 * TraceFileIO.cpp
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

#include "fdbrpc/TraceFileIO.h"

#if CENABLED(0, NOT_IN_CLEAN)
// TO RUN: Set the name of file you wish to track
std::string debugFileName = "storage-3807bf710afaa6d39d23db60d6e687c8.fdb-wal";
std::map<int64_t, uint8_t*> debugFileData;
std::map<int64_t, uint8_t*> debugFileMask;
std::map<int64_t, int64_t> debugFileRegions;
uint8_t* debugFileSetPage;

// Setup memory data structures for file debugging.
// TO RUN: setup debugFileRegions map with the regions of the file you wish to track.  Keys of the map are offsets in
// the file, and values are the lengths of the region
void debugFileSetup() {
	debugFileRegions[0] = 1e8;
	for (auto itr = debugFileRegions.begin(); itr != debugFileRegions.end(); ++itr) {
		debugFileData[itr->first] = (uint8_t*)malloc(itr->second);
		memset(debugFileData[itr->first], 0, itr->second);

		debugFileMask[itr->first] = (uint8_t*)malloc(itr->second);
		memset(debugFileMask[itr->first], 0, itr->second);
	}

	debugFileSetPage = (uint8_t*)malloc(4096);
	memset(debugFileSetPage, 1, 4096);
}

// Trim a file path to the file name
std::string debugFileTrim(std::string filename) {
	int index = filename.find_last_of("/");
	if (index == filename.npos)
		return filename;
	else
		return filename.substr(index + 1);
}

// Checks if a block of memory has been written
bool debugFileIsSet(uint8_t* storeMask, int64_t offset, int64_t length) {
	for (int64_t i = 0; i < length; i += 4096)
		if (memcmp(&storeMask[offset + i], debugFileSetPage, std::min((int64_t)4096, length - i)))
			return false;

	return true;
}

// Checks that a given block of data is the same as what has been written by a call to debugFileSet
void debugFileCheck(std::string context, std::string file, const void* data, int64_t offset, int length) {
	if (debugFileRegions.empty())
		debugFileSetup();
	if (debugFileTrim(file) == debugFileName) {
		bool found = false;
		for (auto itr = debugFileRegions.begin(); itr != debugFileRegions.end(); ++itr) {
			if (offset + length > itr->first && offset < itr->first + itr->second) {
				found = true;
				uint8_t* storeData = debugFileData[itr->first];
				uint8_t* storeMask = debugFileMask[itr->first];

				ASSERT(storeData && storeMask);

				int64_t dataOffset = std::max((int64_t)0, itr->first - offset);
				int64_t dbgOffset = std::max((int64_t)0, offset - itr->first);
				int64_t cmpLength = std::min(length - dataOffset, itr->second - dbgOffset);
				ASSERT(cmpLength > 0);
				TraceEvent("DebugFileCheck")
				    .detail("Context", context)
				    .detail("Filename", file)
				    .detail("Offset", offset)
				    .detail("Length", length);
				bool success = true;
				if (debugFileIsSet(storeMask, dbgOffset, cmpLength)) {
					//TraceEvent("DebugFileCheckMemCmp").detail("Context", context).detail("Filename", file).detail("Offset", offset).detail("Length", length).detail("DataOffset", dataOffset).detail("DbgOffset", dbgOffset).detail("OverlapLength", cmpLength);
					if (memcmp(&((uint8_t*)data)[dataOffset], &storeData[dbgOffset], cmpLength))
						success = false;
				} else {
					TraceEvent("DebugFileUnsetCheck")
					    .detail("Context", context)
					    .detail("Filename", file)
					    .detail("Offset", offset)
					    .detail("Length", length);
					for (int64_t i = 0; i < cmpLength; i++) {
						if (storeMask[dbgOffset + i] && storeData[dbgOffset + i] != ((uint8_t*)data)[dataOffset + i]) {
							success = false;
							break;
						}
					}
				}

				if (!success)
					TraceEvent(SevWarnAlways, "DebugFileFail")
					    .detail("Context", context)
					    .detail("Filename", file)
					    .detail("Offset", offset)
					    .detail("Length", length);
			}
		}
		if (!found)
			TraceEvent("DebugFileSkippingCheck")
			    .detail("Context", context)
			    .detail("Filename", file)
			    .detail("Offset", offset)
			    .detail("Length", length);
	}
}

// Updates the in-memory copy of tracked data at a given offset
void debugFileSet(std::string context, std::string file, const void* data, int64_t offset, int length) {
	if (debugFileRegions.empty())
		debugFileSetup();
	if (debugFileTrim(file) == debugFileName) {
		bool found = false;
		for (auto itr = debugFileRegions.begin(); itr != debugFileRegions.end(); ++itr) {
			if (offset + length > itr->first && offset < itr->first + itr->second) {
				found = true;

				uint8_t* storeData = debugFileData[itr->first];
				uint8_t* storeMask = debugFileMask[itr->first];
				ASSERT(storeData && storeMask);

				int64_t dataOffset = std::max((int64_t)0, itr->first - offset);
				int64_t dbgOffset = std::max((int64_t)0, offset - itr->first);
				int64_t cmpLength = std::min(length - dataOffset, itr->second - dbgOffset);
				ASSERT(cmpLength > 0);
				TraceEvent("DebugFileSet")
				    .detail("Context", context)
				    .detail("Filename", file)
				    .detail("Offset", offset)
				    .detail("Length", length);
				//TraceEvent("DebugFileSetMemCpy").detail("Context", context).detail("Filename", file).detail("Offset", offset).detail("Length", length).detail("DataOffset", dataOffset).detail("DbgOffset", dbgOffset).detail("OverlapLength", std::min(length - dataOffset, itr->second - dbgOffset));
				memcpy(&storeData[dbgOffset], &((uint8_t*)data)[dataOffset], cmpLength);
				memset(&storeMask[dbgOffset], 1, cmpLength);
			}
		}
		if (!found)
			TraceEvent("DebugFileSkippingSet")
			    .detail("Context", context)
			    .detail("Filename", file)
			    .detail("Offset", offset)
			    .detail("Length", length);
	}
}

// Updates the in-memory copy of tracked data to account for truncates (this simply invalidates any data after truncate
// point)
void debugFileTruncate(std::string context, std::string file, int64_t offset) {
	if (debugFileRegions.empty())
		debugFileSetup();
	if (debugFileTrim(file) == debugFileName) {
		bool found = false;
		for (auto itr = debugFileRegions.begin(); itr != debugFileRegions.end(); ++itr) {
			if (itr->first + itr->second > offset) {
				found = true;
				TraceEvent("DebugFileTruncate")
				    .detail("Context", context)
				    .detail("Filename", file)
				    .detail("Offset", offset);
				uint8_t* storeMask = debugFileMask[itr->first];
				ASSERT(storeMask);

				int64_t dbgOffset = std::max((int64_t)0, offset - itr->first);
				memset(&storeMask[dbgOffset], 0, itr->second - dbgOffset);
			}
		}
		if (!found)
			TraceEvent("DebugFileSkippingTruncate")
			    .detail("Context", context)
			    .detail("Filename", file)
			    .detail("Offset", offset);
	}
}

#else
void debugFileCheck(std::string context, std::string file, const void* data, int64_t offset, int length) {}
void debugFileSet(std::string context, std::string file, const void* data, int64_t offset, int length) {}
void debugFileTruncate(std::string context, std::string file, int64_t offset) {}
#endif

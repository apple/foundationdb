/*
 * IAsyncFile.actor.cpp
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

#include "IAsyncFile.h"
#include "flow/Error.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/UnitTest.h"
#include <iostream>

IAsyncFile::~IAsyncFile() = default;

ACTOR static Future<Void> incrementalDeleteHelper( std::string filename, bool mustBeDurable, int64_t truncateAmt, double interval ) {
	state Reference<IAsyncFile> file;
	state int64_t remainingFileSize;
	state bool exists = fileExists(filename);

	if(exists) {
		Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(filename, IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_UNCACHED, 0));
		file = f;

		int64_t fileSize = wait(file->size());
		remainingFileSize = fileSize;
	}

	Void _ = wait(IAsyncFileSystem::filesystem()->deleteFile(filename, mustBeDurable));

	if(exists) {
		for( ; remainingFileSize > 0; remainingFileSize -= truncateAmt ){
			Void _ = wait(file->truncate(remainingFileSize));
			Void _ = wait(file->sync());
			Void _ = wait(delay(interval));
		}
	}

	return Void();
}

Future<Void> IAsyncFileSystem::incrementalDeleteFile( std::string filename, bool mustBeDurable ) {
	return uncancellable(incrementalDeleteHelper(
		filename,
		mustBeDurable,
		FLOW_KNOBS->INCREMENTAL_DELETE_TRUNCATE_AMOUNT,
		FLOW_KNOBS->INCREMENTAL_DELETE_INTERVAL));
}

TEST_CASE( "fileio/incrementalDelete" ) {
	//about 5GB
	state int64_t fileSize = 5e9;
	state std::string filename = "/tmp/__JUNK__";
	state Reference<IAsyncFile> f =
		wait(IAsyncFileSystem::filesystem()->open(
			filename,
			IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE,
			0));
	Void _ = wait(f->sync());
	Void _ = wait(f->truncate(fileSize));
	//close the file by deleting the reference
	f.clear();
	Void _ = wait(IAsyncFileSystem::filesystem()->incrementalDeleteFile(filename, true));
	return Void();
}

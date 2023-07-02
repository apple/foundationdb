/*
 * IKeyValueStore.h
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

#ifndef FDBSERVER_IKEYVALUESTORE_H
#define FDBSERVER_IKEYVALUESTORE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/IKeyValueStore.actor.h"
#include "flow/BooleanParam.h"

extern IKeyValueStore* keyValueStoreSQLite(std::string const& filename,
                                           UID logID,
                                           KeyValueStoreType storeType,
                                           bool checkChecksums = false,
                                           bool checkIntegrity = false);
extern IKeyValueStore* keyValueStoreRedwoodV1(std::string const& filename,
                                              UID logID,
                                              Reference<AsyncVar<struct ServerDBInfo> const> db = {},
                                              Optional<EncryptionAtRestMode> encryptionMode = {},
                                              int64_t pageCacheBytes = 0);
extern IKeyValueStore* keyValueStoreRocksDB(std::string const& path,
                                            UID logID,
                                            KeyValueStoreType storeType,
                                            bool checkChecksums = false,
                                            bool checkIntegrity = false);
extern IKeyValueStore* keyValueStoreShardedRocksDB(std::string const& path,
                                                   UID logID,
                                                   KeyValueStoreType storeType,
                                                   bool checkChecksums = false,
                                                   bool checkIntegrity = false);
extern IKeyValueStore* keyValueStoreMemory(std::string const& basename,
                                           UID logID,
                                           int64_t memoryLimit,
                                           std::string ext = "fdq",
                                           KeyValueStoreType storeType = KeyValueStoreType::MEMORY);
extern IKeyValueStore* keyValueStoreLogSystem(class IDiskQueue* queue,
                                              Reference<AsyncVar<struct ServerDBInfo> const> db,
                                              UID logID,
                                              int64_t memoryLimit,
                                              bool disableSnapshot,
                                              bool replaceContent,
                                              bool exactRecovery,
                                              bool enableEncryption);

extern IKeyValueStore* openRemoteKVStore(KeyValueStoreType storeType,
                                         std::string const& filename,
                                         UID logID,
                                         int64_t memoryLimit,
                                         bool checkChecksums = false,
                                         bool checkIntegrity = false);

IKeyValueStore* openKVStore(KeyValueStoreType storeType,
                            std::string const& filename,
                            UID logID,
                            int64_t memoryLimit,
                            bool checkChecksums = false,
                            bool checkIntegrity = false,
                            bool openRemotely = false,
                            Reference<AsyncVar<struct ServerDBInfo> const> db = {},
                            Optional<EncryptionAtRestMode> encryptionMode = {},
                            int64_t pageCacheBytes = 0);

void GenerateIOLogChecksumFile(std::string filename);
Future<Void> KVFileCheck(std::string const& filename, bool const& integrity);
Future<Void> KVFileDump(std::string const& filename);

#endif

set(FDBCLIENT_SRCS
  AsyncFileS3BlobStore.actor.cpp
  AsyncFileS3BlobStore.actor.h
  AsyncTaskThread.actor.cpp
  AsyncTaskThread.h
  Atomic.h
  AutoPublicAddress.cpp
  BackupAgent.actor.h
  BackupAgentBase.actor.cpp
  BackupContainer.actor.cpp
  BackupContainer.h
  BackupContainerFileSystem.actor.cpp
  BackupContainerFileSystem.h
  BackupContainerLocalDirectory.actor.cpp
  BackupContainerLocalDirectory.h
  BackupContainerS3BlobStore.actor.cpp
  BackupContainerS3BlobStore.h
  ClientBooleanParams.cpp
  ClientBooleanParams.h
  ClientKnobCollection.cpp
  ClientKnobCollection.h
  ClientKnobs.cpp
  ClientKnobs.h
  ClientLogEvents.h
  ClientWorkerInterface.h
  ClusterInterface.h
  CommitProxyInterface.h
  CommitTransaction.h
  ConfigKnobs.cpp
  ConfigKnobs.h
  ConfigTransactionInterface.cpp
  ConfigTransactionInterface.h
  CoordinationInterface.h
  DatabaseBackupAgent.actor.cpp
  DatabaseConfiguration.cpp
  DatabaseConfiguration.h
  DatabaseContext.h
  EventTypes.actor.h
  FDBOptions.h
  FDBTypes.cpp
  FDBTypes.h
  FileBackupAgent.actor.cpp
  GlobalConfig.h
  GlobalConfig.actor.h
  GlobalConfig.actor.cpp
  GrvProxyInterface.h
  HTTP.actor.cpp
  IClientApi.h
  IConfigTransaction.cpp
  IConfigTransaction.h
  ISingleThreadTransaction.cpp
  ISingleThreadTransaction.h
  JsonBuilder.cpp
  JsonBuilder.h
  KeyBackedTypes.h
  KeyRangeMap.actor.cpp
  KeyRangeMap.h
  Knobs.h
  IKnobCollection.cpp
  IKnobCollection.h
  ManagementAPI.actor.cpp
  ManagementAPI.actor.h
  MonitorLeader.actor.cpp
  MonitorLeader.h
  MultiVersionAssignmentVars.h
  MultiVersionTransaction.actor.cpp
  MultiVersionTransaction.h
  MutationList.h
  NativeAPI.actor.cpp
  NativeAPI.actor.h
  Notified.h
  ParallelStream.actor.cpp
  ParallelStream.actor.h
  PaxosConfigTransaction.actor.cpp
  PaxosConfigTransaction.h
  SimpleConfigTransaction.actor.cpp
  SpecialKeySpace.actor.cpp
  SpecialKeySpace.actor.h
  ReadYourWrites.actor.cpp
  ReadYourWrites.h
  RestoreInterface.cpp
  RestoreInterface.h
  RunTransaction.actor.h
  RYWIterator.cpp
  RYWIterator.h
  S3BlobStore.actor.cpp
  Schemas.cpp
  Schemas.h
  ServerKnobCollection.cpp
  ServerKnobCollection.h
  ServerKnobs.cpp
  ServerKnobs.h
  SimpleConfigTransaction.h
  SnapshotCache.h
  SpecialKeySpace.actor.cpp
  SpecialKeySpace.actor.h
  Status.h
  StatusClient.actor.cpp
  StatusClient.h
  StorageServerInterface.cpp
  StorageServerInterface.h
  Subspace.cpp
  Subspace.h
  SystemData.cpp
  SystemData.h
  TagThrottle.actor.cpp
  TagThrottle.h
  TaskBucket.actor.cpp
  TaskBucket.h
  TestKnobCollection.cpp
  TestKnobCollection.h
  ThreadSafeTransaction.cpp
  ThreadSafeTransaction.h
  Tuple.cpp
  Tuple.h
  VersionedMap.actor.h
  VersionedMap.h
  VersionedMap.cpp
  WriteMap.h
  json_spirit/json_spirit_error_position.h
  json_spirit/json_spirit_reader_template.h
  json_spirit/json_spirit_value.h
  json_spirit/json_spirit_writer_options.h
  json_spirit/json_spirit_writer_template.h
  libb64/cdecode.c
  libb64/cencode.c
  md5/md5.c
  sha1/SHA1.cpp
  zipf.c
  zipf.h)

set(options_srcs ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp)

vexillographer_compile(TARGET fdboptions LANG cpp OUT ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp)

set(BUILD_AZURE_BACKUP OFF CACHE BOOL "Build Azure backup client")
if(BUILD_AZURE_BACKUP)
  add_compile_definitions(BUILD_AZURE_BACKUP)
  set(FDBCLIENT_SRCS
    ${FDBCLIENT_SRCS}
    BackupContainerAzureBlobStore.actor.cpp
    BackupContainerAzureBlobStore.h)

  configure_file(azurestorage.cmake azurestorage-download/CMakeLists.txt)

  execute_process(
    COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
    RESULT_VARIABLE results
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/azurestorage-download
  )

  if(results)
    message(FATAL_ERROR "Configuration step for AzureStorage has Failed. ${results}")
  endif()

  execute_process(
    COMMAND ${CMAKE_COMMAND} --build . --config Release
    RESULT_VARIABLE results
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/azurestorage-download
  )

  if(results)
    message(FATAL_ERROR "Build step for AzureStorage has Failed. ${results}")
  endif()

  add_subdirectory(
    ${CMAKE_CURRENT_BINARY_DIR}/azurestorage-src
    ${CMAKE_CURRENT_BINARY_DIR}/azurestorage-build
  )
endif()

add_flow_target(STATIC_LIBRARY NAME fdbclient SRCS ${FDBCLIENT_SRCS} ADDL_SRCS ${options_srcs})
add_dependencies(fdbclient fdboptions fdb_c_options)
if(BUILD_AZURE_BACKUP)
  target_link_libraries(fdbclient PUBLIC fdbrpc PRIVATE curl uuid azure-storage-lite)
else()
  target_link_libraries(fdbclient PUBLIC fdbrpc)
endif()

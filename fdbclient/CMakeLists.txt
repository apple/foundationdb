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
  ClientLogEvents.h
  ClientWorkerInterface.h
  ClusterInterface.h
  CommitTransaction.h
  CoordinationInterface.h
  DatabaseBackupAgent.actor.cpp
  DatabaseConfiguration.cpp
  DatabaseConfiguration.h
  DatabaseContext.h
  EventTypes.actor.h
  FDBOptions.h
  FDBTypes.h
  FileBackupAgent.actor.cpp
  GrvProxyInterface.h
  HTTP.actor.cpp
  IClientApi.h
  JsonBuilder.cpp
  JsonBuilder.h
  KeyBackedTypes.h
  KeyRangeMap.actor.cpp
  KeyRangeMap.h
  Knobs.cpp
  Knobs.h
  ManagementAPI.actor.cpp
  ManagementAPI.actor.h
  CommitProxyInterface.h
  MonitorLeader.actor.cpp
  MonitorLeader.h
  MultiVersionAssignmentVars.h
  MultiVersionTransaction.actor.cpp
  MultiVersionTransaction.h
  MutationList.h
  NativeAPI.actor.cpp
  NativeAPI.actor.h
  Notified.h
  SpecialKeySpace.actor.cpp
  SpecialKeySpace.actor.h
  ReadYourWrites.actor.cpp
  ReadYourWrites.h
  RestoreWorkerInterface.actor.h
  RunTransaction.actor.h
  RYWIterator.cpp
  RYWIterator.h
  S3BlobStore.actor.cpp
  Schemas.cpp
  Schemas.h
  SnapshotCache.h
  Status.h
  StatusClient.actor.cpp
  StatusClient.h
  StorageServerInterface.h
  Subspace.cpp
  Subspace.h
  SystemData.cpp
  SystemData.h
  TagThrottle.actor.cpp
  TagThrottle.h
  TaskBucket.actor.cpp
  TaskBucket.h
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
add_dependencies(fdbclient fdboptions)
if(BUILD_AZURE_BACKUP)
  target_link_libraries(fdbclient PUBLIC fdbrpc PRIVATE curl uuid azure-storage-lite)
else()
  target_link_libraries(fdbclient PUBLIC fdbrpc)
endif()

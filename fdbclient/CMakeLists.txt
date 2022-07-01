set(FDBCLIENT_SRCS
  ActorLineageProfiler.h
  ActorLineageProfiler.cpp
  AnnotateActor.h
  AnnotateActor.cpp
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
  BlobConnectionProvider.h
  BlobConnectionProvider.cpp
  BlobWorkerInterface.h
  BlobGranuleReader.actor.cpp
  BlobGranuleReader.actor.h
  BlobGranuleCommon.h
  BlobGranuleFiles.cpp
  BlobGranuleFiles.h
  BlobMetadataUtils.h
  BlobWorkerCommon.h
  ClientBooleanParams.cpp
  ClientBooleanParams.h
  ClientKnobCollection.cpp
  ClientKnobCollection.h
  ClientKnobs.cpp
  ClientKnobs.h
  ClientLogEvents.h
  ClientVersion.h
  ClientWorkerInterface.h
  ClusterConnectionFile.actor.cpp
  ClusterConnectionFile.h
  ClusterConnectionKey.actor.cpp
  ClusterConnectionKey.actor.h
  ClusterConnectionMemoryRecord.actor.cpp
  ClusterConnectionMemoryRecord.h
  ClusterInterface.h
  CommitProxyInterface.h
  CommitTransaction.h
  ConfigKnobs.cpp
  ConfigKnobs.h
  ConfigTransactionInterface.cpp
  ConfigTransactionInterface.h
  ConvertUTF.h
  CoordinationInterface.h
  DatabaseBackupAgent.actor.cpp
  DatabaseConfiguration.cpp
  DatabaseConfiguration.h
  DatabaseContext.h
  EventTypes.actor.h
  FDBOptions.h
  FDBTypes.cpp
  FDBTypes.h
  FluentDSampleIngestor.cpp
  FileBackupAgent.actor.cpp
  GenericManagementAPI.actor.h
  GlobalConfig.h
  GlobalConfig.actor.h
  GlobalConfig.actor.cpp
  GrvProxyInterface.h
  HTTP.h
  HTTP.actor.cpp
  HighContentionPrefixAllocator.actor.h
  IClientApi.h
  IConfigTransaction.cpp
  IConfigTransaction.h
  ISingleThreadTransaction.cpp
  ISingleThreadTransaction.h
  JSONDoc.h
  JsonBuilder.cpp
  JsonBuilder.h
  KeyBackedTypes.h
  KeyRangeMap.actor.cpp
  KeyRangeMap.h
  Knobs.h
  IKnobCollection.cpp
  IKnobCollection.h
  LocalClientAPI.cpp
  LocalClientAPI.h
  ManagementAPI.actor.cpp
  ManagementAPI.actor.h
  MonitorLeader.actor.cpp
  MonitorLeader.h
  MultiVersionAssignmentVars.h
  MultiVersionTransaction.actor.cpp
  MultiVersionTransaction.h
  MutationList.h
  MutationLogReader.actor.cpp
  MutationLogReader.actor.h
  NameLineage.h
  NameLineage.cpp
  NativeAPI.actor.cpp
  NativeAPI.actor.h
  Notified.h
  ParallelStream.actor.cpp
  ParallelStream.actor.h
  PaxosConfigTransaction.actor.cpp
  PaxosConfigTransaction.h
  PImpl.h
  ProcessInterface.h
  SimpleConfigTransaction.actor.cpp
  SpecialKeySpace.actor.cpp
  SpecialKeySpace.actor.h
  RESTClient.h
  RESTClient.actor.cpp
  RESTUtils.h
  RESTUtils.actor.cpp
  ReadYourWrites.actor.cpp
  ReadYourWrites.h
  RestoreInterface.cpp
  RestoreInterface.h
  RunTransaction.actor.h
  RYWIterator.cpp
  RYWIterator.h
  S3BlobStore.h
  S3BlobStore.actor.cpp
  Schemas.cpp
  Schemas.h
  ServerKnobCollection.cpp
  ServerKnobCollection.h
  ServerKnobs.cpp
  ServerKnobs.h
  SimpleConfigTransaction.h
  SimpleIni.h
  SnapshotCache.h
  SpecialKeySpace.actor.cpp
  SpecialKeySpace.actor.h
  Status.h
  StatusClient.actor.cpp
  StatusClient.h
  StorageServerInterface.cpp
  StorageServerInterface.h
  StorageServerShard.h
  StorageCheckpoint.h
  Subspace.cpp
  Subspace.h
  StackLineage.h
  StackLineage.cpp
  SystemData.cpp
  SystemData.h
  TagThrottle.actor.cpp
  TagThrottle.actor.h
  TaskBucket.actor.cpp
  TaskBucket.h
  Tenant.cpp
  Tenant.h
  TestKnobCollection.cpp
  TestKnobCollection.h
  ThreadSafeTransaction.cpp
  ThreadSafeTransaction.h
  Tracing.h
  Tracing.actor.cpp
  TransactionLineage.h
  Tuple.cpp
  Tuple.h
  VersionedMap.actor.h
  VersionedMap.h
  VersionedMap.cpp
  Versionstamp.cpp
  Versionstamp.h
  VersionVector.h
  VersionVector.cpp
  WriteMap.h
  WriteMap.cpp
  json_spirit/json_spirit_error_position.h
  json_spirit/json_spirit_reader_template.h
  json_spirit/json_spirit_value.h
  json_spirit/json_spirit_writer_options.h
  json_spirit/json_spirit_writer_template.h
  libb64/encode.h
  libb64/decode.h
  libb64/cdecode.h
  libb64/cdecode.c
  libb64/cencode.h
  libb64/cencode.c
  md5/md5.h
  md5/md5.c
  rapidxml/rapidxml.hpp
  rapidxml/rapidxml_iterators.hpp
  rapidxml/rapidxml_print.hpp
  rapidxml/rapidxml_utils.hpp
  sha1/SHA1.h
  sha1/SHA1.cpp
  zipf.c
  zipf.h)

message(STATUS "FDB version is ${FDB_VERSION}")
message(STATUS "FDB package name is ${FDB_PACKAGE_NAME}")

set(options_srcs ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp)

make_directory(${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/)
vexillographer_compile(TARGET fdboptions_vex LANG cpp OUT ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp)

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/FDBOptions.g.h
  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h
  COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/FDBOptions.g.h)

vexillographer_compile(TARGET fdboptions_c LANG c OUT ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/fdb_c_options.g.h
  OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/fdb_c_options.g.h)

add_custom_target(fdboptions DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/FDBOptions.g.h)
add_dependencies(fdboptions fdboptions_c)

################################################################################
# Build information
################################################################################

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/BuildFlags.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/BuildFlags.h)


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


if(WITH_AWS_BACKUP)
  add_compile_definitions(BUILD_AWS_BACKUP)

  set(FDBCLIENT_SRCS
    ${FDBCLIENT_SRCS}
    FDBAWSCredentialsProvider.h)

  include(awssdk)
endif()

add_flow_target(STATIC_LIBRARY NAME fdbclient SRCS ${FDBCLIENT_SRCS} ADDL_SRCS ${options_srcs})
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/versions.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/include/fdbclient/versions.h)
add_dependencies(fdbclient fdboptions)
target_link_libraries(fdbclient PUBLIC fdbrpc msgpack)

# Create a separate fdbclient library with sampling enabled. This lets
# fdbserver retain sampling functionality in client code while disabling
# sampling for pure clients.
add_flow_target(STATIC_LIBRARY NAME fdbclient_sampling SRCS ${FDBCLIENT_SRCS} ADDL_SRCS ${options_srcs})
add_dependencies(fdbclient_sampling fdboptions)
target_link_libraries(fdbclient_sampling PUBLIC fdbrpc_sampling msgpack)
target_compile_definitions(fdbclient_sampling PRIVATE -DENABLE_SAMPLING)
if(WIN32)
  add_dependencies(fdbclient_sampling_actors fdbclient_actors)
endif()

if(BUILD_AZURE_BACKUP)
  target_link_libraries(fdbclient PRIVATE curl uuid azure-storage-lite)
  target_link_libraries(fdbclient_sampling PRIVATE curl uuid azure-storage-lite)
endif()

if(BUILD_AWS_BACKUP)
  target_link_libraries(fdbclient PUBLIC awssdk_target)
  target_link_libraries(fdbclient_sampling PUBLIC awssdk_target)
endif()

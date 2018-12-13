set(FDBCLIENT_SRCS
  AsyncFileBlobStore.actor.cpp
  AsyncFileBlobStore.actor.h
  Atomic.h
  AutoPublicAddress.cpp
  BackupAgent.h
  BackupAgentBase.actor.cpp
  BackupContainer.actor.cpp
  BackupContainer.h
  BlobStore.actor.cpp
  ClientDBInfo.h
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
  FailureMonitorClient.actor.cpp
  FailureMonitorClient.h
  FDBOptions.h
  FDBTypes.h
  FileBackupAgent.actor.cpp
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
  ManagementAPI.h
  MasterProxyInterface.h
  MetricLogger.actor.cpp
  MetricLogger.h
  MonitorLeader.actor.cpp
  MonitorLeader.h
  MultiVersionAssignmentVars.h
  MultiVersionTransaction.actor.cpp
  MultiVersionTransaction.h
  MutationList.h
  NativeAPI.actor.cpp
  NativeAPI.h
  Notified.h
  ReadYourWrites.actor.cpp
  ReadYourWrites.h
  RunTransaction.actor.h
  RYWIterator.cpp
  RYWIterator.h
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
  TaskBucket.actor.cpp
  TaskBucket.h
  ThreadSafeTransaction.actor.cpp
  ThreadSafeTransaction.h
  Tuple.cpp
  Tuple.h
  VersionedMap.actor.h
  VersionedMap.h
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
  ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp)


add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp
  COMMAND ${MONO_EXECUTABLE} ${VEXILLOGRAPHER_EXE} ${CMAKE_CURRENT_SOURCE_DIR}/vexillographer/fdb.options cpp ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/vexillographer/fdb.options vexillographer
  COMMENT "Generate FDBOptions c++ files")
add_custom_target(fdboptions DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.h ${CMAKE_CURRENT_BINARY_DIR}/FDBOptions.g.cpp)

actor_set(FDBCLIENT_BUILD "${FDBCLIENT_SRCS}")
add_library(fdbclient STATIC ${FDBCLIENT_BUILD})
add_dependencies(fdbclient fdboptions)
actor_compile(fdbclient "${FDBCLIENT_SRCS}")
target_link_libraries(fdbclient PUBLIC fdbrpc)

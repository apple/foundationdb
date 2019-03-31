set(FDBRPC_SRCS
  AsyncFileCached.actor.h
  AsyncFileEIO.actor.h
  AsyncFileKAIO.actor.h
  AsyncFileNonDurable.actor.h
  AsyncFileReadAhead.actor.h
  AsyncFileWinASIO.actor.h
  AsyncFileCached.actor.cpp
  AsyncFileNonDurable.actor.cpp
  AsyncFileWriteChecker.cpp
  batcher.actor.h
  crc32c.cpp
  FailureMonitor.actor.cpp
  FlowTransport.actor.cpp
  genericactors.actor.h
  genericactors.actor.cpp
  IAsyncFile.actor.cpp
  LoadBalance.actor.h
  Locality.cpp
  Net2FileSystem.cpp
  networksender.actor.h
  Platform.cpp
  QueueModel.cpp
  ReplicationPolicy.cpp
  ReplicationTypes.cpp
  ReplicationUtils.cpp
  sim2.actor.cpp
  sim_validation.cpp
  TLSConnection.actor.cpp
  TraceFileIO.cpp
  # C files
  libcoroutine/Common.c
  libcoroutine/Coro.c
  zlib/adler32.c
  zlib/crc32.c
  zlib/deflate.c
  zlib/gzclose.c
  zlib/gzlib.c
  zlib/gzread.c
  zlib/gzwrite.c
  zlib/infback.c
  zlib/inffast.c
  zlib/inflate.c
  zlib/inftrees.c
  zlib/trees.c
  zlib/zutil.c)

if(APPLE)
  list(APPEND FDBRPC_SRCS libcoroutine/asm.S)
endif()
if(NOT WIN32)
  list(APPEND FDBRPC_SRCS libcoroutine/context.c libeio/eio.c)
endif()

set(FDBRPC_SRCS_DISABLE_ACTOR_WITHOUT_WAIT_WARNING
  ActorFuzz.actor.cpp
  FlowTests.actor.cpp
  dsltest.actor.cpp)

add_flow_target(STATIC_LIBRARY NAME fdbrpc
  SRCS ${FDBRPC_SRCS}
  DISABLE_ACTOR_WITHOUT_WAIT_WARNING ${FDBRPC_SRCS_DISABLE_ACTOR_WITHOUT_WAIT_WARNING})
target_include_directories(fdbrpc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)
target_link_libraries(fdbrpc PUBLIC flow)

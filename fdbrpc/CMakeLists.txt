set(FDBRPC_SRCS
  ActorFuzz.actor.cpp
  AsyncFileBlobStore.actor.cpp
  AsyncFileBlobStore.actor.h
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
  BlobStore.actor.cpp
  crc32c.cpp
  dsltest.actor.cpp
  FailureMonitor.actor.cpp
  FlowTests.actor.cpp
  FlowTransport.actor.cpp
  genericactors.actor.h
  genericactors.actor.cpp
  HTTP.actor.cpp
  IAsyncFile.actor.cpp
  LoadBalance.actor.h
  Locality.cpp
  Net2FileSystem.cpp
  Platform.cpp
  QueueModel.cpp
  ReplicationPolicy.cpp
  ReplicationTypes.cpp
  ReplicationUtils.cpp
  sha1/SHA1.cpp
  sim2.actor.cpp
  sim_validation.cpp
  TLSConnection.actor.cpp
  TraceFileIO.cpp
  # C files
  libb64/cdecode.c
  libb64/cencode.c
  libcoroutine/Common.c
  libcoroutine/context.c
  libcoroutine/Coro.c
  libeio/eio.c
  md5/md5.c
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
  list(APPEND FDBRPC_SRCS libcoroutine/asm.S libcoroutine/context.c)
endif()

actor_set(FDBRPC_BUILD "${FDBRPC_SRCS}")
add_library(fdbrpc STATIC ${FDBRPC_BUILD})
actor_compile(fdbrpc "${FDBRPC_SRCS}")
target_include_directories(fdbrpc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)
target_link_libraries(fdbrpc PUBLIC flow)

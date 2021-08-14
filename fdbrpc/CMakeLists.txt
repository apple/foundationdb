set(FDBRPC_SRCS
  AsyncFileCached.actor.h
  AsyncFileEIO.actor.h
  AsyncFileEncrypted.h
  AsyncFileKAIO.actor.h
  AsyncFileNonDurable.actor.h
  AsyncFileReadAhead.actor.h
  AsyncFileWinASIO.actor.h
  AsyncFileCached.actor.cpp
  AsyncFileNonDurable.actor.cpp
  AsyncFileWriteChecker.cpp
  FailureMonitor.actor.cpp
  FlowTransport.actor.cpp
  genericactors.actor.h
  genericactors.actor.cpp
  HealthMonitor.actor.cpp
  IAsyncFile.actor.cpp
  LoadBalance.actor.cpp
  LoadBalance.actor.h
  Locality.cpp
  Net2FileSystem.cpp
  networksender.actor.h
  QueueModel.cpp
  ReplicationPolicy.cpp
  ReplicationTypes.cpp
  ReplicationUtils.cpp
  SimExternalConnection.actor.cpp
  SimExternalConnection.h
  Stats.actor.cpp
  Stats.h
  sim2.actor.cpp
  sim_validation.cpp
  TimedRequest.h
  TraceFileIO.cpp
  TSSComparison.h)

if(WITH_TLS AND NOT WIN32)
  set(FDBRPC_SRCS
    ${FDBRPC_SRCS}
    AsyncFileEncrypted.actor.cpp)
endif()

set(COMPILE_EIO OFF)

if(NOT WIN32)
  if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    find_library(EIO eio)
    if(NOT EIO)
      set(COMPILE_EIO ON)
    endif()
  else()
    include(ExternalProject)
    ExternalProject_Get_Property(io_uring BINARY_DIR)
    set(URING_LIBRARIES ${BINARY_DIR}/src/liburing.a)
    set(URING_INCLUDE_DIR ${BINARY_DIR}/src/include)
    set(COMPILE_EIO ON)
  endif()
endif()

set(FDBRPC_SRCS_DISABLE_ACTOR_DIAGNOSTICS
  ActorFuzz.actor.cpp
  FlowTests.actor.cpp
  dsltest.actor.cpp)

add_flow_target(STATIC_LIBRARY NAME fdbrpc
  SRCS ${FDBRPC_SRCS}
  DISABLE_ACTOR_DIAGNOSTICS ${FDBRPC_SRCS_DISABLE_ACTOR_DIAGNOSTICS})
target_include_directories(fdbrpc PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)
target_link_libraries(fdbrpc PUBLIC flow)

add_flow_target(STATIC_LIBRARY NAME fdbrpc_sampling
  SRCS ${FDBRPC_SRCS}
  DISABLE_ACTOR_DIAGNOSTICS ${FDBRPC_SRCS_DISABLE_ACTOR_DIAGNOSTICS})
target_include_directories(fdbrpc_sampling PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)
target_link_libraries(fdbrpc_sampling PUBLIC flow_sampling)
target_compile_definitions(fdbrpc_sampling PRIVATE -DENABLE_SAMPLING)

if(COMPILE_EIO)
  add_library(eio STATIC libeio/eio.c)
  if(USE_VALGRIND)
    target_link_libraries(eio PUBLIC Valgrind)
  endif()
  target_compile_definitions(eio PRIVATE USE_UCONTEXT)
  target_compile_options(eio BEFORE PRIVATE -w) # disable warnings for eio
  target_link_libraries(fdbrpc PRIVATE eio)
  target_link_libraries(fdbrpc_sampling PRIVATE eio)
  target_include_directories(fdbrpc PRIVATE ${URING_INCLUDE_DIR})
  target_link_libraries(fdbrpc PRIVATE ${URING_LIBRARIES})
  target_include_directories(fdbrpc_sampling PRIVATE ${URING_INCLUDE_DIR})
  target_link_libraries(fdbrpc_sampling PRIVATE ${URING_LIBRARIES})
endif()
if(WIN32)
  add_library(coro STATIC libcoroutine/Common.c libcoroutine/Coro.c)
  target_link_libraries(fdbrpc PRIVATE coro)
  target_link_libraries(fdbrpc_sampling PRIVATE coro)
endif()

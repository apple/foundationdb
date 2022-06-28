set(FDBRPC_SRCS
  ActorFuzz.h
  AsyncFileCached.actor.h
  AsyncFileChaos.h
  AsyncFileEIO.actor.h
  AsyncFileEncrypted.h
  AsyncFileKAIO.actor.h
  AsyncFileNonDurable.actor.h
  AsyncFileReadAhead.actor.h
  AsyncFileWinASIO.actor.h
  AsyncFileCached.actor.cpp
  AsyncFileEncrypted.actor.cpp
  AsyncFileNonDurable.actor.cpp
  AsyncFileWriteChecker.h
  AsyncFileWriteChecker.cpp
  Base64UrlDecode.h
  Base64UrlDecode.cpp
  Base64UrlEncode.h
  Base64UrlEncode.cpp
  ContinuousSample.h
  FailureMonitor.h
  FailureMonitor.actor.cpp
  fdbrpc.h
  FlowProcess.actor.h
  FlowTransport.h
  FlowTransport.actor.cpp
  genericactors.actor.h
  genericactors.actor.cpp
  HealthMonitor.h
  HealthMonitor.actor.cpp
  IPAllowList.h
  IPAllowList.cpp
  linux_kaio.h
  LoadBalance.h
  LoadBalance.actor.h
  LoadBalance.actor.cpp
  Locality.h
  Locality.cpp
  MultiInterface.h
  Net2FileSystem.h
  Net2FileSystem.cpp
  networksender.actor.h
  PerfMetric.cpp
  PerfMetric.h
  QueueModel.h
  QueueModel.cpp
  RangeMap.h
  Replication.h
  ReplicationPolicy.h
  ReplicationPolicy.cpp
  ReplicationTypes.h
  ReplicationTypes.cpp
  ReplicationUtils.h
  ReplicationUtils.cpp
  SimExternalConnection.actor.cpp
  SimExternalConnection.h
  Smoother.h
  Stats.actor.cpp
  Stats.h
  simulator.h
  sim2.actor.cpp
  sim_validation.h
  sim_validation.cpp
  TenantInfo.h
  TimedRequest.h
  TokenSign.h
  TokenSign.cpp
  TraceFileIO.h
  TraceFileIO.cpp
  TSSComparison.h
  WellKnownEndpoints.h)

set(COMPILE_EIO OFF)

if(NOT WIN32)
  if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    find_library(EIO eio)
    if(NOT EIO)
      set(COMPILE_EIO ON)
    endif()
  else()
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
target_link_libraries(fdbrpc PUBLIC flow PRIVATE rapidjson)

add_flow_target(STATIC_LIBRARY NAME fdbrpc_sampling
  SRCS ${FDBRPC_SRCS}
  DISABLE_ACTOR_DIAGNOSTICS ${FDBRPC_SRCS_DISABLE_ACTOR_DIAGNOSTICS})
target_include_directories(fdbrpc_sampling PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)
target_link_libraries(fdbrpc_sampling PUBLIC flow_sampling PRIVATE rapidjson)
target_compile_definitions(fdbrpc_sampling PRIVATE -DENABLE_SAMPLING)
if(WIN32)
  add_dependencies(fdbrpc_sampling_actors fdbrpc_actors)
endif()

if(COMPILE_EIO)
  add_library(eio STATIC libeio/eio.c)
  if(USE_VALGRIND)
    target_link_libraries(eio PUBLIC Valgrind)
  endif()
  target_compile_definitions(eio PRIVATE USE_UCONTEXT)
  target_compile_options(eio BEFORE PRIVATE -w) # disable warnings for eio
  target_link_libraries(fdbrpc PRIVATE eio)
  target_link_libraries(fdbrpc_sampling PRIVATE eio)
endif()

if(${COROUTINE_IMPL} STREQUAL libcoro)
  set(CORO_SRCS libcoroutine/Common.c libcoroutine/Coro.c)
  if(APPLE)
    list(APPEND CORO_SRCS libcoroutine/asm.S)
  endif()
  if(NOT WIN32)
    list(APPEND CORO_SRCS libcoroutine/context.c)
  endif()
  add_library(coro STATIC ${CORO_SRCS})
  target_include_directories(coro PRIVATE ${CMAKE_BINARY_DIR}/flow/include)
  target_include_directories(coro PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/libcoroutine)
  # we don't want to link against flow, but we need tob make sure it is built first
  # since we rely on files being copied because we include Platform.h
  add_dependencies(coro flow)
  if(WIN32)
    target_compile_definitions(coro PRIVATE USE_FIBERS)
  else()
    target_compile_definitions(coro PRIVATE USE_UCONTEXT)
    target_compile_options(coro BEFORE PRIVATE -w) # disable warnings for third party
  endif()
  if(USE_VALGRIND)
    target_link_libraries(coro PUBLIC Valgrind)
  endif()
  target_link_libraries(fdbrpc PUBLIC coro)
  target_link_libraries(fdbrpc_sampling PUBLIC coro)
endif()

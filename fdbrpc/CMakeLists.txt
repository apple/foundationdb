fdb_find_sources(FDBRPC_SRCS)

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
add_flow_target(STATIC_LIBRARY NAME fdbrpc_sampling
  SRCS ${FDBRPC_SRCS}
  DISABLE_ACTOR_DIAGNOSTICS ${FDBRPC_SRCS_DISABLE_ACTOR_DIAGNOSTICS})
add_flow_target(STATIC_LIBRARY NAME tokensign
  SRCS TokenSign/TokenSignStdTypes.cpp
  DISABLE_ACTOR_DIAGNOSTICS ${FDBRPC_SRCS_DISABLE_ACTOR_DIAGNOSTICS})
target_link_libraries(tokensign PRIVATE fdbrpc flow)

add_flow_target(LINK_TEST NAME fdbrpclinktest SRCS LinkTest.cpp)
target_link_libraries(fdbrpclinktest PRIVATE fdbrpc rapidjson)
target_include_directories(fdbrpclinktest PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)

if(COMPILE_EIO)
  add_library(eio STATIC libeio/eio.c)
  if(USE_VALGRIND)
    target_link_libraries(eio PUBLIC Valgrind)
  endif()
  target_compile_definitions(eio PRIVATE USE_UCONTEXT)
  target_compile_options(eio BEFORE PRIVATE -w) # disable warnings for eio
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
  target_link_libraries(coro PRIVATE flow)
  target_include_directories(coro PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/libcoroutine)
  if(WIN32)
    target_compile_definitions(coro PRIVATE USE_FIBERS)
  else()
    target_compile_definitions(coro PRIVATE USE_UCONTEXT)
    target_compile_options(coro BEFORE PRIVATE -w) # disable warnings for third party
  endif()
  if(USE_VALGRIND)
    target_link_libraries(coro PUBLIC Valgrind)
  endif()
endif()

target_include_directories(fdbrpc PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include" PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)
target_link_libraries(fdbrpc PUBLIC flow libb64 md5 PRIVATE rapidjson)

target_include_directories(fdbrpc_sampling PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include" PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/libeio)
target_link_libraries(fdbrpc_sampling PUBLIC flow_sampling libb64 md5 PRIVATE rapidjson)

if(${COROUTINE_IMPL} STREQUAL libcoro)
  target_link_libraries(fdbrpc PUBLIC coro)
  target_link_libraries(fdbrpc_sampling PUBLIC coro)
endif()

if(COMPILE_EIO)
  target_link_libraries(fdbrpc PRIVATE eio)
  target_link_libraries(fdbrpc_sampling PRIVATE eio)
endif()

target_compile_definitions(fdbrpc_sampling PRIVATE -DENABLE_SAMPLING)
if(WIN32)
  add_dependencies(fdbrpc_sampling_actors fdbrpc_actors)
endif()

add_subdirectory(tests)

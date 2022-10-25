find_package(Threads REQUIRED)

fdb_find_sources(FLOW_SRCS)

# Remove files with `main` defined so we can create a link test executable.
list(REMOVE_ITEM FLOW_SRCS TLSTest.cpp)
list(REMOVE_ITEM FLOW_SRCS MkCertCli.cpp)

if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64")
  list(APPEND FLOW_SRCS aarch64/memcmp.S aarch64/memcpy.S)
endif()

make_directory(${CMAKE_CURRENT_BINARY_DIR}/include/flow)
set(FDB_API_VERSION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/ApiVersions.cmake" CACHE STRING "Api version cmake file." FORCE)
include(${FDB_API_VERSION_FILE})
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/ApiVersion.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/include/flow/ApiVersion.h)
set(FDB_PROTOCOL_VERSION_FILE "${CMAKE_CURRENT_SOURCE_DIR}/ProtocolVersions.cmake" CACHE STRING "Protocol version cmake file." FORCE)
include(${FDB_PROTOCOL_VERSION_FILE})
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/ProtocolVersion.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/include/flow/ProtocolVersion.h)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/SourceVersion.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/include/flow/SourceVersion.h)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/config.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/include/flow/config.h)

add_flow_target(STATIC_LIBRARY NAME flow SRCS ${FLOW_SRCS})
add_flow_target(STATIC_LIBRARY NAME flow_sampling SRCS ${FLOW_SRCS})

# When creating a static or shared library, undefined symbols will be ignored.
# Since we want to ensure no symbols from other modules are used, create an
# executable so the linker will throw errors if it can't find the declaration
# of a symbol.
add_flow_target(LINK_TEST NAME flowlinktest SRCS LinkTest.cpp)
target_link_libraries(flowlinktest PRIVATE flow stacktrace)

find_package(ZLIB)
if(ZLIB_FOUND)
  target_compile_definitions(flow PUBLIC ZLIB_LIB_SUPPORTED)
  target_link_libraries(flow PUBLIC ZLIB::ZLIB)
else()
  message(STATUS "ZLIB package not found")
endif()

foreach(ft flow flow_sampling flowlinktest)
  target_include_directories(${ft} PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")

  target_link_libraries(${ft} PRIVATE stacktrace)
  target_link_libraries(${ft} PUBLIC fmt::fmt SimpleOpt crc32)
  if(UNIX AND NOT APPLE)
    target_link_libraries(${ft} PRIVATE folly_memcpy)
    target_compile_definitions(${ft} PRIVATE WITH_FOLLY_MEMCPY)
  endif()

  if (NOT APPLE AND NOT WIN32)
    set (FLOW_LIBS ${FLOW_LIBS} rt)
  elseif(WIN32)
    target_link_libraries(${ft} PUBLIC winmm.lib)
    target_link_libraries(${ft} PUBLIC psapi.lib)
  endif()

  if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
    set (FLOW_LIBS ${FLOW_LIBS} execinfo devstat)
    find_library(EIO eio)
    if(EIO)
      target_link_libraries(${ft} PUBLIC ${EIO})
    endif()
  endif()
  target_link_libraries(${ft} PRIVATE ${FLOW_LIBS})

  if(USE_VALGRIND)
    target_link_libraries(${ft} PUBLIC Valgrind)
  endif()
  target_link_libraries(${ft} PUBLIC OpenSSL::SSL)
  if(USE_WOLFSSL)
    target_include_directories(${ft} SYSTEM BEFORE PUBLIC ${WOLFSSL_INCLUDE_DIR}/wolfssl)
  endif()
  target_link_libraries(${ft} PUBLIC Threads::Threads ${CMAKE_DL_LIBS})
  target_link_libraries(${ft} PUBLIC boost_target)
  if(USE_VALGRIND)
    target_link_libraries(${ft} PUBLIC Valgrind)
  endif()

  if(APPLE)
    find_library(IO_KIT IOKit)
    find_library(CORE_FOUNDATION CoreFoundation)
    target_link_libraries(${ft} PRIVATE ${IO_KIT} ${CORE_FOUNDATION})
  endif()
endforeach()

target_compile_definitions(flow_sampling PRIVATE -DENABLE_SAMPLING)
if(WIN32)
  add_dependencies(flow_sampling_actors flow_actors)
endif()

add_executable(mkcert MkCertCli.cpp)
target_link_libraries(mkcert PUBLIC flow)

add_executable(mtls_unittest TLSTest.cpp)
target_link_libraries(mtls_unittest PUBLIC flow)
add_test(NAME mutual_tls_unittest
         COMMAND $<TARGET_FILE:mtls_unittest>)


include(GenerateModulemap)

set(FLOW_BINARY_DIR "${CMAKE_BINARY_DIR}/flow")
generate_modulemap("${CMAKE_BINARY_DIR}/flow/include" "Flow" flow
  OMIT
  sse2neon.h
  ppc-asm.h
)

add_library(flow_swift STATIC
    future_support.swift
    clock_support.swift
    SwiftBridging.swift
    SwiftFileB.swift)

target_include_directories(flow_swift PUBLIC
    "${CMAKE_BINARY_DIR}/flow/include"
    "${CMAKE_BINARY_DIR}/flow/"
    "${CMAKE_BINARY_DIR}/fdbclient/include"
    "${CMAKE_BINARY_DIR}/fdbserver/include"
    "${CMAKE_SOURCE_DIR}/fdbrpc/include"
    "${CMAKE_BINARY_DIR}/fdbrpc/include"
    "${CMAKE_SOURCE_DIR}/contrib/fmt-8.1.1/include"
    "${CMAKE_SOURCE_DIR}/contrib/md5/include"
    "${CMAKE_SOURCE_DIR}/contrib/libb64/include"
    "${CMAKE_SOURCE_DIR}/contrib/sqlite"
    "${CMAKE_BINARY_DIR}/boost_install/include"
    "${CMAKE_BINARY_DIR}/msgpackProject-prefix/src/msgpackProject/include"
)

# TODO: We need a platform-agnostic way to find the C++ stdlib searchpaths
target_include_directories(flow_swift PRIVATE
    "/opt/rh/devtoolset-8/root/usr/include/c++/8/x86_64-redhat-linux/"
    "/opt/rh/devtoolset-8/root/usr/lib/gcc/x86_64-redhat-linux/8/../../../../include/c++/8"
)

# TODO: the TBD validation skip is because of swift_job_run_generic, though it seems weird why we need to do that?
target_compile_options(flow_swift PRIVATE "$<$<COMPILE_LANGUAGE:Swift>:SHELL:-Xcc -std=c++17 -enable-experimental-cxx-interop -Xfrontend -validate-tbd-against-ir=none -Xcc -DNO_INTELLISENSE -Xcc -ivfsoverlay${CMAKE_BINARY_DIR}/flow/include/headeroverlay.yaml>")

# Ensure that C++ code in fdbserver can import Swift using a compatibility header.
include(SwiftToCXXInterop)
generate_cxx_compat_header(flow_swift "Flow")

add_dependencies(flow_swift flow_actors)
add_dependencies(flow_swift boost_targetProject)
# TODO(swift): rdar://99107402 - this will only work once CMake 3.25 is released:
# target_link_libraries(flow PRIVATE flow_swift)
add_dependencies(flow flow_swift)

find_package(Threads REQUIRED)

option(FLOW_USE_ZSTD "Enable zstd compression in flow" OFF)

fdb_find_sources(FLOW_SRCS)

if (FLOW_USE_ZSTD)
  # NOTE: To enable boost::iostreams with zstd library support, manually add
  # zstd.cpp to source files is required. Ref:
  #   https://www.boost.org/doc/libs/1_79_0/libs/iostreams/doc/installation.html
  list(APPEND FLOW_SRCS ../contrib/boost_zstd/zstd.cpp)
endif()

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

if (FLOW_USE_ZSTD)
  include(CompileZstd)
  compile_zstd()

  target_link_libraries(flow PRIVATE libzstd_static)
  target_compile_definitions(flow PUBLIC ZSTD_LIB_SUPPORTED)
endif()

# When creating a static or shared library, undefined symbols will be ignored.
# Since we want to ensure no symbols from other modules are used, create an
# executable so the linker will throw errors if it can't find the declaration
# of a symbol.
add_flow_target(LINK_TEST NAME flowlinktest SRCS LinkTest.cpp)
target_link_libraries(flowlinktest PRIVATE flow stacktrace)

set(IS_ARM_MAC NO)
if(APPLE AND CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
    set(IS_ARM_MAC YES)
endif()

foreach(ft flow flow_sampling flowlinktest)
    target_include_directories(${ft} PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include" "${CMAKE_CURRENT_BINARY_DIR}/include")
    if (FLOW_USE_ZSTD)
        target_include_directories(${ft} PRIVATE SYSTEM ${ZSTD_LIB_INCLUDE_DIR})
    endif()

    target_link_libraries(${ft} PRIVATE stacktrace)
    target_link_libraries(${ft} PUBLIC fmt::fmt SimpleOpt crc32)
    target_link_libraries(${ft} PRIVATE jemalloc)
    if(UNIX AND NOT APPLE)
        target_link_libraries(${ft} PRIVATE folly_memcpy)
        target_compile_definitions(${ft} PRIVATE WITH_FOLLY_MEMCPY)
    endif()

    if(NOT APPLE AND NOT WIN32)
        set(FLOW_LIBS ${FLOW_LIBS} rt)
    elseif(WIN32)
        target_link_libraries(${ft} PUBLIC winmm.lib)
        target_link_libraries(${ft} PUBLIC psapi.lib)
    endif()

    if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
        set(FLOW_LIBS ${FLOW_LIBS} execinfo devstat)
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

if(OPEN_FOR_IDE)
    add_library(mkcert OBJECT MkCertCli.cpp)
else()
    add_executable(mkcert MkCertCli.cpp)
endif()
target_link_libraries(mkcert PUBLIC flow)

#
# CMakeLists.txt
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
cmake_minimum_required(VERSION 3.24.2)

# silence deprecation warnings in newer versions of cmake
if(POLICY CMP0135)
  cmake_policy(SET CMP0135 NEW)
endif()

project(foundationdb
  VERSION 8.0.0
  DESCRIPTION "FoundationDB is a scalable, fault-tolerant, ordered key-value store with full ACID transactions."
  HOMEPAGE_URL "http://www.foundationdb.org/"
  LANGUAGES C CXX ASM Swift)

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")

message(STATUS "${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}")
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are forbidden")
endif()

set(OPEN_FOR_IDE
    OFF
    CACHE BOOL "Open this in an IDE (won't compile/link)")
set(AUTO_DISCOVER_UNIT_TESTS
    OFF
    CACHE BOOL "Automatically discover unit-tests")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  if(OPEN_FOR_IDE)
    message(STATUS "Defaulting build type to 'Debug' for OPEN_FOR_IDE")
    set(CMAKE_BUILD_TYPE
        Debug
        CACHE STRING "Choose the type of build" FORCE)
  else()
    message(STATUS "Setting build type to 'Release' as none was specified")
    set(CMAKE_BUILD_TYPE
        Release
        CACHE STRING "Choose the type of build" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
                                                 "MinSizeRel" "RelWithDebInfo")
  endif()
endif()

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

option(USE_SCCACHE "Use sccache if found" ON)
if(USE_SCCACHE)
  find_package(sccache)
endif()

option(WITH_ACAC "Enable actor stack recording" OFF)
if(WITH_ACAC)
  message(STATUS "Build FoundationDB with AcAC support")
  if(FDB_RELEASE OR FDB_RELEASE_CANDIDATE)
    message(
      FATAL_ERROR
        "ACAC will cause severe slowdown of the system and SHOULD not be enabled in Release."
    )
  endif()
  add_compile_definitions(WITH_ACAC)
endif()

###############################################################################
# Packages used for bindings
###############################################################################

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

###############################################################################
# Compiler configuration
###############################################################################

include(ConfigureCompiler)

###############################################################################
# Components configuration
###############################################################################

include(FDBComponents)

###############################################################################
# Get repository information
###############################################################################

add_custom_target(branch_file ALL DEPENDS ${CURR_BRANCH_FILE})
execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  OUTPUT_VARIABLE CURRENT_GIT_VERSION_WNL
  OUTPUT_STRIP_TRAILING_WHITESPACE)
string(STRIP "${CURRENT_GIT_VERSION_WNL}" CURRENT_GIT_VERSION)
message(STATUS "Current git version ${CURRENT_GIT_VERSION}")

###############################################################################
# Version information
###############################################################################
if(FDB_RELEASE_CANDIDATE)
  set(FDB_RELEASE_CANDIDATE_VERSION
      1
      CACHE STRING "release candidate version")
  set(FDB_VERSION ${PROJECT_VERSION}-rc${FDB_RELEASE_CANDIDATE_VERSION})
else()
  set(FDB_VERSION ${PROJECT_VERSION})
endif()
if(NOT FDB_RELEASE)
  string(TIMESTAMP FDB_BUILD_TIMESTMAP %Y%m%d%H%M%S)
  # Adding support to pass custom fdb_build timestamp, required to achieve
  # uniform naming across different builds
  set(FDB_BUILDTIME
      "${FDB_BUILD_TIMESTMAP}"
      CACHE STRING "A timestamp for packages")
  set(FDB_BUILDTIME_STRING ".${FDB_BUILDTIME}")
  set(PRERELEASE_TAG "prerelease")
endif()
set(FDB_VERSION_PLAIN ${FDB_VERSION})
string(REPLACE "." ";" FDB_VERSION_LIST ${FDB_VERSION_PLAIN})
list(GET FDB_VERSION_LIST 0 FDB_MAJOR)
list(GET FDB_VERSION_LIST 1 FDB_MINOR)
list(GET FDB_VERSION_LIST 2 FDB_PATCH)
set(FDB_PACKAGE_NAME "${FDB_MAJOR}.${FDB_MINOR}")
configure_file(${CMAKE_SOURCE_DIR}/versions.target.cmake
               ${CMAKE_CURRENT_BINARY_DIR}/versions.target)
file(WRITE ${CMAKE_BINARY_DIR}/version.txt ${FDB_VERSION})

set(FDB_CURRENT_VERSION ${PROJECT_VERSION})
set(FDB_FUTURE_VERSION "8.1.0")
set(FDB_PREV_RELEASE_VERSION "7.4.5")
set(FDB_PREV2_RELEASE_VERSION "7.3.59")

set(MULTIREGION_TEST TRUE)
option(NO_MULTIREGION_TEST "Disable multiregion simulation tests" OFF)

if(NO_MULTIREGION_TEST)
  set(MULTIREGION_TEST FALSE)
  add_definitions(-DNO_MULTIREGION_TEST)
  message(STATUS "NO_MULTIREGION_TEST is ON")
endif()

set(RESTART_TEST TRUE)
option(NO_RESTART_TEST "Disable restart simulation tests" OFF)
if(NO_RESTART_TEST)
  set(RESTART_TEST FALSE)
  add_definitions(-DNO_RESTART_TEST)
  message(STATUS "NO_RESTART_TEST is ON")
endif()

###############################################################################
# Flow
###############################################################################

include(utils)

# First thing we need is the actor compiler
option(
  USE_CSHARP
  "Prefer C# build tools (actor compiler, coverage tool, vexillographer) when a toolchain is available"
  ON)

if(NOT DEFINED FDB_USE_CSHARP_TOOLS)
  set(FDB_USE_CSHARP_TOOLS ${USE_CSHARP})
else()
  set(USE_CSHARP ${FDB_USE_CSHARP_TOOLS})
endif()

set(CSHARP_TOOLCHAIN_FOUND FALSE)
set(COVERAGETOOL_AVAILABLE FALSE)
if(FDB_USE_CSHARP_TOOLS)
  if(WIN32)
    find_program(dotnet_EXECUTABLE NAMES dotnet dotnet.exe)
    if(dotnet_EXECUTABLE)
      set(CSHARP_TOOLCHAIN_FOUND TRUE)
    endif()
  else()
    find_package(dotnet)
    if(dotnet_FOUND)
      set(CSHARP_TOOLCHAIN_FOUND TRUE)
    endif()
  endif()

  if(NOT CSHARP_TOOLCHAIN_FOUND)
    find_package(mono)
    if(mono_FOUND)
      set(CSHARP_TOOLCHAIN_FOUND TRUE)
      set(CSHARP_USE_MONO TRUE)
    endif()
  endif()
endif()

if(FDB_USE_CSHARP_TOOLS AND NOT CSHARP_TOOLCHAIN_FOUND)
  message(
    FATAL_ERROR
    "FDB_USE_CSHARP_TOOLS=ON but no C# toolchain was found. Install .NET (dotnet) or Mono, or set FDB_USE_CSHARP_TOOLS=OFF.")
endif()

include(CompileActorCompiler)

if(FDB_USE_CSHARP_TOOLS AND CSHARP_TOOLCHAIN_FOUND)
  include(CompileCoverageTool)
  set(COVERAGETOOL_AVAILABLE TRUE)
else()
  message(STATUS "C# tooling disabled or not found; skipping coverage tool build")
endif()
# Vexilographer generation is configured inside fdbclient
include(CompileVexillographer)

# with the actor compiler, we can now make the flow commands available
include(FlowCommands)

###############################################################################
# Generate config file
###############################################################################

string(RANDOM LENGTH 8 description1)
string(RANDOM LENGTH 8 description2)
set(CLUSTER_DESCRIPTION1
    ${description1}
    CACHE STRING "Cluster description")
set(CLUSTER_DESCRIPTION2
    ${description2}
    CACHE STRING "Cluster description")

configure_file(fdb.cluster.cmake ${CMAKE_CURRENT_BINARY_DIR}/fdb.cluster)

###############################################################################
# testing
###############################################################################
include(CTest)
enable_testing()

###############################################################################
# Directory structure
###############################################################################

include(cmake/InstallLayout.cmake)

###############################################################################
# Random seed
###############################################################################

string(
  RANDOM
  LENGTH 8
  ALPHABET "0123456789abcdef" SEED_)
set(SEED
    "0x${SEED_}"
    CACHE STRING "Random seed for testing")

###############################################################################
# components
###############################################################################

if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  include_directories(/usr/local/include)
endif()

include(CompileBoost)
include(GetFmt)
include(GetMsgpack)
add_subdirectory(contrib)
add_subdirectory(flow)
add_subdirectory(fdbrpc)
add_subdirectory(fdbctl)
add_subdirectory(fdbclient)
add_subdirectory(fdbserver)
add_subdirectory(fdbcli)
if(NOT WIN32)
  if(NOT FOUNDATIONDB_CROSS_COMPILING) # FIXME(swift): make this work when
                                       # x-compiling.
    add_subdirectory(fdbmonitor)
  endif()
else()
  add_subdirectory(fdbservice)
endif()
add_subdirectory(fdbbackup)
add_subdirectory(tests)
if(NOT FOUNDATIONDB_CROSS_COMPILING) # FIXME(swift): make this work when
                                     # x-compiling.
  add_subdirectory(flowbench EXCLUDE_FROM_ALL)
endif()
if(WITH_PYTHON AND WITH_C_BINDING)
  if(NOT FOUNDATIONDB_CROSS_COMPILING) # FIXME(swift): make this work when
                                       # x-compiling.
    add_subdirectory(bindings)
  endif()
endif()
if(WITH_DOCUMENTATION)
  add_subdirectory(documentation)
  if(BUILD_JAVA_BINDING AND TARGET CopyJavadoc)
    add_dependencies(html CopyJavadoc)
  endif()
endif()

if(WIN32)
  add_subdirectory(packaging/msi)
else()
  include(CPack)
endif()

if(CMAKE_SYSTEM_NAME STREQUAL "FreeBSD")
  add_link_options(-lexecinfo)
endif()

###############################################################################
# process compile commands for IDE
###############################################################################
if(CMAKE_EXPORT_COMPILE_COMMANDS AND WITH_PYTHON)
  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
    COMMAND
      $<TARGET_FILE:Python3::Interpreter>
      ${CMAKE_CURRENT_SOURCE_DIR}/contrib/gen_compile_db.py ARGS -b
      ${CMAKE_CURRENT_BINARY_DIR} -s ${CMAKE_CURRENT_SOURCE_DIR} -o
      ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json -ninjatool
      ${CMAKE_MAKE_PROGRAM} ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/contrib/gen_compile_db.py
            ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
    COMMENT "Build compile commands for IDE")
  add_custom_target(
    processed_compile_commands ALL
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
            ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json)

  # A prebuild target ensures that all actors, Swift-generated headers, and
  # Swift modules are built.
  if(WITH_SWIFT)
    add_custom_target(prebuild_for_ide ALL DEPENDS fdbserver_swift
                                                   processed_compile_commands)
  endif()
endif()

###############################################################################
# Inform user which components we are going to build
###############################################################################

print_components()

message(STATUS "CPACK_COMPONENTS_ALL ${CPACK_COMPONENTS_ALL}")

#
# CMakeLists.txt
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
cmake_minimum_required(VERSION 3.12)
project(fdb
  VERSION 6.1.0
  DESCRIPTION "FoundationDB is a scalable, fault-tolerant, ordered key-value store with full ACID transactions."
  HOMEPAGE_URL "http://www.foundationdb.org/"
  LANGUAGES C CXX ASM Java)

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH};${PROJECT_SOURCE_DIR}/cmake")
message (STATUS "${PROJECT_SOURCE_DIR} ${PROJECT_BINARY_DIR}")
if("${PROJECT_SOURCE_DIR}" STREQUAL "${PROJECT_BINARY_DIR}")
  message(FATAL_ERROR "In-source builds are forbidden, unsupported, and stupid!!")
endif()

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  message(STATUS "Setting build type to 'Release' as none was specified")
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build" FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release"
    "MinSizeRel" "RelWithDebInfo")
endif()

set(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

################################################################################
# Packages used for bindings
################################################################################

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake/")

find_package(PythonInterp 3.4 REQUIRED)
set(Python_ADDITIONAL_VERSIONS 3.4 3.5 3.5)
find_package(PythonLibs 3.4 REQUIRED)


################################################################################
# Compiler configuration
################################################################################

include(ConfigureCompiler)

################################################################################
# Get repository information
################################################################################

add_custom_target(branch_file ALL DEPENDS ${CURR_BRANCH_FILE})
execute_process(
  COMMAND git rev-parse HEAD
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
  OUTPUT_VARIABLE CURRENT_GIT_VERSION_WNL)
string(STRIP "${CURRENT_GIT_VERSION_WNL}" CURRENT_GIT_VERSION)
message(STATUS "Current git version ${CURRENT_GIT_VERSION}")

################################################################################
# Version information
################################################################################

set(USE_VERSIONS_TARGET OFF CACHE BOOL "Use the deprecated versions.target file")
if(USE_VERSIONS_TARGET)
  add_custom_target(version_file ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/versions.target)
  execute_process(
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/get_version.sh ${CMAKE_CURRENT_SOURCE_DIR}/versions.target
    OUTPUT_VARIABLE FDB_VERSION_WNL)
  execute_process(
    COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/get_package_name.sh ${CMAKE_CURRENT_SOURCE_DIR}/versions.target
    OUTPUT_VARIABLE FDB_PACKAGE_NAME_WNL)
  string(STRIP "${FDB_VERSION_WNL}" FDB_VERSION)
  string(STRIP "${FDB_PACKAGE_NAME_WNL}" FDB_PACKAGE_NAME)
  set(FDB_VERSION_PLAIN ${FDB_VERSION})
  if(NOT FDB_RELEASE)
    set(FDB_VERSION "${FDB_VERSION}-PRERELEASE")
  endif()
else()
  set(FDB_PACKAGE_NAME "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}")
  set(FDB_VERSION ${PROJECT_VERSION})
  set(FDB_VERSION_PLAIN ${FDB_VERSION})
endif()

message(STATUS "FDB version is ${FDB_VERSION}")
message(STATUS "FDB package name is ${FDB_PACKAGE_NAME}")
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/versions.h.cmake ${CMAKE_CURRENT_BINARY_DIR}/fdbclient/versions.h)


################################################################################
# Flow
################################################################################

# First thing we need is the actor compiler - and to compile and run the
# actor compiler, we need mono
include(CompileActorCompiler)

# with the actor compiler, we can now make the flow commands available
include(FlowCommands)

################################################################################
# Vexilographer
################################################################################

include(CompileVexillographer)

# This macro can be used to install symlinks, which turns out to be
# non-trivial due to CMake version differences and limitations on how
# files can be installed when building binary packages.
#
# The rule for binary packaging is that files (including symlinks) must
# be installed with the standard CMake install() macro.
#
# The rule for non-binary packaging is that CMake 2.6 cannot install()
# symlinks, but can create the symlink at install-time via scripting.
# Though, we assume that CMake 2.6 isn't going to be used to generate
# packages because versions later than 2.8.3 are superior for that purpose.
#
#   _filepath: the absolute path to the file to symlink
#   _sympath: absolute path of the installed symlink

macro(InstallSymlink _filepath _sympath)
    get_filename_component(_symname ${_sympath} NAME)
    get_filename_component(_installdir ${_sympath} PATH)

    if (BINARY_PACKAGING_MODE)
        execute_process(COMMAND "${CMAKE_COMMAND}" -E create_symlink
                        ${_filepath}
                        ${CMAKE_CURRENT_BINARY_DIR}/${_symname})
        install(FILES ${CMAKE_CURRENT_BINARY_DIR}/${_symname}
          DESTINATION ${_installdir}
		  COMPONENT clients)
    else ()
        # scripting the symlink installation at install time should work
        # for CMake 2.6.x and 2.8.x
        install(CODE "
            if (\"\$ENV{DESTDIR}\" STREQUAL \"\")
                execute_process(COMMAND \"${CMAKE_COMMAND}\" -E create_symlink
                                ${_filepath}
                                ${_installdir}/${_symname})
            else ()
                execute_process(COMMAND \"${CMAKE_COMMAND}\" -E create_symlink
                                ${_filepath}
                                \$ENV{DESTDIR}/${_installdir}/${_symname})
            endif ()
        "
		  COMPONENT clients)
    endif ()
endmacro(InstallSymlink)

################################################################################
# Generate config file
################################################################################

string(RANDOM LENGTH 8 description1)
string(RANDOM LENGTH 8 description2)
set(CLUSTER_DESCRIPTION1 ${description1} CACHE STRING "Cluster description")
set(CLUSTER_DESCRIPTION2 ${description2} CACHE STRING "Cluster description")

configure_file(fdb.cluster.cmake ${CMAKE_CURRENT_BINARY_DIR}/fdb.cluster)


################################################################################
# testing
################################################################################
enable_testing()

################################################################################
# Directory structure
################################################################################

include(cmake/InstallLayout.cmake)

################################################################################
# Random seed
################################################################################

string(RANDOM LENGTH 8 ALPHABET "0123456789abcdef" SEED_)
set(SEED "0x${SEED_}" CACHE STRING "Random seed for testing")

################################################################################
# components
################################################################################

include(CompileBoost)
add_subdirectory(flow)
add_subdirectory(fdbrpc)
add_subdirectory(fdbclient)
add_subdirectory(fdbserver)
add_subdirectory(fdbcli)
add_subdirectory(fdbmonitor)
add_subdirectory(bindings)
add_subdirectory(fdbbackup)

include(CPack)

################################################################################
# process compile commands for IDE
################################################################################

if (CMAKE_EXPORT_COMPILE_COMMANDS)
  add_custom_command(
	OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json
	COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/build/gen_compile_db.py
	ARGS -b ${CMAKE_CURRENT_BINARY_DIR} -s ${CMAKE_CURRENT_SOURCE_DIR} -o ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
	DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/build/gen_compile_db.py ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json
	COMMENT "Build compile commands for IDE"
	)
  add_custom_target(procossed_compile_commands ALL DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/compile_commands.json ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json)
endif()

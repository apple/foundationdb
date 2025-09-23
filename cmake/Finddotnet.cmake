# Distributed under the OSI-approved Apache 2.0. See the LICENSE file in
# FoundationDB source code

#[=======================================================================[.rst:
Finddotnet
-------

Find dotnet, the free, open-source, cross-platform framework for building modern apps and powerful cloud services

dotnet_ROOT variable can be used for HINTS for different version of dotnet.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``dotnet_FOUND``
  If false, do not try to use dotnet.
``dotnet_VERSION``
  the version of the dotnet found
``dotnet_EXECUTABLE``
  path to `dotnet` executable
#]=======================================================================]

include(FindPackageHandleStandardArgs)
include(FindPackageMessage)

if(NOT dotnet_ROOT)
  set(dotnet_ROOT $ENV{dotnet_ROOT})
endif()

find_program(
  dotnet_EXECUTABLE NAME dotnet dotnet.exe
  HINTS ${dotnet_ROOT}
  DOC ".NET framework executable")

if(dotnet_EXECUTABLE)
  execute_process(
    COMMAND ${dotnet_EXECUTABLE}
    OUTPUT_VARIABLE dotnet_VERSION
    ERROR_QUIET)
endif()

find_package_handle_standard_args(
  dotnet
  FOUND_VAR dotnet_FOUND
  REQUIRED_VARS dotnet_EXECUTABLE dotnet_VERSION)

mark_as_advanced(dotnet_FOUND dotnet_VERSION dotnet_EXECUTABLE)

function(dotnet_build project_file_path)
  if(NOT dotnet_FOUND)
    message(
      FATAL_ERROR "-- dotnet command is not found, cannot build .NET projects")
  endif()

  set(options)
  set(oneValueArg CONFIGURATION)
  set(multiValueArgs SOURCE)
  cmake_parse_arguments(PARSE_ARGV 1 ARG "${options}" "${oneValueArgs}"
                        "${multiValueArgs}")

  if(NOT ARG_CONFIGURATION)
    set(configuration "Release")
  else()
    set(configuration "${ARG_CONFIGURATION}")
  endif()

  cmake_path(GET project_file_path STEM project)
  cmake_path(GET project_file_path PARENT_PATH project_root_directory)
  cmake_path(APPEND project_root_directory "bin" OUTPUT_VARIABLE
             project_binary_directory)
  cmake_path(APPEND project_binary_directory "${project}" OUTPUT_VARIABLE
             project_binary_path)
  message(
    STATUS "Building project ${project} using dotnet, in ${configuration} mode")

  add_custom_command(
    OUTPUT ${project_binary_path}
    COMMAND
      ${dotnet_EXECUTABLE} ARGS build ${project_file_path} --configuration
      "${configuration}" --output "${project_binary_directory}" --self-contained
    DEPENDS ${ARG_SOURCE}
    WORKING_DIRECTORY "${project_root_path}"
    COMMENT "Build ${project} using .NET framework")
  add_custom_target(${project} DEPENDS ${project_binary_path})

  set(${project}_EXECUTABLE_PATH
      "${project_binary_path}"
      PARENT_SCOPE)
endfunction()

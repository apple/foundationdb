#
# A CMake Module for finding Mono.
#
# The following variables are set: CSHARP_MONO_FOUND
# CSHARP_MONO_COMPILER_${version} eg. "CSHARP_MONO_COMPILER_2.10.2"
# CSHARP_MONO_INTERPRETER_${version} eg. "CSHARP_MONO_INTERPRETER_2.10.2"
# CSHARP_MONO_VERSION eg. "2.10.2" CSHARP_MONO_VERSIONS eg. "2.10.2, 2.6.7"
#
# Additional references can be found here: http://www.mono-project.com/Main_Page
# http://www.mono-project.com/CSharp_Compiler
# http://mono-project.com/FAQ:_Technical (How can I tell where the Mono runtime
# is installed)
#
# This file is based on the work of GDCM:
# http://gdcm.svn.sf.net/viewvc/gdcm/trunk/CMake/FindMono.cmake Copyright (c)
# 2006-2010 Mathieu Malaterre <mathieu.malaterre@gmail.com>
#
# This file is based on the work of SimpleITK under Apache-2.0 license
# https://github.com/SimpleITK/SimpleITK/blob/master/CMake/FindMono.cmake

set(csharp_mono_valid 1)
if(DEFINED CSHARP_MONO_FOUND)
  # The Mono compiler has already been found It may have been reset by the user,
  # verify it is correct
  if(NOT DEFINED CSHARP_MONO_COMPILER_${CSHARP_MONO_VERSION})
    set(csharp_mono_version_user ${CSHARP_MONO_VERSION})
    set(csharp_mono_valid 0)
    set(CSHARP_MONO_FOUND 0)
    set(CSHARP_MONO_VERSION
        "CSHARP_MONO_VERSION-NOTVALID"
        CACHE STRING
              "C# Mono compiler version, choices: ${CSHARP_MONO_VERSIONS}"
              FORCE)
    message(
      FATAL_ERROR
        "The C# Mono version '${csharp_mono_version_user}' is not valid. Please enter one of the following: ${CSHARP_MONO_VERSIONS}"
    )
  endif(NOT DEFINED CSHARP_MONO_COMPILER_${CSHARP_MONO_VERSION})
endif(DEFINED CSHARP_MONO_FOUND)

unset(CSHARP_MONO_VERSIONS CACHE) # Clear versions
if(WIN32)
  message(
    STATUS
      "FoundationDB does not support building using Mono on Windows platform.")
else() # UNIX
  # Search for Mono on non-Win32 systems
  set(chsarp_mono_names
      "mcs"
      "mcs.exe"
      "dmcs"
      "dmcs.exe"
      "smcs"
      "smcs.exe"
      "gmcs"
      "gmcs.exe")
  set(csharp_mono_paths "/usr/bin/" "/usr/local/bin/" "/usr/lib/mono/"
                        "/opt/novell/mono/bin" ${mono_ROOT})
  find_program(
    csharp_mono_compiler # variable is added to the cache, we removed it below
    NAMES ${chsarp_mono_names} NAMES_PER_DIR
    PATHS ${csharp_mono_paths})


  if(EXISTS ${csharp_mono_compiler})
    set(MCS_EXECUTABLE "${csharp_mono_version_temp}")
    mark_as_advanced(MCS_EXECUTABLE)

    # Determine version
    find_program(
      csharp_mono_interpreter # variable is added to the cache, we removed it
                              # below
      NAMES mono
      PATHS ${csharp_mono_paths})
    if(EXISTS ${csharp_mono_interpreter})
      execute_process(COMMAND ${csharp_mono_interpreter} -V
                      OUTPUT_VARIABLE csharp_mono_version_string)
      string(REGEX MATCH "([0-9]*)([.])([0-9]*)([.]*)([0-9]*)"
                   csharp_mono_version_temp ${csharp_mono_version_string})
      set(CSHARP_MONO_INTERPRETER_${csharp_mono_version_temp}
          ${csharp_mono_interpreter}
          CACHE STRING "C# Mono interpreter ${csharp_mono_version_temp}" FORCE)
      mark_as_advanced(CSHARP_MONO_INTERPRETER_${csharp_mono_version_temp})
    endif(EXISTS ${csharp_mono_interpreter})
    unset(csharp_mono_interpreter CACHE)

    # We found Mono compiler
    set(CSHARP_MONO_VERSION
        ${csharp_mono_version_temp}
        CACHE STRING "C# Mono compiler version")
    mark_as_advanced(CSHARP_MONO_VERSION)
    set(CSHARP_MONO_COMPILER_${CSHARP_MONO_VERSION}
        ${csharp_mono_compiler}
        CACHE STRING "C# Mono compiler ${CSHARP_MONO_VERSION}" FORCE)
    mark_as_advanced(CSHARP_MONO_COMPILER_${CSHARP_MONO_VERSION})
    set(CSHARP_MONO_VERSIONS
        ${CSHARP_MONO_VERSION}
        CACHE STRING "Available C# Mono compiler versions" FORCE)
    mark_as_advanced(CSHARP_MONO_VERSIONS)
    set(CSHARP_MONO_FOUND
        1
        CACHE INTERNAL "Boolean indicating if C# Mono was found")
  endif(EXISTS ${csharp_mono_compiler})

  # Remove temp variable from cache
  unset(csharp_mono_compiler CACHE)

endif(WIN32)

if(CSHARP_MONO_FOUND)
  # Report the found versions
  message(
    STATUS "Found the following C# Mono versions: ${CSHARP_MONO_VERSIONS}")
endif(CSHARP_MONO_FOUND)

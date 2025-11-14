# Distributed under the OSI-approved Apache 2.0. See the LICENSE file in
# FoundationDB source code

#[=======================================================================[.rst:
Findmono
-------

Find mono, an open source implementation of Microsoft's .NET Framework as part of the
.NET Foundation and based on the ECMA standards for C# and the Common Language Runtime. 

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``mono_FOUND``
  If false, do not try to use mono.
``CSHARP_COMPILER_EXECUTABLE``
  Mono CSharp compiler executable
``MONO_EXECUTABLE``
  mono virtual machine executable
#]=======================================================================]

if(NOT mono_ROOT)
  set(mono_ROOT $ENV{mono_ROOT})
endif()

find_program(
  MONO_EXECUTABLE
  NAME mono
  HINTS ${mono_ROOT})

# Here Microsoft Visual C# Compiler (csc) does not work
# use Turbo C# compiler (mcs)
find_program(
  CSHARP_COMPILER_EXECUTABLE
  NAMES mcs
  HINTS ${mono_ROOT})

if(CSHARP_COMPILER_EXECUTABLE AND MONO_EXECUTABLE)
  message(STATUS "Found mono C# compiler: ${CSHARP_COMPILER_EXECUTABLE}")
  message(STATUS "Found mono runtime: ${MONO_EXECUTABLE}")
  set(mono_FOUND TRUE)
else()
  message(STATUS "mono C# compiler is not found")
  set(mono_FOUND FALSE)
endif()

mark_as_advanced(mono_FOUND MONO_EXECUTABLE CSHARP_COMPILER_EXECUTABLE)

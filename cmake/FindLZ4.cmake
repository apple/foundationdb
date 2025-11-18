# Copyright (c) 1993-2015 Ken Martin, Will Schroeder, Bill Lorensen All rights
# reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither name of Ken Martin, Will Schroeder, or Bill Lorensen nor the names
#   of any contributors may be used to endorse or promote products derived from
#   this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS ``AS IS''
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE AUTHORS OR CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
# ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

if(NOT LZ4_ROOT)
  set(LZ4_ROOT $ENV{LZ4_ROOT})
endif()

find_path(
  LZ4_INCLUDE_DIR
  NAMES lz4.h
  HINTS ${LZ4_ROOT}
  DOC "lz4 include directory")
find_library(
  LZ4_LIBRARY
  NAMES lz4 liblz4
  HINTS ${LZ4_ROOT}
  DOC "lz4 library")

if(LZ4_INCLUDE_DIR)
  file(STRINGS "${LZ4_INCLUDE_DIR}/lz4.h" _lz4_version_lines
       REGEX "#define[ \t]+LZ4_VERSION_(MAJOR|MINOR|RELEASE)")
  string(REGEX REPLACE ".*LZ4_VERSION_MAJOR *\([0-9]*\).*" "\\1"
                       _lz4_version_major "${_lz4_version_lines}")
  string(REGEX REPLACE ".*LZ4_VERSION_MINOR *\([0-9]*\).*" "\\1"
                       _lz4_version_minor "${_lz4_version_lines}")
  string(REGEX REPLACE ".*LZ4_VERSION_RELEASE *\([0-9]*\).*" "\\1"
                       _lz4_version_release "${_lz4_version_lines}")
  set(LZ4_VERSION
      "${_lz4_version_major}.${_lz4_version_minor}.${_lz4_version_release}")
  unset(_lz4_version_major)
  unset(_lz4_version_minor)
  unset(_lz4_version_release)
  unset(_lz4_version_lines)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(
  LZ4
  REQUIRED_VARS LZ4_LIBRARY LZ4_INCLUDE_DIR
  VERSION_VAR LZ4_VERSION)

if(LZ4_FOUND)
  set(LZ4_INCLUDE_DIRS "${LZ4_INCLUDE_DIR}")
  set(LZ4_LIBRARIES "${LZ4_LIBRARY}")

  if(NOT TARGET LZ4::LZ4)
    add_library(LZ4::LZ4 UNKNOWN IMPORTED)
    set_target_properties(
      LZ4::LZ4 PROPERTIES IMPORTED_LOCATION "${LZ4_LIBRARY}"
                          INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")
  endif()
endif()

mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY LZ4_INCLUDE_DIRS LZ4_LIBRARIES LZ4_VERSION)
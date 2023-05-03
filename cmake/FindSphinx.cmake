# Distributed under the OSI-approved Apache 2.0. See the LICENSE file in
# FoundationDB source code

#[=======================================================================[.rst:
FindSphinx
-------

Find Sphinx, the Python documentation generator

Sphinx_ROOT variable can be used for HINTS for different version of Sphinx.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``Sphinx_EXECUTABLE``
  The executable of Sphinx
``Sphinx_FOUND``
  If false, Sphinx is not available
#]=======================================================================]

include(FindPackageHandleStandardArgs)

if(NOT Sphinx_ROOT)
  set(Sphinx_ROOT $ENV{Sphinx_ROOT})
endif()

find_program(
  Sphinx_EXECUTABLE
  NAMES sphinx-build
  HINTS ${Sphinx_ROOT}
  DOC "Sphinx-build tool")

if(Sphinx_EXECUTABLE)
  execute_process(
    COMMAND ${Sphinx_EXECUTABLE} --version
    OUTPUT_VARIABLE _Sphinx_VERSION_STRING
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  # Strip the leading "sphinx-build " e.g. sphinx-build 5.1.1
  string(SUBSTRING ${_Sphinx_VERSION_STRING} 13 -1 Sphinx_VERSION)
endif()

find_package_handle_standard_args(
  Sphinx
  FOUND_VAR Sphinx_FOUND
  REQUIRED_VARS Sphinx_EXECUTABLE Sphinx_VERSION)
mark_as_advanced(Sphinx_FOUND Sphinx_VERSION Sphinx_EXECUTABLE)

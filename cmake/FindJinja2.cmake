# Distributed under the OSI-approved Apache 2.0. See the LICENSE file in
# FoundationDB source code

#[=======================================================================[.rst:
FindJinja2
-------

Find Jinja2, the Python templating engine

Jinja2_ROOT variable can be used for HINTS for different version of Jinja2.

Result variables
^^^^^^^^^^^^^^^^

This module will set the following variables in your project:

``Jinja2_VERSION``
  The version of Jinja2
``Jinja2_FOUND``
  If false, Jinja2 is not available
#]=======================================================================]

include(FindPackageHandleStandardArgs)
find_package(Python3 COMPONENTS Interpreter REQUIRED)

if(NOT Jinja2_ROOT)
  set(Jinja2_ROOT $ENV{Jinja2_ROOT})
endif()

# Check for Jinja2 using Python
execute_process(
  COMMAND ${Python3_EXECUTABLE} -c "import jinja2; print(jinja2.__version__)"
  RESULT_VARIABLE _Jinja2_NOT_FOUND
  OUTPUT_VARIABLE _Jinja2_VERSION_STRING
  OUTPUT_STRIP_TRAILING_WHITESPACE)

if(NOT _Jinja2_NOT_FOUND)
  set(Jinja2_VERSION ${_Jinja2_VERSION_STRING})
endif()

find_package_handle_standard_args(
  Jinja2
  FOUND_VAR Jinja2_FOUND
  REQUIRED_VARS Jinja2_VERSION)

mark_as_advanced(Jinja2_FOUND Jinja2_VERSION)

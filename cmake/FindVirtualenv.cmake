find_program(_VIRTUALENV_EXE virtualenv)

# get version and test that program actually works
if(_VIRTUALENV_EXE)
  execute_process(
    COMMAND ${_VIRTUALENV_EXE} --version
    RESULT_VARIABLE ret_code
    OUTPUT_VARIABLE version_string
    ERROR_VARIABLE error_output
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(ret_code EQUAL 0 AND NOT ERROR_VARIABLE)
    # we found a working virtualenv
    set(VIRTUALENV_EXE ${_VIRTUALENV_EXE})
    set(VIRTUALENV_VERSION version_string)
  endif()
endif()

find_package_handle_standard_args(Virtualenv
  REQUIRED_VARS VIRTUALENV_EXE
  VERSION_VAR ${VIRTUALENV_VERSION})

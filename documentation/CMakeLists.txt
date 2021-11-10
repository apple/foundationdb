add_subdirectory(tutorial)
# build a virtualenv
set(sphinx_dir ${CMAKE_CURRENT_SOURCE_DIR}/sphinx)
set(venv_dir ${CMAKE_CURRENT_BINARY_DIR}/venv)
set(EXE_SUFFIX "")
if(WIN32)
  set(EXE_SUFFIX ".exe")
endif()
set(pip_command ${venv_dir}/bin/pip${EXE_SUFFIX})
set(python_command ${venv_dir}/bin/python${EXE_SUFFIX})

add_custom_command(OUTPUT ${venv_dir}/venv_setup
  COMMAND ${Python3_EXECUTABLE} -m venv venv &&
          ${CMAKE_COMMAND} -E copy ${sphinx_dir}/.pip.conf ${venv_dir}/pip.conf &&
          . ${venv_dir}/bin/activate &&
          ${pip_command} install --upgrade pip &&
          ${pip_command} install --upgrade -r ${sphinx_dir}/requirements.txt &&
          ${pip_command} install sphinx-autobuild && # somehow this is missing in requirements.txt
          ${CMAKE_COMMAND} -E touch ${venv_dir}/venv_setup
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
  COMMENT "Set up virtualenv")
add_custom_target(buildsphinx DEPENDS ${venv_dir}/venv_setup)

file(GLOB_RECURSE SRCS *.rst)

function(add_documentation_target)
  set(options)
  set(oneValueArgs GENERATOR SPHINX_COMMAND DOCTREE)
  set(multiValueArgs ADDITIONAL_ARGUMENTS)
  cmake_parse_arguments(ADT "${options}" "${oneValueArgs}" "${multiValueArgs}" "${ARGN}")
  if(NOT ADT_GENERATOR)
    message(ERROR "GENERATOR is a required argument to add_documentation_target")
  endif()
  set(target ${ADT_GENERATOR})
  set(SPHINX_COMMAND "${venv_dir}/bin/sphinx-build")
  if(ADT_SPHINX_COMMAND)
    set(SPHINX_COMMAND "${venv_dir}/bin/${ADT_SPHINX_COMMAND}")
  endif()
  set(doctree "doctree")
  if (ADT_DOCTREE)
    set(doctree "${ADT_DOCTREE}")
  endif()
  set(out_dir ${CMAKE_CURRENT_BINARY_DIR}/${target})
  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${target}_done
    COMMAND ${CMAKE_COMMAND} -E make_directory ${out_dir} &&
            ${python_command} ${SPHINX_COMMAND} -W -b ${target}
                              -d ${doctree} -c ${sphinx_dir}
                              ${sphinx_dir}/source
                              ${CMAKE_CURRENT_BINARY_DIR}/${target} &&
            ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/${target}_done
    DEPENDS ${SRCS}
    WORKING_DIRECTORY ${venv_dir})
  message(STATUS "add_custom_target(${target} DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${target}_done)")
  add_custom_target(${target} DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/${target}_done)
  add_dependencies(${target} buildsphinx)
endfunction()

message(STATUS "Add html target")
add_documentation_target(GENERATOR html)

set(DOCSERVER_PORT "-1" CACHE STRING "Port to which the documentation server should bind (negative means cmake will choose one)")

if(DOCSERVER_PORT GREATER_EQUAL 0)
  set(port ${DOCSERVER_PORT})
else()
  if(WIN32)
    if(DEFINED $ENV{USERNAME})
      set(username $ENV{USERNAME})
    else()
      set(username "dummy_user")
    endif()
  else()
    if(DEFINED $ENV{USER})
      set(username $ENV{USER})
    else()
      set(username "dummy_user")
    endif()
  endif()
  string(MD5 username_hash ${username})
  # cmake math function can only use 64 bit signed integers - so we just truncate the string
  string(SUBSTRING "${username_hash}" 0 15 username_hash_small)
  message(STATUS math(EXPR port "(0x${username_hash_small} % 8000) + 8000" OUTPUT_FORMAT DECIMAL))
  math(EXPR port "(0x${username_hash_small} % 8000) + 8000" OUTPUT_FORMAT DECIMAL)
  message(STATUS "Port is ${port}")
endif()

add_custom_target(docpreview
  COMMAND ${python_command} -m http.server ${port}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html
  USES_TERMINAL)
add_dependencies(docpreview html)

set(tar_file ${CMAKE_BINARY_DIR}/packages/${CMAKE_PROJECT_NAME}-docs-${FDB_VERSION}.tar.gz)
add_custom_command(
  OUTPUT ${tar_file}
  COMMAND ${CMAKE_COMMAND} -E tar czf ${tar_file} .
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html)
add_custom_target(package_html DEPENDS ${tar_file})
add_dependencies(package_html html)
add_dependencies(packages package_html)

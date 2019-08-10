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
  COMMAND ${VIRTUALENV_EXE} venv &&
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

set(tar_file ${CMAKE_BINARY_DIR}/packages/${CMAKE_PROJECT_NAME}-docs-${FDB_VERSION}.tar.gz)
add_custom_command(
  OUTPUT ${tar_file}
  COMMAND ${CMAKE_COMMAND} -E tar czf ${tar_file} .
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html)
add_custom_target(package_html DEPENDS ${tar_file})
add_dependencies(package_html html)
add_dependencies(packages package_html)

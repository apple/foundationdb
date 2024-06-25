add_subdirectory(tutorial)
add_subdirectory(coro_tutorial)

set(SPHINX_DOCUMENT_DIR "${CMAKE_SOURCE_DIR}/documentation/sphinx")

find_package(Python3 REQUIRED COMPONENTS Interpreter)
find_package(Sphinx)
if(NOT Sphinx_FOUND)
  message(STATUS "Sphinx not found, setting up sphinx virtual environment")
  set(SPHINX_VENV_DIR "${CMAKE_BINARY_DIR}/sphinx-venv")
  execute_process(COMMAND "${Python3_EXECUTABLE}" -m venv ${SPHINX_VENV_DIR})
  find_program(
    VENV_Python3_EXECUTABLE
    NAMES python3 python3.exe
    PATHS ${SPHINX_VENV_DIR}/Scripts ${SPHINX_VENV_DIR}/bin REQUIRED
    NO_DEFAULT_PATH NO_CACHE
    DOC "Checking Python3 executable in virtual environment")
  execute_process(COMMAND "${VENV_Python3_EXECUTABLE}" -m ensurepip COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${VENV_Python3_EXECUTABLE}" -m pip install --upgrade pip COMMAND_ERROR_IS_FATAL ANY)
  execute_process(COMMAND "${VENV_Python3_EXECUTABLE}" -m pip install -r "${SPHINX_DOCUMENT_DIR}/requirements.txt" COMMAND_ERROR_IS_FATAL ANY)
  set(Sphinx_ROOT "${SPHINX_VENV_DIR}")
  unset(Sphinx_FOUND)
  find_package(Sphinx REQUIRED)
endif()

message(
  STATUS "Sphinx found at ${Sphinx_EXECUTABLE} with version ${Sphinx_VERSION}")

function(add_documentation_target)
  set(options)
  set(oneValueArgs GENERATOR DOCTREE)
  set(multiValueArgs ADDITIONAL_ARGUMENTS)
  cmake_parse_arguments(ADT "${options}" "${oneValueArgs}" "${multiValueArgs}"
                        "${ARGN}")

  if(NOT ADT_GENERATOR)
    message(ERROR
            "GENERATOR is a required argument to add_documentation_target")
  endif()
  set(TARGET ${ADT_GENERATOR})

  set(DOCTREE "doctree")
  if(ADT_DOCTREE)
    set(DOCTREE "${ADT_DOCTREE}")
  endif()

  set(OUTPUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/${TARGET})
  set(STAMP_FILE ${CMAKE_CURRENT_BINARY_DIR}/document_${TARGET}_done)
  file(
    GLOB_RECURSE DOC_FILES
    LIST_DIRECTORIES false
    CONFIGURE_DEPENDS ${SPHINX_DOCUMENT_DIR}/*.*)
  add_custom_command(
    OUTPUT ${STAMP_FILE}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${OUTPUT_DIR}
    COMMAND
      ${Sphinx_EXECUTABLE} -W -b ${TARGET} -d ${DOCTREE} -c
      ${SPHINX_DOCUMENT_DIR} ${SPHINX_DOCUMENT_DIR}/source ${OUTPUT_DIR}
      -Dversion=${FDB_VERSION} -Drelease=${FDB_RELEASE}
    COMMAND ${CMAKE_COMMAND} -E touch ${STAMP_FILE}
    DEPENDS ${DOC_FILES}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
  add_custom_target(${TARGET}
                    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/document_${TARGET}_done)
endfunction()

message(STATUS "Add html target")
add_documentation_target(GENERATOR html)

add_custom_target(CopyJavadoc
  COMMAND ${CMAKE_COMMAND} -E copy_directory ${SPHINX_DOCUMENT_DIR}/javadoc ${CMAKE_BINARY_DIR}/docs/javadoc
  COMMENT "Copying Javadoc to build directory"
)

set(DOCSERVER_PORT
    "-1"
    CACHE
      STRING
      "Port to which the documentation server should bind (negative means cmake will choose one)"
)

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
  # cmake math function can only use 64 bit signed integers - so we just
  # truncate the string
  string(SUBSTRING "${username_hash}" 0 15 username_hash_small)
  math(EXPR port "(0x${username_hash_small} % 8000) + 8000"
       OUTPUT_FORMAT DECIMAL)
  message(STATUS "Port is ${port}")
endif()

add_custom_target(
  docpreview
  COMMAND ${Python3_EXECUTABLE} -m http.server ${port}
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html
  USES_TERMINAL)
add_dependencies(docpreview html)

set(tar_file
    ${CMAKE_BINARY_DIR}/packages/${CMAKE_PROJECT_NAME}-docs-${FDB_VERSION}.tar.gz
)
add_custom_command(
  OUTPUT ${tar_file}
  COMMAND ${CMAKE_COMMAND} -E tar czf ${tar_file} .
  WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html)
add_custom_target(package_html DEPENDS ${tar_file})
add_dependencies(package_html html)
add_dependencies(packages package_html)


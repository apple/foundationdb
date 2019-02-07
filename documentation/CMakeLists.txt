find_package(Sphinx)
if(SPHINX_FOUND)
  set(SPHINX_OPTIONS "" CACHE STRING "Additional Options to pass to sphinx-build")
  set(SPHINX_PAPER_SIZE "letter" CACHE STRING "Paper size for documents generated with sphinx")
  set(SPHINX_OPTS "${SPHINX_OPTIONS} -c . -d ${CMAKE_CURRENT_BINARY_DIR}/doctrees -D latex_paper_size=${SPHINX_PAPER_SIZE}")
  file(GLOB_RECURSE RST_FILES "source/*rst")
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/html ${CMAKE_CURRENT_BINARY_DIR}/doctrees)
  add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/html/index.html
    COMMAND ${SPHINXBUILD} -b html ${SPHINX_OPTS} ${CMAKE_CURRENT_BINARY_DIR}/html
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/sphinx
    DEPENDS ${RST_FILES})
  add_custom_target(html DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/html/index.html)
else()
  message(WARNING "Cound not find sphinx-build - will not build documentatioin")
endif()

find_program(SPHINXBUILD
  sphinx-build
  DOC "Sphinx-build tool")

find_package_handle_standard_args(Sphinx
  FOUND_VAR SPHINX_FOUND
  REQUIRED_VARS SPHINXBUILD)

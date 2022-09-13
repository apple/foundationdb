# Generate flow modulemap

function(generate_modulemap out module target)
  cmake_parse_arguments(ARG "" "" "OMIT" ${ARGN})

  set(MODULE_NAME "${module}")
  get_target_property(MODULE_HEADERS ${target} HEADER_FILES)
  foreach(header ${MODULE_HEADERS})
    get_filename_component(fname ${header} NAME)
    if(NOT ${fname} IN_LIST ARG_OMIT)
      set(header_list "${header_list}    header \"${header}\"\n")
    endif()
  endforeach()
  set(MODULE_HEADERS "${header_list}")
  configure_file("${CMAKE_SOURCE_DIR}/swifttestapp/empty.modulemap" "${out}/module.modulemap" @ONLY)
endfunction()

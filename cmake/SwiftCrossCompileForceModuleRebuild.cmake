# This function forces the Swift compiler to rebuild 'Swift' and 'CxxStdlib'
# from .swiftinterface files. This is useful when cross-compiling, when the
# host compiler version is different than the Swift version on the linux
# target container.

function(swift_force_import_rebuild_of_stdlib)
    message(STATUS "Making sure Swift builtin modules are up-to-date...")
    string(REPLACE " " ";" Swift_FLAGS_LIST ${CMAKE_Swift_FLAGS})
    set(Rebuilt_Swift_FLAGS_LIST)
    foreach (flag ${Swift_FLAGS_LIST})
      if ("${flag}" STREQUAL "-resource-dir")
        list(APPEND Rebuilt_Swift_FLAGS_LIST "-Xfrontend" ${flag} "-Xfrontend")
      else()
        list(APPEND Rebuilt_Swift_FLAGS_LIST ${flag})
      endif()
    endforeach()
    list(APPEND Rebuilt_Swift_FLAGS_LIST "-Xfrontend" "-strict-implicit-module-context")
    file(WRITE "${CMAKE_BINARY_DIR}/CMakeTmp/rebuildStdlib.swift" "import Swift")
    execute_process(COMMAND "${CMAKE_Swift_COMPILER}" -c -o "${CMAKE_BINARY_DIR}/CMakeTmp/rebuildStdlib.o" ${Rebuilt_Swift_FLAGS_LIST}  "${CMAKE_BINARY_DIR}/CMakeTmp/rebuildStdlib.swift"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    RESULT_VARIABLE
      CanSwiftImportSwift
    )
    if (NOT ${CanSwiftImportSwift} EQUAL 0)
      message(FATAL_ERROR "Swift couldn't import/rebuild standard library.")
    endif()
    file(WRITE "${CMAKE_BINARY_DIR}/CMakeTmp/rebuildCxxStdlib.swift" "import CxxStdlib")
    execute_process(COMMAND "${CMAKE_Swift_COMPILER}" -c -o "${CMAKE_BINARY_DIR}/CMakeTmp/rebuildCxxStdlib.o" ${Rebuilt_Swift_FLAGS_LIST}  "${CMAKE_BINARY_DIR}/CMakeTmp/rebuildCxxStdlib.swift"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    RESULT_VARIABLE
      CanSwiftImportSwiftCxxStdlib
    )
    if (NOT ${CanSwiftImportSwiftCxxStdlib} EQUAL 0)
      message(FATAL_ERROR "Swift couldn't import/rebuild C++ standard library.")
    endif()
endfunction()

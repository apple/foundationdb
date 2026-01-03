set(VEXILLOGRAPHER_CSPROJ
    ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/vexillographer.csproj)
set(VEXILLOGRAPHER_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/c.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/cpp.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/java.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/python.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/ruby.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/vexillographer.cs)
set(VEXILLOGRAPHER_PY ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/vexillographer.py)

if(NOT DEFINED FDB_USE_CSHARP_TOOLS)
  set(FDB_USE_CSHARP_TOOLS TRUE)
endif()
set(VEXILLOGRAPHER_COMMAND "")

if(WIN32 AND FDB_USE_CSHARP_TOOLS)
  add_executable(vexillographer ${VEXILLOGRAPHER_SRCS})
  target_compile_options(vexillographer PRIVATE "/langversion:6")
  set_property(
    TARGET vexillographer PROPERTY VS_DOTNET_REFERENCES "System" "System.Core"
                                   "System.Data" "System.Xml" "System.Xml.Linq")
  set(VEXILLOGRAPHER_DEPENDS vexillographer)
elseif(FDB_USE_CSHARP_TOOLS AND CSHARP_TOOLCHAIN_FOUND)
  if(CSHARP_USE_MONO)
    set(VEXILLOGRAPHER_REFERENCES
        "-r:System,System.Core,System.Data,System.Xml,System.Xml.Linq")
    set(VEXILLOGRAPHER_EXE "${CMAKE_CURRENT_BINARY_DIR}/vexillographer.exe")
    add_custom_command(
      OUTPUT ${VEXILLOGRAPHER_EXE}
      COMMAND ${CSHARP_COMPILER_EXECUTABLE} ARGS ${VEXILLOGRAPHER_REFERENCES}
              ${VEXILLOGRAPHER_SRCS} -target:exe -out:${VEXILLOGRAPHER_EXE}
      DEPENDS ${VEXILLOGRAPHER_SRCS}
      COMMENT "Compile Vexillographer")
    add_custom_target(vexillographer DEPENDS ${VEXILLOGRAPHER_EXE})
    set(VEXILLOGRAPHER_DEPENDS vexillographer)
    set(VEXILLOGRAPHER_COMMAND ${MONO_EXECUTABLE} ${VEXILLOGRAPHER_EXE})
  else()
    dotnet_build(${VEXILLOGRAPHER_CSPROJ} SOURCE ${VEXILLOGRAPHER_SRCS})
    message(STATUS "Generated executable: ${vexillographer_EXECUTABLE_PATH}")
    set(VEXILLOGRAPHER_EXE ${vexillographer_EXECUTABLE_PATH})
    set(VEXILLOGRAPHER_COMMAND ${dotnet_EXECUTABLE} ${vexillographer_EXECUTABLE_PATH})
    set(VEXILLOGRAPHER_DEPENDS ${vexillographer_EXECUTABLE_PATH})
  endif()
else()
  find_package(Python3 COMPONENTS Interpreter REQUIRED)
  set(VEXILLOGRAPHER_COMMAND ${Python3_EXECUTABLE} ${VEXILLOGRAPHER_PY})
  set(VEXILLOGRAPHER_DEPENDS ${VEXILLOGRAPHER_PY})
endif()

function(vexillographer_compile)
  set(CX_OPTIONS ALL)
  set(CX_ONE_VALUE_ARGS TARGET LANG OUT)
  set(CX_MULTI_VALUE_ARGS OUTPUT)
  cmake_parse_arguments(VX "${CX_OPTIONS}" "${CX_ONE_VALUE_ARGS}"
                        "${CX_MULTI_VALUE_ARGS}" "${ARGN}")
  if(NOT VX_OUTPUT)
    set(VX_OUTPUT ${VX_OUT})
  endif()

  if(WIN32 AND FDB_USE_CSHARP_TOOLS)
    add_custom_command(
      OUTPUT ${VX_OUTPUT}
      COMMAND $<TARGET_FILE:vexillographer>
        ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options ${VX_LANG}
        ${VX_OUT}
      DEPENDS ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
              vexillographer
      COMMENT "Generate FDBOptions ${VX_LANG} files")
  elseif(FDB_USE_CSHARP_TOOLS AND CSHARP_TOOLCHAIN_FOUND AND CSHARP_USE_MONO)
    add_custom_command(
      OUTPUT ${VX_OUTPUT}
      COMMAND ${VEXILLOGRAPHER_COMMAND}
        ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options ${VX_LANG}
        ${VX_OUT}
      DEPENDS ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
              vexillographer
      COMMENT "Generate FDBOptions ${VX_LANG} files")
  else()
    add_custom_command(
      OUTPUT ${VX_OUTPUT}
      COMMAND ${VEXILLOGRAPHER_COMMAND}
        ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options ${VX_LANG}
        ${VX_OUT}
      DEPENDS ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options
              ${VEXILLOGRAPHER_DEPENDS}
      COMMENT "Generate FDBOptions ${VX_LANG} files")
  endif()

  if(VX_ALL)
    add_custom_target(${VX_TARGET} ALL DEPENDS ${VX_OUTPUT})
  else()
    add_custom_target(${VX_TARGET} DEPENDS ${VX_OUTPUT})
  endif()
endfunction()

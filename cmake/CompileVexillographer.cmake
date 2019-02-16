set(VEXILLOGRAPHER_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/c.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/cpp.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/java.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/python.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/ruby.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/vexillographer.cs)

if(WIN32)
  add_executable(vexillographer ${VEXILLOGRAPHER_SRCS})
  target_compile_options(vexillographer PRIVATE  "/langversion:6")
  set_property(TARGET vexillographer PROPERTY VS_DOTNET_REFERENCES
    "System"
    "System.Core"
    "System.Data"
    "System.Xml"
    "System.Xml.Linq")
else()
  set(VEXILLOGRAPHER_REFERENCES "-r:System,System.Core,System.Data,System.Xml,System.Xml.Linq")
  set(VEXILLOGRAPHER_EXE "${CMAKE_CURRENT_BINARY_DIR}/vexillographer.exe")
  add_custom_command(OUTPUT ${VEXILLOGRAPHER_EXE}
    COMMAND ${MCS_EXECUTABLE} ARGS ${VEXILLOGRAPHER_REFERENCES} ${VEXILLOGRAPHER_SRCS} -target:exe -out:${VEXILLOGRAPHER_EXE}
    DEPENDS ${VEXILLOGRAPHER_SRCS}
    COMMENT "Compile Vexillographer")
  add_custom_target(vexillographer DEPENDS ${VEXILLOGRAPHER_EXE})
endif()

function(vexillographer_compile)
  set(CX_OPTIONS ALL)
  set(CX_ONE_VALUE_ARGS TARGET LANG OUT)
  set(CX_MULTI_VALUE_ARGS OUTPUT)
  cmake_parse_arguments(VX "${CX_OPTIONS}" "${CX_ONE_VALUE_ARGS}" "${CX_MULTI_VALUE_ARGS}" "${ARGN}")
  if(NOT VX_OUTPUT)
    set(VX_OUTPUT ${VX_OUT})
  endif()
  if(WIN32)
    add_custom_command(
      OUTPUT ${VX_OUTPUT}
      COMMAND $<TARGET_FILE:vexillographer> ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options ${VX_LANG} ${VX_OUT}
      DEPENDS ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options vexillographer
      COMMENT "Generate FDBOptions ${VX_LANG} files")
  else()
    add_custom_command(
      OUTPUT ${VX_OUTPUT}
      COMMAND ${MONO_EXECUTABLE} ${VEXILLOGRAPHER_EXE} ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options ${VX_LANG} ${VX_OUT}
      DEPENDS ${CMAKE_SOURCE_DIR}/fdbclient/vexillographer/fdb.options vexillographer
      COMMENT "Generate FDBOptions ${VX_LANG} files")
  endif()
  if(VX_ALL)
    add_custom_target(${VX_TARGET} ALL DEPENDS ${VX_OUTPUT})
  else()
    add_custom_target(${VX_TARGET} DEPENDS ${VX_OUTPUT})
  endif()
endfunction()

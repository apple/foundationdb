set(VEXILLOGRAPHER_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/c.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/cpp.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/java.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/python.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/ruby.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/fdbclient/vexillographer/vexillographer.cs)

set(VEXILLOGRAPHER_REFERENCES "-r:System,System.Core,System.Data,System.Xml,System.Xml.Linq")
set(VEXILLOGRAPHER_EXE "${CMAKE_CURRENT_BINARY_DIR}/vexillographer.exe")
add_custom_command(OUTPUT ${VEXILLOGRAPHER_EXE}
  COMMAND ${MCS_EXECUTABLE} ARGS ${VEXILLOGRAPHER_REFERENCES} ${VEXILLOGRAPHER_SRCS} -target:exe -out:${VEXILLOGRAPHER_EXE}
  DEPENDS ${VEXILLOGRAPHER_SRCS}
  COMMENT "Compile Vexillographer")
add_custom_target(vexillographer DEPENDS ${VEXILLOGRAPHER_EXE})

set(ERROR_GEN_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/error_gen.cs)
set(ERROR_GEN_REFERENCES "-r:System,System.Core,System.Data,System.Xml,System.Xml.Linq")
set(ERROR_GEN_EXE "${CMAKE_CURRENT_BINARY_DIR}/error_gen.exe")
add_custom_command (OUTPUT ${ERROR_GEN_EXE}
  COMMAND ${MCS_EXECUTABLE} ARGS ${ERROR_GEN_REFERENCES} ${ERROR_GEN_SRCS} -target:exe -out:${ERROR_GEN_EXE}
  DEPENDS ${ERROR_GEN_SRCS}
  COMMENT "Compile error_gen")
add_custom_target(error_gen DEPENDS ${ERROR_GEN_EXE})

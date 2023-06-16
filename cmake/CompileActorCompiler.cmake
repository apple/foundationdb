set(ACTORCOMPILER_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ActorCompiler.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ActorParser.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ParseTree.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/Program.cs
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/Properties/AssemblyInfo.cs)
set(ACTOR_COMPILER_REFERENCES
  "-r:System,System.Core,System.Xml.Linq,System.Data.DataSetExtensions,Microsoft.CSharp,System.Data,System.Xml")

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe
  COMMAND ${MCS_EXECUTABLE} ARGS ${ACTOR_COMPILER_REFERENCES} ${ACTORCOMPILER_SRCS} "-target:exe" "-out:actorcompiler.exe"
  DEPENDS ${ACTORCOMPILER_SRCS}
  COMMENT "Compile actor compiler" VERBATIM)
add_custom_target(actorcompiler DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe)
set(actor_exe "${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe")

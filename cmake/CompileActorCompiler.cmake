find_package(Python3 REQUIRED COMPONENTS Interpreter)

find_program(MCS_EXECUTABLE mcs)
find_program(MONO_EXECUTABLE mono)

set(ACTORCOMPILER_PY_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler_py/__main__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler_py/errors.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler_py/actor_parser.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler_py/actor_compiler.py)

set(ACTORCOMPILER_CSPROJ
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/actorcompiler.csproj)

set(ACTORCOMPILER_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ActorCompiler.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ActorParser.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ParseTree.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/Program.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/Properties/AssemblyInfo.cs)

set(ACTOR_COMPILER_REFERENCES
    "-r:System,System.Core,System.Xml.Linq,System.Data.DataSetExtensions,Microsoft.CSharp,System.Data,System.Xml"
)

add_custom_target(actorcompiler_py DEPENDS ${ACTORCOMPILER_PY_SRCS})

if(WIN32)
  add_executable(actorcompiler_csharp ${ACTORCOMPILER_SRCS})
  target_compile_options(actorcompiler_csharp PRIVATE "/langversion:6")
  set_property(
    TARGET actorcompiler_csharp
    PROPERTY VS_DOTNET_REFERENCES
             "System"
             "System.Core"
             "System.Xml.Linq"
             "System.Data.DataSetExtensions"
             "Microsoft.CSharp"
             "System.Data"
             "System.Xml")
  set(ACTORCOMPILER_CSHARP_COMMAND $<TARGET_FILE:actorcompiler_csharp>
      CACHE INTERNAL "Command to run the C# actor compiler")
  add_custom_target(actorcompiler)
  add_dependencies(actorcompiler actorcompiler_csharp actorcompiler_py)
elseif(CSHARP_USE_MONO)
  add_custom_command(
    OUTPUT actorcompiler.exe
    COMMAND ${CSHARP_COMPILER_EXECUTABLE} ARGS ${ACTOR_COMPILER_REFERENCES}
            ${ACTORCOMPILER_SRCS} "-target:exe" "-out:actorcompiler.exe"
    DEPENDS ${ACTORCOMPILER_SRCS}
    COMMENT "Compile actor compiler"
    VERBATIM)
  add_custom_target(actorcompiler_csharp
                    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe)
  set(actor_exe "${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe")
  set(ACTORCOMPILER_CSHARP_COMMAND ${MONO_EXECUTABLE} ${actor_exe}
      CACHE INTERNAL "Command to run the C# actor compiler")
  add_custom_target(actorcompiler)
  add_dependencies(actorcompiler actorcompiler_csharp actorcompiler_py)
else()
  dotnet_build(${ACTORCOMPILER_CSPROJ} SOURCE ${ACTORCOMPILER_SRCS})
  set(actor_exe "${actorcompiler_EXECUTABLE_PATH}")
  message(STATUS "Actor compiler path: ${actor_exe}")
  # dotnet_build already creates a target named 'actorcompiler', so we just add Python dependency
  add_dependencies(actorcompiler actorcompiler_py)
  set(ACTORCOMPILER_CSHARP_COMMAND ${actor_exe}
      CACHE INTERNAL "Command to run the C# actor compiler")
endif()

set(ACTORCOMPILER_COMMAND
  ${Python3_EXECUTABLE} -m flow.actorcompiler_py
  CACHE INTERNAL "Command to run the actor compiler")

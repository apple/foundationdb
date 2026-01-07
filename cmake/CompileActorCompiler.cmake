find_package(Python3 REQUIRED COMPONENTS Interpreter)

if(NOT DEFINED FDB_USE_CSHARP_TOOLS)
  set(FDB_USE_CSHARP_TOOLS TRUE)
endif()

set(ACTORCOMPILER_PY_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler_py/__main__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler_py/errors.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler_py/actor_parser.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler_py/actor_compiler.py)

set(ACTORCOMPILER_CSPROJ
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/actorcompiler.csproj)

set(ACTORCOMPILER_LEGACY_SRCS
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ActorCompiler.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ActorParser.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/ParseTree.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/Program.cs
    ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/Properties/AssemblyInfo.cs)

set(ACTOR_COMPILER_REFERENCES
    "-r:System,System.Core,System.Xml.Linq,System.Data.DataSetExtensions,Microsoft.CSharp,System.Data,System.Xml"
)

add_custom_target(actorcompiler_py DEPENDS ${ACTORCOMPILER_PY_SRCS})

set(ACTORCOMPILER_PY_COMMAND
  ${Python3_EXECUTABLE} -m flow.actorcompiler_py
  CACHE INTERNAL "Command to run the Python actor compiler")
set(ACTORCOMPILER_CSHARP_COMMAND ""
    CACHE INTERNAL "Command to run the C# actor compiler")

set(ACTORCOMPILER_COMMAND ${ACTORCOMPILER_PY_COMMAND}
    CACHE INTERNAL "Command to run the actor compiler")

set(actorcompiler_dependencies actorcompiler_py)

if(FDB_USE_CSHARP_TOOLS AND CSHARP_TOOLCHAIN_FOUND)
  if(WIN32)
    add_executable(actorcompiler_csharp ${ACTORCOMPILER_LEGACY_SRCS})
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
    list(APPEND actorcompiler_dependencies actorcompiler_csharp)
  elseif(CSHARP_USE_MONO)
    add_custom_command(
      OUTPUT actorcompiler.exe
      COMMAND ${CSHARP_COMPILER_EXECUTABLE} ARGS ${ACTOR_COMPILER_REFERENCES}
              ${ACTORCOMPILER_LEGACY_SRCS} "-target:exe" "-out:actorcompiler.exe"
      DEPENDS ${ACTORCOMPILER_LEGACY_SRCS}
      COMMENT "Compile actor compiler"
      VERBATIM)
    add_custom_target(actorcompiler_csharp
                      DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe)
    set(actor_exe "${CMAKE_CURRENT_BINARY_DIR}/actorcompiler.exe")
    set(ACTORCOMPILER_CSHARP_COMMAND ${MONO_EXECUTABLE} ${actor_exe}
        CACHE INTERNAL "Command to run the C# actor compiler")
    list(APPEND actorcompiler_dependencies actorcompiler_csharp)
  else()
    dotnet_build(${ACTORCOMPILER_CSPROJ} SOURCE ${ACTORCOMPILER_LEGACY_SRCS})
    set(actor_exe "${actorcompiler_EXECUTABLE_PATH}")
    message(STATUS "Actor compiler path: ${actor_exe}")
    set(ACTORCOMPILER_CSHARP_COMMAND ${dotnet_EXECUTABLE} ${actor_exe}
        CACHE INTERNAL "Command to run the C# actor compiler")
  endif()
endif()

if(NOT TARGET actorcompiler)
  add_custom_target(actorcompiler)
endif()
add_dependencies(actorcompiler ${actorcompiler_dependencies})

if(ACTORCOMPILER_CSHARP_COMMAND)
  set(ACTORCOMPILER_COMMAND ${ACTORCOMPILER_CSHARP_COMMAND}
      CACHE INTERNAL "Command to run the actor compiler" FORCE)
endif()

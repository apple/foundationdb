find_package(Python3 REQUIRED COMPONENTS Interpreter)

set(ACTORCOMPILER_SRCS
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/__init__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/__main__.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/main.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/actor_parser.py
  ${CMAKE_CURRENT_SOURCE_DIR}/flow/actorcompiler/actor_compiler.py)

add_custom_target(actorcompiler DEPENDS ${ACTORCOMPILER_SRCS})

set(ACTORCOMPILER_COMMAND
  ${Python3_EXECUTABLE} -m flow.actorcompiler
  CACHE INTERNAL "Command to run the actor compiler")

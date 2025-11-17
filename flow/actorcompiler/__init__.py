"""
Python port of the Flow actor compiler.

Typical usage:
    from flow.actorcompiler import main
    main(["input.actor.cpp", "output.g.actor.cpp"])
"""


class ActorCompilerError(Exception):
    """Exception raised for parser or compiler errors with source locations."""

    def __init__(self, source_line: int, message: str, *args: object) -> None:
        if args:
            message = message.format(*args)
        super().__init__(message)
        self.source_line = source_line

    def __str__(self) -> str:
        return f"{super().__str__()} (line {self.source_line})"


from .actor_compiler import ActorCompiler
from .actor_parser import ActorParser, ErrorMessagePolicy
from .main import main

__all__ = [
    "ActorCompiler",
    "ActorParser",
    "ErrorMessagePolicy",
    "ActorCompilerError",
    "main",
]

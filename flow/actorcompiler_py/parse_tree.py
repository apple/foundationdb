from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional, Sequence


@dataclass
class VarDeclaration:
    type: str = ""
    name: str = ""
    initializer: Optional[str] = ""
    initializer_constructor_syntax: bool = False


@dataclass
class Statement(ABC):
    first_source_line: int = 0

    @abstractmethod
    def contains_wait(self) -> bool:
        pass


@dataclass
class CodeBlock(Statement):
    statements: Sequence[Statement] = field(default_factory=list)

    def __str__(self) -> str:
        joined = "\n".join(str(stmt) for stmt in self.statements)
        return f"CodeBlock\n{joined}\nEndCodeBlock"

    def contains_wait(self) -> bool:
        return any(stmt.contains_wait() for stmt in self.statements)


@dataclass
class PlainOldCodeStatement(Statement):
    code: str = ""

    def __str__(self) -> str:
        return self.code

    def contains_wait(self) -> bool:
        return False


@dataclass
class StateDeclarationStatement(Statement):
    decl: VarDeclaration = field(default_factory=VarDeclaration)

    def __str__(self) -> str:
        if self.decl.initializer_constructor_syntax:
            return f"State {self.decl.type} {self.decl.name}({self.decl.initializer});"
        return f"State {self.decl.type} {self.decl.name} = {self.decl.initializer};"

    def contains_wait(self) -> bool:
        return False


@dataclass
class WhileStatement(Statement):
    expression: str = ""
    body: Statement = field(default_factory=CodeBlock)

    def contains_wait(self) -> bool:
        return self.body.contains_wait()


@dataclass
class ForStatement(Statement):
    init_expression: str = ""
    cond_expression: str = ""
    next_expression: str = ""
    body: Statement = field(default_factory=CodeBlock)

    def contains_wait(self) -> bool:
        return self.body.contains_wait()


@dataclass
class RangeForStatement(Statement):
    range_expression: str = ""
    range_decl: str = ""
    body: Statement = field(default_factory=CodeBlock)

    def contains_wait(self) -> bool:
        return self.body.contains_wait()


@dataclass
class LoopStatement(Statement):
    body: Statement = field(default_factory=CodeBlock)

    def __str__(self) -> str:
        return f"Loop {self.body}"

    def contains_wait(self) -> bool:
        return self.body.contains_wait()


@dataclass
class BreakStatement(Statement):
    def contains_wait(self) -> bool:
        return False


@dataclass
class ContinueStatement(Statement):
    def contains_wait(self) -> bool:
        return False


@dataclass
class IfStatement(Statement):
    expression: str = ""
    constexpr: bool = False
    if_body: Statement = field(default_factory=CodeBlock)
    else_body: Optional[Statement] = None

    def contains_wait(self) -> bool:
        return self.if_body.contains_wait() or (
            self.else_body is not None and self.else_body.contains_wait()
        )


@dataclass
class ReturnStatement(Statement):
    expression: str = ""

    def __str__(self) -> str:
        return f"Return {self.expression}"

    def contains_wait(self) -> bool:
        return False


@dataclass
class WaitStatement(Statement):
    result: VarDeclaration = field(default_factory=VarDeclaration)
    future_expression: str = ""
    result_is_state: bool = False
    is_wait_next: bool = False

    def __str__(self) -> str:
        return f"Wait {self.result.type} {self.result.name} <- {self.future_expression} ({'state' if self.result_is_state else 'local'})"

    def contains_wait(self) -> bool:
        return True


@dataclass
class ChooseStatement(Statement):
    body: Statement = field(default_factory=CodeBlock)

    def __str__(self) -> str:
        return f"Choose {self.body}"

    def contains_wait(self) -> bool:
        return self.body.contains_wait()


@dataclass
class WhenStatement(Statement):
    wait: WaitStatement = field(default_factory=WaitStatement)
    body: Optional[Statement] = None

    def __str__(self) -> str:
        return f"When ({self.wait}) {self.body}"

    def contains_wait(self) -> bool:
        return True


@dataclass
class TryStatement(Statement):
    @dataclass
    class Catch:
        expression: str = ""
        body: Statement = field(default_factory=CodeBlock)
        first_source_line: int = 0

    try_body: Statement = field(default_factory=CodeBlock)
    catches: List["TryStatement.Catch"] = field(default_factory=list)

    def contains_wait(self) -> bool:
        if self.try_body.contains_wait():
            return True
        return any(c.body.contains_wait() for c in self.catches)


@dataclass
class ThrowStatement(Statement):
    expression: str = ""

    def contains_wait(self) -> bool:
        return False


@dataclass
class Declaration:
    type: str = ""
    name: str = ""
    comment: str = ""


@dataclass
class Actor:
    attributes: List[str] = field(default_factory=list)
    return_type: Optional[str] = None
    name: str = ""
    enclosing_class: Optional[str] = None
    parameters: Sequence[VarDeclaration] = field(default_factory=list)
    template_formals: Optional[Sequence[VarDeclaration]] = None
    body: CodeBlock = field(default_factory=CodeBlock)
    source_line: int = 0
    is_static: bool = False
    _is_uncancellable: bool = False
    test_case_parameters: Optional[str] = None
    name_space: Optional[str] = None
    is_forward_declaration: bool = False
    is_test_case: bool = False

    def is_cancellable(self) -> bool:
        return self.return_type is not None and not self._is_uncancellable

    def set_uncancellable(self) -> None:
        self._is_uncancellable = True


@dataclass
class Descr:
    name: str = ""
    super_class_list: Optional[str] = None
    body: List[Declaration] = field(default_factory=list)

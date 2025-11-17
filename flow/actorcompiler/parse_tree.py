from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Sequence


@dataclass
class VarDeclaration:
    type: str = ""
    name: str = ""
    initializer: Optional[str] = ""
    initializerConstructorSyntax: bool = False


@dataclass
class Statement:
    FirstSourceLine: int = 0

    def containsWait(self) -> bool:
        return False


@dataclass
class PlainOldCodeStatement(Statement):
    code: str = ""

    def __str__(self) -> str:
        return self.code


@dataclass
class StateDeclarationStatement(Statement):
    decl: VarDeclaration = field(default_factory=VarDeclaration)

    def __str__(self) -> str:
        if self.decl.initializerConstructorSyntax:
            return f"State {self.decl.type} {self.decl.name}({self.decl.initializer});"
        return f"State {self.decl.type} {self.decl.name} = {self.decl.initializer};"


@dataclass
class WhileStatement(Statement):
    expression: str = ""
    body: Statement = field(default_factory=Statement)

    def containsWait(self) -> bool:
        return self.body.containsWait()


@dataclass
class ForStatement(Statement):
    initExpression: str = ""
    condExpression: str = ""
    nextExpression: str = ""
    body: Statement = field(default_factory=Statement)

    def containsWait(self) -> bool:
        return self.body.containsWait()


@dataclass
class RangeForStatement(Statement):
    rangeExpression: str = ""
    rangeDecl: str = ""
    body: Statement = field(default_factory=Statement)

    def containsWait(self) -> bool:
        return self.body.containsWait()


@dataclass
class LoopStatement(Statement):
    body: Statement = field(default_factory=Statement)

    def __str__(self) -> str:
        return f"Loop {self.body}"

    def containsWait(self) -> bool:
        return self.body.containsWait()


@dataclass
class BreakStatement(Statement):
    pass


@dataclass
class ContinueStatement(Statement):
    pass


@dataclass
class IfStatement(Statement):
    expression: str = ""
    constexpr: bool = False
    ifBody: Statement = field(default_factory=Statement)
    elseBody: Optional[Statement] = None

    def containsWait(self) -> bool:
        return self.ifBody.containsWait() or (
            self.elseBody is not None and self.elseBody.containsWait()
        )


@dataclass
class ReturnStatement(Statement):
    expression: str = ""

    def __str__(self) -> str:
        return f"Return {self.expression}"


@dataclass
class WaitStatement(Statement):
    result: VarDeclaration = field(default_factory=VarDeclaration)
    futureExpression: str = ""
    resultIsState: bool = False
    isWaitNext: bool = False

    def __str__(self) -> str:
        return f"Wait {self.result.type} {self.result.name} <- {self.futureExpression} ({'state' if self.resultIsState else 'local'})"

    def containsWait(self) -> bool:
        return True


@dataclass
class ChooseStatement(Statement):
    body: Statement = field(default_factory=Statement)

    def __str__(self) -> str:
        return f"Choose {self.body}"

    def containsWait(self) -> bool:
        return self.body.containsWait()


@dataclass
class WhenStatement(Statement):
    wait: WaitStatement = field(default_factory=WaitStatement)
    body: Optional[Statement] = None

    def __str__(self) -> str:
        return f"When ({self.wait}) {self.body}"

    def containsWait(self) -> bool:
        return True


@dataclass
class TryStatement(Statement):
    @dataclass
    class Catch:
        expression: str = ""
        body: Statement = field(default_factory=Statement)
        FirstSourceLine: int = 0

    tryBody: Statement = field(default_factory=Statement)
    catches: List["TryStatement.Catch"] = field(default_factory=list)

    def containsWait(self) -> bool:
        if self.tryBody.containsWait():
            return True
        return any(c.body.containsWait() for c in self.catches)


@dataclass
class ThrowStatement(Statement):
    expression: str = ""


@dataclass
class CodeBlock(Statement):
    statements: Sequence[Statement] = field(default_factory=list)

    def __str__(self) -> str:
        joined = "\n".join(str(stmt) for stmt in self.statements)
        return f"CodeBlock\n{joined}\nEndCodeBlock"

    def containsWait(self) -> bool:
        return any(stmt.containsWait() for stmt in self.statements)


@dataclass
class Declaration:
    type: str = ""
    name: str = ""
    comment: str = ""


@dataclass
class Actor:
    attributes: List[str] = field(default_factory=list)
    returnType: Optional[str] = None
    name: str = ""
    enclosingClass: Optional[str] = None
    parameters: Sequence[VarDeclaration] = field(default_factory=list)
    templateFormals: Optional[Sequence[VarDeclaration]] = None
    body: CodeBlock = field(default_factory=CodeBlock)
    SourceLine: int = 0
    isStatic: bool = False
    _isUncancellable: bool = False
    testCaseParameters: Optional[str] = None
    nameSpace: Optional[str] = None
    isForwardDeclaration: bool = False
    isTestCase: bool = False

    def IsCancellable(self) -> bool:
        return self.returnType is not None and not self._isUncancellable

    def SetUncancellable(self) -> None:
        self._isUncancellable = True


@dataclass
class Descr:
    name: str = ""
    superClassList: Optional[str] = None
    body: List[Declaration] = field(default_factory=list)

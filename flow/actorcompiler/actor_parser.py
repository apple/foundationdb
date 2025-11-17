from __future__ import annotations

import io
import re
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from . import ActorCompilerError
from .actor_compiler import ActorCompiler, DescrCompiler

from .parse_tree import (
    Actor,
    BreakStatement,
    ChooseStatement,
    CodeBlock,
    ContinueStatement,
    Declaration,
    Descr,
    ForStatement,
    IfStatement,
    LoopStatement,
    PlainOldCodeStatement,
    RangeForStatement,
    ReturnStatement,
    StateDeclarationStatement,
    Statement,
    ThrowStatement,
    TryStatement,
    VarDeclaration,
    WaitStatement,
    WhenStatement,
    WhileStatement,
)


class ErrorMessagePolicy:
    def __init__(self) -> None:
        self.DisableDiagnostics = False

    def HandleActorWithoutWait(self, sourceFile: str, actor: Actor) -> None:
        if not self.DisableDiagnostics and not actor.isTestCase:
            print(
                f"{sourceFile}:{actor.SourceLine}: warning: ACTOR {actor.name} does not contain a wait() statement",
                file=sys.stderr,
            )

    def ActorsNoDiscardByDefault(self) -> bool:
        return not self.DisableDiagnostics


@dataclass
class Token:
    Value: str
    Position: int = 0
    SourceLine: int = 0
    BraceDepth: int = 0
    ParenDepth: int = 0

    @property
    def IsWhitespace(self) -> bool:
        return (
            self.Value in (" ", "\n", "\r", "\r\n", "\t")
            or self.Value.startswith("//")
            or self.Value.startswith("/*")
        )

    def Assert(self, error: str, pred) -> "Token":
        if not pred(self):
            raise ActorCompilerError(self.SourceLine, error)
        return self

    def GetMatchingRangeIn(self, token_range: "TokenRange") -> "TokenRange":
        if self.Value == "<":
            sub = TokenRange(token_range.tokens, self.Position, token_range.End)
            gen = AngleBracketParser.NotInsideAngleBrackets(sub)
            next(gen, None)  # skip the "<"
            closing = next(gen, None)
            if closing is None:
                raise ActorCompilerError(self.SourceLine, "Syntax error: Unmatched <")
            return TokenRange(token_range.tokens, self.Position + 1, closing.Position)
        if self.Value == "[":
            sub = TokenRange(token_range.tokens, self.Position, token_range.End)
            gen = BracketParser.NotInsideBrackets(sub)
            next(gen, None)  # skip the "["
            closing = next(gen, None)
            if closing is None:
                raise ActorCompilerError(self.SourceLine, "Syntax error: Unmatched [")
            return TokenRange(token_range.tokens, self.Position + 1, closing.Position)

        pairs = {"(": ")", ")": "(", "{": "}", "}": "{"}
        if self.Value not in pairs:
            raise RuntimeError("Can't match this token")
        pred = (
            (lambda t: t.Value != ")" or t.ParenDepth != self.ParenDepth)
            if self.Value == "("
            else (
                (lambda t: t.Value != "(" or t.ParenDepth != self.ParenDepth)
                if self.Value == ")"
                else (
                    (lambda t: t.Value != "}" or t.BraceDepth != self.BraceDepth)
                    if self.Value == "{"
                    else (lambda t: t.Value != "{" or t.BraceDepth != self.BraceDepth)
                )
            )
        )
        direction = 1 if self.Value in ("(", "{") else -1
        if direction == -1:
            rng = token_range.Range(token_range.Begin, self.Position).RevTakeWhile(pred)
            if rng.Begin == token_range.Begin:
                raise ActorCompilerError(
                    self.SourceLine, f"Syntax error: Unmatched {self.Value}"
                )
            return rng
        rng = token_range.Range(self.Position + 1, token_range.End).TakeWhile(pred)
        if rng.End == token_range.End:
            raise ActorCompilerError(
                self.SourceLine, f"Syntax error: Unmatched {self.Value}"
            )
        return rng


class TokenRange:
    def __init__(self, tokens: List[Token], begin: int, end: int) -> None:
        if begin > end:
            raise RuntimeError("Invalid TokenRange")
        self.tokens = tokens
        self.beginPos = begin
        self.endPos = end

    @property
    def Begin(self) -> int:
        return self.beginPos

    @property
    def End(self) -> int:
        return self.endPos

    def __iter__(self) -> Iterator[Token]:
        for i in range(self.beginPos, self.endPos):
            yield self.tokens[i]

    def First(self, predicate=None) -> Token:
        if self.beginPos == self.endPos:
            raise RuntimeError("Empty TokenRange")
        if predicate is None:
            return self.tokens[self.beginPos]
        for t in self:
            if predicate(t):
                return t
        raise RuntimeError("Matching token not found")

    def Last(self, predicate=None) -> Token:
        if self.beginPos == self.endPos:
            raise RuntimeError("Empty TokenRange")
        if predicate is None:
            return self.tokens[self.endPos - 1]
        for i in range(self.endPos - 1, self.beginPos - 1, -1):
            if predicate(self.tokens[i]):
                return self.tokens[i]
        raise RuntimeError("Matching token not found")

    def Skip(self, count: int) -> "TokenRange":
        return TokenRange(self.tokens, self.beginPos + count, self.endPos)

    def Consume(self, value_or_error, predicate=None) -> "TokenRange":
        if predicate is None:
            self.First().Assert(
                f"Expected {value_or_error}", lambda t: t.Value == value_or_error
            )
        else:
            self.First().Assert(value_or_error, predicate)
        return self.Skip(1)

    def SkipWhile(self, predicate) -> "TokenRange":
        e = self.beginPos
        while e < self.endPos and predicate(self.tokens[e]):
            e += 1
        return TokenRange(self.tokens, e, self.endPos)

    def TakeWhile(self, predicate) -> "TokenRange":
        e = self.beginPos
        while e < self.endPos and predicate(self.tokens[e]):
            e += 1
        return TokenRange(self.tokens, self.beginPos, e)

    def RevTakeWhile(self, predicate) -> "TokenRange":
        e = self.endPos - 1
        while e >= self.beginPos and predicate(self.tokens[e]):
            e -= 1
        return TokenRange(self.tokens, e + 1, self.endPos)

    def RevSkipWhile(self, predicate) -> "TokenRange":
        e = self.endPos - 1
        while e >= self.beginPos and predicate(self.tokens[e]):
            e -= 1
        return TokenRange(self.tokens, self.beginPos, e + 1)

    def Range(self, begin: int, end: int) -> "TokenRange":
        return TokenRange(self.tokens, begin, end)

    def IsEmpty(self) -> bool:
        return self.beginPos == self.endPos

    def Length(self) -> int:
        return self.endPos - self.beginPos

    def All(self, predicate) -> bool:
        return all(predicate(t) for t in self)

    def Any(self, predicate) -> bool:
        return any(predicate(t) for t in self)


class BracketParser:
    @staticmethod
    def NotInsideBrackets(tokens: Iterable[Token]) -> Iterator[Token]:
        bracket_depth = 0
        base_pd = None
        for tok in tokens:
            if base_pd is None:
                base_pd = tok.ParenDepth
            if tok.ParenDepth == base_pd and tok.Value == "]":
                bracket_depth -= 1
            if bracket_depth == 0:
                yield tok
            if tok.ParenDepth == base_pd and tok.Value == "[":
                bracket_depth += 1


class AngleBracketParser:
    @staticmethod
    def NotInsideAngleBrackets(tokens: Iterable[Token]) -> Iterator[Token]:
        angle_depth = 0
        base_pd = None
        for tok in tokens:
            if base_pd is None:
                base_pd = tok.ParenDepth
            if tok.ParenDepth == base_pd and tok.Value == ">":
                angle_depth -= 1
            if angle_depth == 0:
                yield tok
            if tok.ParenDepth == base_pd and tok.Value == "<":
                angle_depth += 1


class ActorParser:
    tokenExpressions = [
        r"\{",
        r"\}",
        r"\(",
        r"\)",
        r"\[",
        r"\]",
        r"//[^\n]*",
        r"/[*]([*][^/]|[^*])*[*]/",
        r"'(\\.|[^\'\n])*'",
        r'"(\\.|[^"\n])*"',
        r"[a-zA-Z_][a-zA-Z_0-9]*",
        r"\r\n",
        r"\n",
        r"::",
        r":",
        r"#[a-z]*",
        r".",
    ]

    identifierPattern = re.compile(r"^[a-zA-Z_][a-zA-Z_0-9]*$")

    def __init__(
        self,
        text: str,
        sourceFile: str,
        errorMessagePolicy: ErrorMessagePolicy,
        generateProbes: bool,
    ) -> None:
        self.sourceFile = sourceFile
        self.errorMessagePolicy = errorMessagePolicy
        self.generateProbes = generateProbes
        self.tokens = [Token(Value=t) for t in self.Tokenize(text)]
        self.LineNumbersEnabled = True
        self.uidObjects: Dict[Tuple[int, int], str] = {}
        self.TokenArray = self.tokens
        self.CountParens()

    def Tokenize(self, text: str) -> List[str]:
        regexes = [re.compile(pattern, re.S) for pattern in self.tokenExpressions]
        pos = 0
        tokens: List[str] = []
        while pos < len(text):
            for regex in regexes:
                m = regex.match(text, pos)
                if m:
                    tokens.append(m.group(0))
                    pos += len(m.group(0))
                    break
            else:
                raise RuntimeError(f"Can't tokenize! {pos}")
        return tokens

    def CountParens(self) -> None:
        brace_depth = 0
        paren_depth = 0
        line_count = 1
        last_paren = None
        last_brace = None
        for i, token in enumerate(self.tokens):
            value = token.Value
            if value == "}":
                brace_depth -= 1
            elif value == ")":
                paren_depth -= 1
            elif value == "\r\n":
                line_count += 1
            elif value == "\n":
                line_count += 1

            if brace_depth < 0:
                raise ActorCompilerError(line_count, "Mismatched braces")
            if paren_depth < 0:
                raise ActorCompilerError(line_count, "Mismatched parenthesis")

            token.Position = i
            token.SourceLine = line_count
            token.BraceDepth = brace_depth
            token.ParenDepth = paren_depth

            if value.startswith("/*"):
                line_count += value.count("\n")
            if value == "{":
                brace_depth += 1
                if brace_depth == 1:
                    last_brace = token
            elif value == "(":
                paren_depth += 1
                if paren_depth == 1:
                    last_paren = token
        if brace_depth != 0:
            raise ActorCompilerError(
                last_brace.SourceLine if last_brace else line_count, "Unmatched brace"
            )
        if paren_depth != 0:
            raise ActorCompilerError(
                last_paren.SourceLine if last_paren else line_count,
                "Unmatched parenthesis",
            )

    def Write(self, writer: io.TextIOBase, destFileName: str) -> None:
        ActorCompiler.usedClassNames.clear()
        writer.write("#define POST_ACTOR_COMPILER 1\n")
        outLine = 1
        if self.LineNumbersEnabled:
            writer.write(f'#line {self.tokens[0].SourceLine} "{self.sourceFile}"\n')
            outLine += 1
        inBlocks = 0
        classContextStack: List[Tuple[str, int]] = []
        i = 0
        while i < len(self.tokens):
            tok = self.tokens[i]
            if tok.SourceLine == 0:
                raise RuntimeError("Invalid source line (0)")
            if tok.Value in ("ACTOR", "SWIFT_ACTOR", "TEST_CASE"):
                actor = self.ParseActor(i)
                end = self._parse_end
                if classContextStack:
                    actor.enclosingClass = "::".join(
                        name for name, _ in classContextStack
                    )
                actor_writer = io.StringIO()
                actorCompiler = ActorCompiler(
                    actor,
                    self.sourceFile,
                    inBlocks == 0,
                    self.LineNumbersEnabled,
                    self.generateProbes,
                )
                actorCompiler.Write(actor_writer)
                for key, value in actorCompiler.uidObjects.items():
                    self.uidObjects.setdefault(key, value)
                actor_lines = actor_writer.getvalue().split("\n")
                hasLineNumber = False
                hadLineNumber = True
                for line in actor_lines:
                    if self.LineNumbersEnabled:
                        isLine = "#line" in line
                        if isLine:
                            hadLineNumber = True
                        if not isLine and not hasLineNumber and hadLineNumber:
                            writer.write(
                                '\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} "{1}"\n'.format(
                                    outLine + 1, destFileName
                                )
                            )
                            outLine += 1
                            hadLineNumber = False
                        hasLineNumber = isLine
                    writer.write(line.rstrip("\n\r") + "\n")
                    outLine += 1
                i = end
                if i < len(self.tokens) and self.LineNumbersEnabled:
                    writer.write(
                        f'#line {self.tokens[i].SourceLine} "{self.sourceFile}"\n'
                    )
                    outLine += 1
            elif tok.Value == "DESCR":
                descr, end = self.ParseDescr(i)
                descr_writer = io.StringIO()
                lines = DescrCompiler(descr, tok.BraceDepth).write(descr_writer)
                writer.write(descr_writer.getvalue())
                outLine += lines
                i = end
                if i < len(self.tokens) and self.LineNumbersEnabled:
                    writer.write(
                        f'#line {self.tokens[i].SourceLine} "{self.sourceFile}"\n'
                    )
                    outLine += 1
            elif tok.Value in ("class", "struct", "union"):
                writer.write(tok.Value)
                success, name = self.ParseClassContext(
                    self.range(i + 1, len(self.tokens))
                )
                if success:
                    classContextStack.append((name, inBlocks))
            else:
                if tok.Value == "{":
                    inBlocks += 1
                elif tok.Value == "}":
                    inBlocks -= 1
                    if classContextStack and classContextStack[-1][1] == inBlocks:
                        classContextStack.pop()
                writer.write(tok.Value)
                outLine += tok.Value.count("\n")
            i += 1

    # Parsing helpers

    def range(self, begin: int, end: int) -> TokenRange:
        return TokenRange(self.tokens, begin, end)

    def ParseActor(self, pos: int) -> Actor:
        token = self.tokens[pos]
        actor = Actor()
        actor.SourceLine = token.SourceLine
        toks = self.range(pos + 1, len(self.tokens))
        heading = toks.TakeWhile(lambda t: t.Value != "{")
        toSemicolon = toks.TakeWhile(lambda t: t.Value != ";")
        actor.isForwardDeclaration = toSemicolon.Length() < heading.Length()
        if actor.isForwardDeclaration:
            heading = toSemicolon
            if token.Value in ("ACTOR", "SWIFT_ACTOR"):
                self.ParseActorHeading(actor, heading)
            else:
                token.Assert("ACTOR expected!", lambda _: False)
            self._parse_end = heading.End + 1
        else:
            body = self.range(heading.End + 1, len(self.tokens)).TakeWhile(
                lambda t: t.BraceDepth > toks.First().BraceDepth
            )
            if token.Value in ("ACTOR", "SWIFT_ACTOR"):
                self.ParseActorHeading(actor, heading)
            elif token.Value == "TEST_CASE":
                self.ParseTestCaseHeading(actor, heading)
                actor.isTestCase = True
            else:
                token.Assert("ACTOR or TEST_CASE expected!", lambda _: False)
            actor.body = self.ParseCodeBlock(body)
            if not actor.body.containsWait():
                self.errorMessagePolicy.HandleActorWithoutWait(self.sourceFile, actor)
            self._parse_end = body.End + 1
        return actor

    def ParseDescr(self, pos: int) -> Tuple[Descr, int]:
        descr = Descr()
        toks = self.range(pos + 1, len(self.tokens))
        heading = toks.TakeWhile(lambda t: t.Value != "{")
        body = self.range(heading.End + 1, len(self.tokens)).TakeWhile(
            lambda t: t.BraceDepth > toks.First().BraceDepth or t.Value == ";"
        )
        self.ParseDescrHeading(descr, heading)
        descr.body = self.ParseDescrCodeBlock(body)
        end = body.End + 1
        return descr, end

    def ParseDescrHeading(self, descr: Descr, toks: TokenRange) -> None:
        nonWhitespace = lambda t: not t.IsWhitespace
        toks.First(nonWhitespace).Assert(
            "non-struct DESCR!", lambda t: t.Value == "struct"
        )
        toks = (
            toks.SkipWhile(lambda t: t.IsWhitespace)
            .Skip(1)
            .SkipWhile(lambda t: t.IsWhitespace)
        )
        colon = next((t for t in toks if t.Value == ":"), None)
        if colon:
            descr.superClassList = self.str(
                self.range(colon.Position + 1, toks.End)
            ).strip()
            toks = self.range(toks.Begin, colon.Position)
        descr.name = self.str(toks).strip()

    def ParseDescrCodeBlock(self, toks: TokenRange) -> List[Declaration]:
        declarations: List[Declaration] = []
        while True:
            delim = next((t for t in toks if t.Value == ";"), None)
            if delim is None:
                break
            pos = delim.Position + 1
            potential_comment = self.range(pos, toks.End).SkipWhile(
                lambda t: t.Value in ("\t", " ")
            )
            if (
                not potential_comment.IsEmpty()
                and potential_comment.First().Value.startswith("//")
            ):
                pos = potential_comment.First().Position + 1
            self.ParseDeclarationRange(self.range(toks.Begin, pos), declarations)
            toks = self.range(pos, toks.End)
        if not toks.All(lambda t: t.IsWhitespace):
            raise ActorCompilerError(
                toks.First(lambda t: not t.IsWhitespace).SourceLine,
                "Trailing unterminated statement in code block",
            )
        return declarations

    def ParseDeclarationRange(
        self, toks: TokenRange, declarations: List[Declaration]
    ) -> None:
        delim = toks.First(lambda t: t.Value == ";")
        nameRange = (
            self.range(toks.Begin, delim.Position)
            .RevSkipWhile(lambda t: t.IsWhitespace)
            .RevTakeWhile(lambda t: not t.IsWhitespace)
        )
        typeRange = self.range(toks.Begin, nameRange.Begin)
        commentRange = self.range(delim.Position + 1, toks.End)
        declarations.append(
            Declaration(
                name=self.str(nameRange).strip(),
                type=self.str(typeRange).strip(),
                comment=self.str(commentRange).strip().lstrip("/"),
            )
        )

    def ParseTestCaseHeading(self, actor: Actor, toks: TokenRange) -> None:
        actor.isStatic = True
        nonWhitespace = lambda t: not t.IsWhitespace
        paramRange = (
            toks.Last(nonWhitespace)
            .Assert(
                "Unexpected tokens after test case parameter list.",
                lambda t: t.Value == ")" and t.ParenDepth == toks.First().ParenDepth,
            )
            .GetMatchingRangeIn(toks)
        )
        actor.testCaseParameters = self.str(paramRange)
        actor.name = f"flowTestCase{toks.First().SourceLine}"
        actor.parameters = [
            VarDeclaration(
                name="params",
                type="UnitTestParameters",
                initializer="",
                initializerConstructorSyntax=False,
            )
        ]
        actor.returnType = "Void"

    def ParseActorHeading(self, actor: Actor, toks: TokenRange) -> None:
        nonWhitespace = lambda t: not t.IsWhitespace
        template = toks.First(nonWhitespace)
        if template.Value == "template":
            templateParams = (
                self.range(template.Position + 1, toks.End)
                .First(nonWhitespace)
                .Assert("Invalid template declaration", lambda t: t.Value == "<")
                .GetMatchingRangeIn(toks)
            )
            actor.templateFormals = [
                self.ParseVarDeclaration(p)
                for p in self.SplitParameterList(templateParams, ",")
            ]
            toks = self.range(templateParams.End + 1, toks.End)
        attribute = toks.First(nonWhitespace)
        while attribute.Value == "[":
            contents = attribute.GetMatchingRangeIn(toks)
            as_array = list(contents)
            if (
                len(as_array) < 2
                or as_array[0].Value != "["
                or as_array[-1].Value != "]"
            ):
                raise ActorCompilerError(
                    actor.SourceLine, "Invalid attribute: Expected [[...]]"
                )
            actor.attributes.append(
                "[" + self.str(self.NormalizeWhitespace(contents)) + "]"
            )
            toks = self.range(contents.End + 1, toks.End)
            attribute = toks.First(nonWhitespace)
        static_keyword = toks.First(nonWhitespace)
        if static_keyword.Value == "static":
            actor.isStatic = True
            toks = self.range(static_keyword.Position + 1, toks.End)
        uncancellable = toks.First(nonWhitespace)
        if uncancellable.Value == "UNCANCELLABLE":
            actor.SetUncancellable()
            toks = self.range(uncancellable.Position + 1, toks.End)
        paramRange = (
            toks.Last(nonWhitespace)
            .Assert(
                "Unexpected tokens after actor parameter list.",
                lambda t: t.Value == ")" and t.ParenDepth == toks.First().ParenDepth,
            )
            .GetMatchingRangeIn(toks)
        )
        actor.parameters = [
            self.ParseVarDeclaration(p)
            for p in self.SplitParameterList(paramRange, ",")
        ]
        nameToken = self.range(toks.Begin, paramRange.Begin - 1).Last(nonWhitespace)
        actor.name = nameToken.Value
        return_range = self.range(
            toks.First().Position + 1, nameToken.Position
        ).SkipWhile(lambda t: t.IsWhitespace)
        retToken = return_range.First()
        if retToken.Value == "Future":
            ofType = (
                return_range.Skip(1)
                .First(nonWhitespace)
                .Assert("Expected <", lambda tok: tok.Value == "<")
                .GetMatchingRangeIn(return_range)
            )
            actor.returnType = self.str(self.NormalizeWhitespace(ofType))
            toks = self.range(ofType.End + 1, return_range.End)
        elif retToken.Value == "void":
            actor.returnType = None
            toks = return_range.Skip(1)
        else:
            raise ActorCompilerError(
                actor.SourceLine, "Actor apparently does not return Future<T>"
            )
        toks = toks.SkipWhile(lambda t: t.IsWhitespace)
        if not toks.IsEmpty():
            if toks.Last().Value == "::":
                actor.nameSpace = self.str(self.range(toks.Begin, toks.End - 1))
            else:
                raise ActorCompilerError(
                    actor.SourceLine,
                    "Unrecognized tokens preceding parameter list in actor declaration",
                )
        if (
            self.errorMessagePolicy.ActorsNoDiscardByDefault()
            and "[[flow_allow_discard]]" not in actor.attributes
        ):
            if actor.IsCancellable():
                actor.attributes.append("[[nodiscard]]")
        known_flow_attributes = {"[[flow_allow_discard]]"}
        for flow_attribute in [a for a in actor.attributes if a.startswith("[[flow_")]:
            if flow_attribute not in known_flow_attributes:
                raise ActorCompilerError(
                    actor.SourceLine, f"Unknown flow attribute {flow_attribute}"
                )
        actor.attributes = [a for a in actor.attributes if not a.startswith("[[flow_")]

    def ParseVarDeclaration(self, tokens: TokenRange) -> VarDeclaration:
        name, typeRange, initializer, constructorSyntax = self.ParseDeclaration(tokens)
        return VarDeclaration(
            name=name.Value,
            type=self.str(self.NormalizeWhitespace(typeRange)),
            initializer=(
                ""
                if initializer is None
                else self.str(self.NormalizeWhitespace(initializer))
            ),
            initializerConstructorSyntax=constructorSyntax,
        )

    def ParseDeclaration(self, tokens: TokenRange):
        nonWhitespace = lambda t: not t.IsWhitespace
        initializer = None
        beforeInitializer = tokens
        constructorSyntax = False
        equals = next(
            (
                t
                for t in AngleBracketParser.NotInsideAngleBrackets(tokens)
                if t.Value == "=" and t.ParenDepth == tokens.First().ParenDepth
            ),
            None,
        )
        if equals:
            beforeInitializer = self.range(tokens.Begin, equals.Position)
            initializer = self.range(equals.Position + 1, tokens.End)
        else:
            paren = next(
                (
                    t
                    for t in AngleBracketParser.NotInsideAngleBrackets(tokens)
                    if t.Value == "("
                ),
                None,
            )
            if paren:
                constructorSyntax = True
                beforeInitializer = self.range(tokens.Begin, paren.Position)
                initializer = self.range(paren.Position + 1, tokens.End).TakeWhile(
                    lambda t, p=paren.ParenDepth: t.ParenDepth > p
                )
            else:
                brace = next(
                    (
                        t
                        for t in AngleBracketParser.NotInsideAngleBrackets(tokens)
                        if t.Value == "{"
                    ),
                    None,
                )
                if brace:
                    raise ActorCompilerError(
                        brace.SourceLine,
                        "Uniform initialization syntax is not currently supported for state variables (use '(' instead of '}' ?)",
                    )
        name = beforeInitializer.Last(nonWhitespace)
        if beforeInitializer.Begin == name.Position:
            raise ActorCompilerError(
                beforeInitializer.First().SourceLine, "Declaration has no type."
            )
        typeRange = self.range(beforeInitializer.Begin, name.Position)
        return name, typeRange, initializer, constructorSyntax

    def NormalizeWhitespace(self, tokens: Iterable[Token]) -> Iterable[Token]:
        inWhitespace = False
        leading = True
        for tok in tokens:
            if not tok.IsWhitespace:
                if inWhitespace and not leading:
                    yield Token(Value=" ")
                inWhitespace = False
                yield tok
                leading = False
            else:
                inWhitespace = True

    def SplitParameterList(
        self, toks: TokenRange, delimiter: str
    ) -> Iterable[TokenRange]:
        if toks.Begin == toks.End:
            return []
        ranges: List[TokenRange] = []
        while True:
            comma = next(
                (
                    t
                    for t in AngleBracketParser.NotInsideAngleBrackets(toks)
                    if t.Value == delimiter and t.ParenDepth == toks.First().ParenDepth
                ),
                None,
            )
            if comma is None:
                break
            ranges.append(self.range(toks.Begin, comma.Position))
            toks = self.range(comma.Position + 1, toks.End)
        ranges.append(toks)
        return ranges

    def ParseLoopStatement(self, toks: TokenRange) -> LoopStatement:
        return LoopStatement(body=self.ParseCompoundStatement(toks.Consume("loop")))

    def ParseChooseStatement(self, toks: TokenRange) -> ChooseStatement:
        return ChooseStatement(body=self.ParseCompoundStatement(toks.Consume("choose")))

    def ParseWhenStatement(self, toks: TokenRange) -> WhenStatement:
        expr = (
            toks.Consume("when")
            .SkipWhile(lambda t: t.IsWhitespace)
            .First()
            .Assert("Expected (", lambda t: t.Value == "(")
            .GetMatchingRangeIn(toks)
            .SkipWhile(lambda t: t.IsWhitespace)
        )
        return WhenStatement(
            wait=self.ParseWaitStatement(expr),
            body=self.ParseCompoundStatement(self.range(expr.End + 1, toks.End)),
        )

    def ParseStateDeclaration(self, toks: TokenRange) -> StateDeclarationStatement:
        toks = toks.Consume("state").RevSkipWhile(lambda t: t.Value == ";")
        return StateDeclarationStatement(decl=self.ParseVarDeclaration(toks))

    def ParseReturnStatement(self, toks: TokenRange) -> ReturnStatement:
        toks = toks.Consume("return").RevSkipWhile(lambda t: t.Value == ";")
        return ReturnStatement(expression=self.str(self.NormalizeWhitespace(toks)))

    def ParseThrowStatement(self, toks: TokenRange) -> ThrowStatement:
        toks = toks.Consume("throw").RevSkipWhile(lambda t: t.Value == ";")
        return ThrowStatement(expression=self.str(self.NormalizeWhitespace(toks)))

    def ParseWaitStatement(self, toks: TokenRange) -> WaitStatement:
        ws = WaitStatement()
        ws.FirstSourceLine = toks.First().SourceLine
        if toks.First().Value == "state":
            ws.resultIsState = True
            toks = toks.Consume("state")
        initializer = None
        if toks.First().Value in ("wait", "waitNext"):
            initializer = toks.RevSkipWhile(lambda t: t.Value == ";")
            ws.result = VarDeclaration(
                name="_",
                type="Void",
                initializer="",
                initializerConstructorSyntax=False,
            )
        else:
            name, typeRange, initializer, constructorSyntax = self.ParseDeclaration(
                toks.RevSkipWhile(lambda t: t.Value == ";")
            )
            type_str = self.str(self.NormalizeWhitespace(typeRange))
            if type_str == "Void":
                raise ActorCompilerError(
                    ws.FirstSourceLine,
                    "Assigning the result of a Void wait is not allowed.  Just use a standalone wait statement.",
                )
            ws.result = VarDeclaration(
                name=name.Value,
                type=self.str(self.NormalizeWhitespace(typeRange)),
                initializer="",
                initializerConstructorSyntax=False,
            )
        if initializer is None:
            raise ActorCompilerError(
                ws.FirstSourceLine,
                "Wait statement must be a declaration or standalone statement",
            )
        waitParams = (
            initializer.SkipWhile(lambda t: t.IsWhitespace)
            .Consume(
                "Statement contains a wait, but is not a valid wait statement or a supported compound statement.1",
                lambda t: True if t.Value in ("wait", "waitNext") else False,
            )
            .SkipWhile(lambda t: t.IsWhitespace)
            .First()
            .Assert("Expected (", lambda t: t.Value == "(")
            .GetMatchingRangeIn(initializer)
        )
        if (
            not self.range(waitParams.End, initializer.End)
            .Consume(")")
            .All(lambda t: t.IsWhitespace)
        ):
            raise ActorCompilerError(
                toks.First().SourceLine,
                "Statement contains a wait, but is not a valid wait statement or a supported compound statement.2",
            )
        ws.futureExpression = self.str(self.NormalizeWhitespace(waitParams))
        ws.isWaitNext = "waitNext" in [t.Value for t in initializer]
        return ws

    def ParseWhileStatement(self, toks: TokenRange) -> WhileStatement:
        expr = (
            toks.Consume("while")
            .First(lambda t: not t.IsWhitespace)
            .Assert("Expected (", lambda t: t.Value == "(")
            .GetMatchingRangeIn(toks)
        )
        return WhileStatement(
            expression=self.str(self.NormalizeWhitespace(expr)),
            body=self.ParseCompoundStatement(self.range(expr.End + 1, toks.End)),
        )

    def ParseForStatement(self, toks: TokenRange) -> Statement:
        head = (
            toks.Consume("for")
            .First(lambda t: not t.IsWhitespace)
            .Assert("Expected (", lambda t: t.Value == "(")
            .GetMatchingRangeIn(toks)
        )
        delim = [
            t
            for t in head
            if t.ParenDepth == head.First().ParenDepth
            and t.BraceDepth == head.First().BraceDepth
            and t.Value == ";"
        ]
        if len(delim) == 2:
            init = self.range(head.Begin, delim[0].Position)
            cond = self.range(delim[0].Position + 1, delim[1].Position)
            next_expr = self.range(delim[1].Position + 1, head.End)
            body = self.range(head.End + 1, toks.End)
            return ForStatement(
                initExpression=self.str(self.NormalizeWhitespace(init)),
                condExpression=self.str(self.NormalizeWhitespace(cond)),
                nextExpression=self.str(self.NormalizeWhitespace(next_expr)),
                body=self.ParseCompoundStatement(body),
            )
        delim = [
            t
            for t in head
            if t.ParenDepth == head.First().ParenDepth
            and t.BraceDepth == head.First().BraceDepth
            and t.Value == ":"
        ]
        if len(delim) != 1:
            raise ActorCompilerError(
                head.First().SourceLine,
                "for statement must be 3-arg style or c++11 2-arg style",
            )
        return RangeForStatement(
            rangeExpression=self.str(
                self.NormalizeWhitespace(
                    self.range(delim[0].Position + 1, head.End).SkipWhile(
                        lambda t: t.IsWhitespace
                    )
                )
            ),
            rangeDecl=self.str(
                self.NormalizeWhitespace(
                    self.range(head.Begin, delim[0].Position - 1).SkipWhile(
                        lambda t: t.IsWhitespace
                    )
                )
            ),
            body=self.ParseCompoundStatement(self.range(head.End + 1, toks.End)),
        )

    def ParseIfStatement(self, toks: TokenRange) -> IfStatement:
        toks = toks.Consume("if").SkipWhile(lambda t: t.IsWhitespace)
        constexpr = toks.First().Value == "constexpr"
        if constexpr:
            toks = toks.Consume("constexpr").SkipWhile(lambda t: t.IsWhitespace)
        expr = (
            toks.First(lambda t: not t.IsWhitespace)
            .Assert("Expected (", lambda t: t.Value == "(")
            .GetMatchingRangeIn(toks)
        )
        return IfStatement(
            expression=self.str(self.NormalizeWhitespace(expr)),
            constexpr=constexpr,
            ifBody=self.ParseCompoundStatement(self.range(expr.End + 1, toks.End)),
        )

    def ParseElseStatement(self, toks: TokenRange, prev: Statement) -> None:
        if_stmt = prev
        while isinstance(if_stmt, IfStatement) and if_stmt.elseBody is not None:
            if_stmt = if_stmt.elseBody
        if not isinstance(if_stmt, IfStatement):
            raise ActorCompilerError(
                toks.First().SourceLine, "else without matching if"
            )
        if_stmt.elseBody = self.ParseCompoundStatement(toks.Consume("else"))

    def ParseTryStatement(self, toks: TokenRange) -> TryStatement:
        return TryStatement(
            tryBody=self.ParseCompoundStatement(toks.Consume("try")),
            catches=[],
        )

    def ParseCatchStatement(self, toks: TokenRange, prev: Statement) -> None:
        if not isinstance(prev, TryStatement):
            raise ActorCompilerError(
                toks.First().SourceLine, "catch without matching try"
            )
        expr = (
            toks.Consume("catch")
            .First(lambda t: not t.IsWhitespace)
            .Assert("Expected (", lambda t: t.Value == "(")
            .GetMatchingRangeIn(toks)
        )
        prev.catches.append(
            TryStatement.Catch(
                expression=self.str(self.NormalizeWhitespace(expr)),
                body=self.ParseCompoundStatement(self.range(expr.End + 1, toks.End)),
                FirstSourceLine=expr.First().SourceLine,
            )
        )

    IllegalKeywords = {"goto", "do", "finally", "__if_exists", "__if_not_exists"}

    def ParseCompoundStatement(self, toks: TokenRange) -> Statement:
        nonWhitespace = lambda t: not t.IsWhitespace
        first = toks.First(nonWhitespace)
        if first.Value == "{":
            inBraces = first.GetMatchingRangeIn(toks)
            if (
                not self.range(inBraces.End, toks.End)
                .Consume("}")
                .All(lambda t: t.IsWhitespace)
            ):
                raise ActorCompilerError(
                    inBraces.Last().SourceLine,
                    "Unexpected tokens after compound statement",
                )
            return self.ParseCodeBlock(inBraces)
        statements: List[Statement] = []
        self.ParseStatement(toks.Skip(1), statements)
        return statements[0]

    def ParseCodeBlock(self, toks: TokenRange) -> CodeBlock:
        statements: List[Statement] = []
        while True:
            delim = next(
                (
                    t
                    for t in toks
                    if t.ParenDepth == toks.First().ParenDepth
                    and t.BraceDepth == toks.First().BraceDepth
                    and t.Value in (";", "}")
                ),
                None,
            )
            if delim is None:
                break
            self.ParseStatement(self.range(toks.Begin, delim.Position + 1), statements)
            toks = self.range(delim.Position + 1, toks.End)
        if not toks.All(lambda t: t.IsWhitespace):
            raise ActorCompilerError(
                toks.First(lambda t: not t.IsWhitespace).SourceLine,
                "Trailing unterminated statement in code block",
            )
        return CodeBlock(statements=statements)

    def ParseStatement(self, toks: TokenRange, statements: List[Statement]) -> None:
        nonWhitespace = lambda t: not t.IsWhitespace
        toks = toks.SkipWhile(lambda t: t.IsWhitespace)

        def Add(stmt: Statement) -> None:
            stmt.FirstSourceLine = toks.First().SourceLine
            statements.append(stmt)

        first_val = toks.First().Value
        if first_val == "loop":
            Add(self.ParseLoopStatement(toks))
        elif first_val == "while":
            Add(self.ParseWhileStatement(toks))
        elif first_val == "for":
            Add(self.ParseForStatement(toks))
        elif first_val == "break":
            Add(BreakStatement())
        elif first_val == "continue":
            Add(ContinueStatement())
        elif first_val == "return":
            Add(self.ParseReturnStatement(toks))
        elif first_val == "{":
            Add(self.ParseCompoundStatement(toks))
        elif first_val == "if":
            Add(self.ParseIfStatement(toks))
        elif first_val == "else":
            self.ParseElseStatement(toks, statements[-1])
        elif first_val == "choose":
            Add(self.ParseChooseStatement(toks))
        elif first_val == "when":
            Add(self.ParseWhenStatement(toks))
        elif first_val == "try":
            Add(self.ParseTryStatement(toks))
        elif first_val == "catch":
            self.ParseCatchStatement(toks, statements[-1])
        elif first_val == "throw":
            Add(self.ParseThrowStatement(toks))
        else:
            if first_val in self.IllegalKeywords:
                raise ActorCompilerError(
                    toks.First().SourceLine,
                    f"Statement '{first_val}' not supported in actors.",
                )
            if any(t.Value in ("wait", "waitNext") for t in toks):
                Add(self.ParseWaitStatement(toks))
            elif first_val == "state":
                Add(self.ParseStateDeclaration(toks))
            elif first_val == "switch" and any(t.Value == "return" for t in toks):
                raise ActorCompilerError(
                    toks.First().SourceLine,
                    "Unsupported compound statement containing return.",
                )
            elif first_val.startswith("#"):
                raise ActorCompilerError(
                    toks.First().SourceLine,
                    f'Found "{first_val}". Preprocessor directives are not supported within ACTORs',
                )
            else:
                cleaned = toks.RevSkipWhile(lambda t: t.Value == ";")
                if any(nonWhitespace(t) for t in cleaned):
                    Add(
                        PlainOldCodeStatement(
                            code=self.str(self.NormalizeWhitespace(cleaned)) + ";"
                        )
                    )

    def ParseClassContext(self, toks: TokenRange) -> Tuple[bool, str]:
        name = ""
        if toks.Begin == toks.End:
            return False, name

        nonWhitespace = lambda t: not t.IsWhitespace
        while True:
            first = toks.First(nonWhitespace)
            if first.Value == "[":
                contents = first.GetMatchingRangeIn(toks)
                toks = self.range(contents.End + 1, toks.End)
            elif first.Value == "alignas":
                toks = self.range(first.Position + 1, toks.End)
                first = toks.First(nonWhitespace)
                first.Assert("Expected ( after alignas", lambda t: t.Value == "(")
                contents = first.GetMatchingRangeIn(toks)
                toks = self.range(contents.End + 1, toks.End)
            else:
                break

        first = toks.First(nonWhitespace)
        if not self.identifierPattern.match(first.Value):
            return False, name

        while True:
            first.Assert(
                "Expected identifier", lambda t: self.identifierPattern.match(t.Value)
            )
            name += first.Value
            toks = self.range(first.Position + 1, toks.End)
            next_token = toks.First(nonWhitespace)
            if next_token.Value == "::":
                name += "::"
                toks = toks.SkipWhile(lambda t: t.IsWhitespace).Skip(1)
            else:
                break
            first = toks.First(nonWhitespace)

        toks = toks.SkipWhile(
            lambda t: t.IsWhitespace or t.Value in ("final", "explicit")
        )
        first = toks.First(nonWhitespace)
        if first.Value in (":", "{"):
            return True, name
        return False, ""

    def str(self, tokens: Iterable[Token]) -> str:
        return "".join(tok.Value for tok in tokens)

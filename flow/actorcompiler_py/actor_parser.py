from __future__ import annotations

import io
import re
import sys
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from .errors import ActorCompilerError
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
        self.disable_diagnostics = False

    def handle_actor_without_wait(self, source_file: str, actor: Actor) -> None:
        if not self.disable_diagnostics and not actor.is_test_case:
            print(
                f"{source_file}:{actor.source_line}: warning: ACTOR {actor.name} does not contain a wait() statement",
                file=sys.stderr,
            )

    def actors_no_discard_by_default(self) -> bool:
        return not self.disable_diagnostics


@dataclass
class Token:
    value: str
    position: int = 0
    source_line: int = 0
    brace_depth: int = 0
    paren_depth: int = 0

    @property
    def is_whitespace(self) -> bool:
        return (
            self.value in (" ", "\n", "\r", "\r\n", "\t")
            or self.value.startswith("//")
            or self.value.startswith("/*")
        )

    def ensure(self, error: str, pred) -> "Token":
        if not pred(self):
            raise ActorCompilerError(self.source_line, error)
        return self

    def get_matching_range_in(self, token_range: "TokenRange") -> "TokenRange":
        if self.value == "<":
            sub = TokenRange(token_range.tokens, self.position, token_range.end_pos)
            gen = AngleBracketParser.not_inside_angle_brackets(sub)
            next(gen, None)  # skip the "<"
            closing = next(gen, None)
            if closing is None:
                raise ActorCompilerError(self.source_line, "Syntax error: Unmatched <")
            return TokenRange(token_range.tokens, self.position + 1, closing.position)
        if self.value == "[":
            sub = TokenRange(token_range.tokens, self.position, token_range.end_pos)
            gen = BracketParser.not_inside_brackets(sub)
            next(gen, None)  # skip the "["
            closing = next(gen, None)
            if closing is None:
                raise ActorCompilerError(self.source_line, "Syntax error: Unmatched [")
            return TokenRange(token_range.tokens, self.position + 1, closing.position)

        pairs = {"(": ")", ")": "(", "{": "}", "}": "{"}
        if self.value not in pairs:
            raise RuntimeError("Can't match this token")
        pred = (
            (lambda t: t.value != ")" or t.paren_depth != self.paren_depth)
            if self.value == "("
            else (
                (lambda t: t.value != "(" or t.paren_depth != self.paren_depth)
                if self.value == ")"
                else (
                    (lambda t: t.value != "}" or t.brace_depth != self.brace_depth)
                    if self.value == "{"
                    else (lambda t: t.value != "{" or t.brace_depth != self.brace_depth)
                )
            )
        )
        direction = 1 if self.value in ("(", "{") else -1
        if direction == -1:
            rng = token_range.sub_range(token_range.begin_pos, self.position).Revtake_while(pred)
            if rng.begin_pos == token_range.begin_pos:
                raise ActorCompilerError(
                    self.source_line, f"Syntax error: Unmatched {self.value}"
                )
            return rng
        rng = token_range.sub_range(self.position + 1, token_range.end_pos).take_while(pred)
        if rng.end_pos == token_range.end_pos:
            raise ActorCompilerError(
                self.source_line, f"Syntax error: Unmatched {self.value}"
            )
        return rng


class TokenRange:
    def __init__(self, tokens: List[Token], begin: int, end: int) -> None:
        if begin > end:
            raise RuntimeError("Invalid TokenRange")
        self.tokens = tokens
        self.begin_pos = begin
        self.end_pos = end

    def __iter__(self) -> Iterator[Token]:
        for i in range(self.begin_pos, self.end_pos):
            yield self.tokens[i]

    def first(self, predicate=None) -> Token:
        if self.begin_pos == self.end_pos:
            raise RuntimeError("Empty TokenRange")
        if predicate is None:
            return self.tokens[self.begin_pos]
        for t in self:
            if predicate(t):
                return t
        raise RuntimeError("Matching token not found")

    def last(self, predicate=None) -> Token:
        if self.begin_pos == self.end_pos:
            raise RuntimeError("Empty TokenRange")
        if predicate is None:
            return self.tokens[self.end_pos - 1]
        for i in range(self.end_pos - 1, self.begin_pos - 1, -1):
            if predicate(self.tokens[i]):
                return self.tokens[i]
        raise RuntimeError("Matching token not found")

    def skip(self, count: int) -> "TokenRange":
        return TokenRange(self.tokens, self.begin_pos + count, self.end_pos)

    def consume(self, value_or_error, predicate=None) -> "TokenRange":
        if predicate is None:
            self.first().ensure(
                f"Expected {value_or_error}", lambda t: t.value == value_or_error
            )
        else:
            self.first().ensure(value_or_error, predicate)
        return self.skip(1)

    def skipWhile(self, predicate) -> "TokenRange":
        e = self.begin_pos
        while e < self.end_pos and predicate(self.tokens[e]):
            e += 1
        return TokenRange(self.tokens, e, self.end_pos)

    def take_while(self, predicate) -> "TokenRange":
        e = self.begin_pos
        while e < self.end_pos and predicate(self.tokens[e]):
            e += 1
        return TokenRange(self.tokens, self.begin_pos, e)

    def Revtake_while(self, predicate) -> "TokenRange":
        e = self.end_pos - 1
        while e >= self.begin_pos and predicate(self.tokens[e]):
            e -= 1
        return TokenRange(self.tokens, e + 1, self.end_pos)

    def Revskip_while(self, predicate) -> "TokenRange":
        e = self.end_pos - 1
        while e >= self.begin_pos and predicate(self.tokens[e]):
            e -= 1
        return TokenRange(self.tokens, self.begin_pos, e + 1)

    def sub_range(self, begin: int, end: int) -> "TokenRange":
        return TokenRange(self.tokens, begin, end)

    def is_empty(self) -> bool:
        return self.begin_pos == self.end_pos

    def length(self) -> int:
        return self.end_pos - self.begin_pos

    def all_match(self, predicate) -> bool:
        return all(predicate(t) for t in self)

    def any_match(self, predicate) -> bool:
        return any(predicate(t) for t in self)


class BracketParser:
    @staticmethod
    def not_inside_brackets(tokens: Iterable[Token]) -> Iterator[Token]:
        bracket_depth = 0
        base_pd = None
        for tok in tokens:
            if base_pd is None:
                base_pd = tok.paren_depth
            if tok.paren_depth == base_pd and tok.value == "]":
                bracket_depth -= 1
            if bracket_depth == 0:
                yield tok
            if tok.paren_depth == base_pd and tok.value == "[":
                bracket_depth += 1


class AngleBracketParser:
    @staticmethod
    def not_inside_angle_brackets(tokens: Iterable[Token]) -> Iterator[Token]:
        angle_depth = 0
        base_pd = None
        for tok in tokens:
            if base_pd is None:
                base_pd = tok.paren_depth
            if tok.paren_depth == base_pd and tok.value == ">":
                angle_depth -= 1
            if angle_depth == 0:
                yield tok
            if tok.paren_depth == base_pd and tok.value == "<":
                angle_depth += 1


class ActorParser:
    token_expressions = [
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

    identifier_pattern = re.compile(r"^[a-zA-Z_][a-zA-Z_0-9]*$")

    def __init__(
        self,
        text: str,
        source_file: str,
        error_message_policy: ErrorMessagePolicy,
        generate_probes: bool,
    ) -> None:
        self.source_file = source_file
        self.error_message_policy = error_message_policy
        self.generate_probes = generate_probes
        self.tokens = [Token(value=t) for t in self.tokenize(text)]
        self.line_numbers_enabled = True
        self.uid_objects: Dict[Tuple[int, int], str] = {}
        self.token_array = self.tokens
        self.count_parens()

    def tokenize(self, text: str) -> List[str]:
        regexes = [re.compile(pattern, re.S) for pattern in self.token_expressions]
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

    def count_parens(self) -> None:
        brace_depth = 0
        paren_depth = 0
        line_count = 1
        last_paren = None
        last_brace = None
        for i, token in enumerate(self.tokens):
            value = token.value
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

            token.position = i
            token.source_line = line_count
            token.brace_depth = brace_depth
            token.paren_depth = paren_depth

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
                last_brace.source_line if last_brace else line_count, "Unmatched brace"
            )
        if paren_depth != 0:
            raise ActorCompilerError(
                last_paren.source_line if last_paren else line_count,
                "Unmatched parenthesis",
            )

    def write(self, writer: io.TextIOBase, destFileName: str) -> None:
        ActorCompiler.used_class_names.clear()
        writer.write("#define POST_ACTOR_COMPILER 1\n")
        outLine = 1
        if self.line_numbers_enabled:
            writer.write(f'#line {self.tokens[0].source_line} "{self.source_file}"\n')
            outLine += 1
        inBlocks = 0
        classContextStack: List[Tuple[str, int]] = []
        i = 0
        while i < len(self.tokens):
            tok = self.tokens[i]
            if tok.source_line == 0:
                raise RuntimeError("Invalid source line (0)")
            if tok.value in ("ACTOR", "SWIFT_ACTOR", "TEST_CASE"):
                actor = self.parse_actor(i)
                end = self._parse_end
                if classContextStack:
                    actor.enclosing_class = "::".join(
                        name for name, _ in classContextStack
                    )
                actor_writer = io.StringIO()
                actorCompiler = ActorCompiler(
                    actor,
                    self.source_file,
                    inBlocks == 0,
                    self.line_numbers_enabled,
                    self.generate_probes,
                )
                actorCompiler.write(actor_writer)
                for key, value in actorCompiler.uid_objects.items():
                    self.uid_objects.setdefault(key, value)
                actor_lines = actor_writer.getvalue().split("\n")
                hasLineNumber = False
                hadLineNumber = True
                for line in actor_lines:
                    if self.line_numbers_enabled:
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
                if i < len(self.tokens) and self.line_numbers_enabled:
                    writer.write(
                        f'#line {self.tokens[i].source_line} "{self.source_file}"\n'
                    )
                    outLine += 1
            elif tok.value == "DESCR":
                descr, end = self.parse_descr(i)
                descr_writer = io.StringIO()
                lines = DescrCompiler(descr, tok.brace_depth).write(descr_writer)
                writer.write(descr_writer.getvalue())
                outLine += lines
                i = end
                if i < len(self.tokens) and self.line_numbers_enabled:
                    writer.write(
                        f'#line {self.tokens[i].source_line} "{self.source_file}"\n'
                    )
                    outLine += 1
            elif tok.value in ("class", "struct", "union"):
                writer.write(tok.value)
                success, name = self.parse_class_context(
                    self.range(i + 1, len(self.tokens))
                )
                if success:
                    classContextStack.append((name, inBlocks))
            else:
                if tok.value == "{":
                    inBlocks += 1
                elif tok.value == "}":
                    inBlocks -= 1
                    if classContextStack and classContextStack[-1][1] == inBlocks:
                        classContextStack.pop()
                writer.write(tok.value)
                outLine += tok.value.count("\n")
            i += 1

    # Parsing helpers

    def range(self, begin: int, end: int) -> TokenRange:
        return TokenRange(self.tokens, begin, end)

    def parse_actor(self, pos: int) -> Actor:
        token = self.tokens[pos]
        actor = Actor()
        actor.source_line = token.source_line
        toks = self.range(pos + 1, len(self.tokens))
        heading = toks.take_while(lambda t: t.value != "{")
        toSemicolon = toks.take_while(lambda t: t.value != ";")
        actor.is_forward_declaration = toSemicolon.length() < heading.length()
        if actor.is_forward_declaration:
            heading = toSemicolon
            if token.value in ("ACTOR", "SWIFT_ACTOR"):
                self.parse_actorHeading(actor, heading)
            else:
                token.ensure("ACTOR expected!", lambda _: False)
            self._parse_end = heading.end_pos + 1
        else:
            body = self.range(heading.end_pos + 1, len(self.tokens)).take_while(
                lambda t: t.brace_depth > toks.first().brace_depth
            )
            if token.value in ("ACTOR", "SWIFT_ACTOR"):
                self.parse_actorHeading(actor, heading)
            elif token.value == "TEST_CASE":
                self.parse_test_caseHeading(actor, heading)
                actor.is_test_case = True
            else:
                token.ensure("ACTOR or TEST_CASE expected!", lambda _: False)
            actor.body = self.parse_code_block(body)
            if not actor.body.contains_wait():
                self.error_message_policy.handle_actor_without_wait(self.source_file, actor)
            self._parse_end = body.end_pos + 1
        return actor

    def parse_descr(self, pos: int) -> Tuple[Descr, int]:
        descr = Descr()
        toks = self.range(pos + 1, len(self.tokens))
        heading = toks.take_while(lambda t: t.value != "{")
        body = self.range(heading.end_pos + 1, len(self.tokens)).take_while(
            lambda t: t.brace_depth > toks.first().brace_depth or t.value == ";"
        )
        self.parse_descrHeading(descr, heading)
        descr.body = self.parse_descrCodeBlock(body)
        end = body.end_pos + 1
        return descr, end

    def parse_descrHeading(self, descr: Descr, toks: TokenRange) -> None:
        nonWhitespace = lambda t: not t.is_whitespace
        toks.first(nonWhitespace).ensure(
            "non-struct DESCR!", lambda t: t.value == "struct"
        )
        toks = (
            toks.skipWhile(lambda t: t.is_whitespace)
            .skip(1)
            .skipWhile(lambda t: t.is_whitespace)
        )
        colon = next((t for t in toks if t.value == ":"), None)
        if colon:
            descr.super_class_list = self.str(
                self.range(colon.position + 1, toks.end_pos)
            ).strip()
            toks = self.range(toks.begin_pos, colon.position)
        descr.name = self.str(toks).strip()

    def parse_descrCodeBlock(self, toks: TokenRange) -> List[Declaration]:
        declarations: List[Declaration] = []
        while True:
            delim = next((t for t in toks if t.value == ";"), None)
            if delim is None:
                break
            pos = delim.position + 1
            potential_comment = self.range(pos, toks.end_pos).skipWhile(
                lambda t: t.value in ("\t", " ")
            )
            if (
                not potential_comment.is_empty()
                and potential_comment.first().value.startswith("//")
            ):
                pos = potential_comment.first().position + 1
            self.parse_declarationRange(self.range(toks.begin_pos, pos), declarations)
            toks = self.range(pos, toks.end_pos)
        if not toks.all_match(lambda t: t.is_whitespace):
            raise ActorCompilerError(
                toks.first(lambda t: not t.is_whitespace).source_line,
                "Trailing unterminated statement in code block",
            )
        return declarations

    def parse_declarationRange(
        self, toks: TokenRange, declarations: List[Declaration]
    ) -> None:
        delim = toks.first(lambda t: t.value == ";")
        nameRange = (
            self.range(toks.begin_pos, delim.position)
            .Revskip_while(lambda t: t.is_whitespace)
            .Revtake_while(lambda t: not t.is_whitespace)
        )
        typeRange = self.range(toks.begin_pos, nameRange.begin_pos)
        commentRange = self.range(delim.position + 1, toks.end_pos)
        declarations.append(
            Declaration(
                name=self.str(nameRange).strip(),
                type=self.str(typeRange).strip(),
                comment=self.str(commentRange).strip().lstrip("/"),
            )
        )

    def parse_test_caseHeading(self, actor: Actor, toks: TokenRange) -> None:
        actor.is_static = True
        nonWhitespace = lambda t: not t.is_whitespace
        paramRange = (
            toks.last(nonWhitespace)
            .ensure(
                "Unexpected tokens after test case parameter list.",
                lambda t: t.value == ")" and t.paren_depth == toks.first().paren_depth,
            )
            .get_matching_range_in(toks)
        )
        actor.test_case_parameters = self.str(paramRange)
        actor.name = f"flowTestCase{toks.first().source_line}"
        actor.parameters = [
            VarDeclaration(
                name="params",
                type="UnitTestParameters",
                initializer="",
                initializer_constructor_syntax=False,
            )
        ]
        actor.return_type = "Void"

    def parse_actorHeading(self, actor: Actor, toks: TokenRange) -> None:
        nonWhitespace = lambda t: not t.is_whitespace
        template = toks.first(nonWhitespace)
        if template.value == "template":
            templateParams = (
                self.range(template.position + 1, toks.end_pos)
                .first(nonWhitespace)
                .ensure("Invalid template declaration", lambda t: t.value == "<")
                .get_matching_range_in(toks)
            )
            actor.template_formals = [
                self.parse_var_declaration(p)
                for p in self.split_parameter_list(templateParams, ",")
            ]
            toks = self.range(templateParams.end_pos + 1, toks.end_pos)
        attribute = toks.first(nonWhitespace)
        while attribute.value == "[":
            contents = attribute.get_matching_range_in(toks)
            as_array = list(contents)
            if (
                len(as_array) < 2
                or as_array[0].value != "["
                or as_array[-1].value != "]"
            ):
                raise ActorCompilerError(
                    actor.source_line, "Invalid attribute: Expected [[...]]"
                )
            actor.attributes.append(
                "[" + self.str(self.normalize_whitespace(contents)) + "]"
            )
            toks = self.range(contents.end_pos + 1, toks.end_pos)
            attribute = toks.first(nonWhitespace)
        static_keyword = toks.first(nonWhitespace)
        if static_keyword.value == "static":
            actor.is_static = True
            toks = self.range(static_keyword.position + 1, toks.end_pos)
        uncancellable = toks.first(nonWhitespace)
        if uncancellable.value == "UNCANCELLABLE":
            actor.set_uncancellable()
            toks = self.range(uncancellable.position + 1, toks.end_pos)
        paramRange = (
            toks.last(nonWhitespace)
            .ensure(
                "Unexpected tokens after actor parameter list.",
                lambda t: t.value == ")" and t.paren_depth == toks.first().paren_depth,
            )
            .get_matching_range_in(toks)
        )
        actor.parameters = [
            self.parse_var_declaration(p)
            for p in self.split_parameter_list(paramRange, ",")
        ]
        nameToken = self.range(toks.begin_pos, paramRange.begin_pos - 1).last(nonWhitespace)
        actor.name = nameToken.value
        return_range = self.range(
            toks.first().position + 1, nameToken.position
        ).skipWhile(lambda t: t.is_whitespace)
        retToken = return_range.first()
        if retToken.value == "Future":
            ofType = (
                return_range.skip(1)
                .first(nonWhitespace)
                .ensure("Expected <", lambda tok: tok.value == "<")
                .get_matching_range_in(return_range)
            )
            actor.return_type = self.str(self.normalize_whitespace(ofType))
            toks = self.range(ofType.end_pos + 1, return_range.end_pos)
        elif retToken.value == "void":
            actor.return_type = None
            toks = return_range.skip(1)
        else:
            raise ActorCompilerError(
                actor.source_line, "Actor apparently does not return Future<T>"
            )
        toks = toks.skipWhile(lambda t: t.is_whitespace)
        if not toks.is_empty():
            if toks.last().value == "::":
                actor.name_space = self.str(self.range(toks.begin_pos, toks.end_pos - 1))
            else:
                raise ActorCompilerError(
                    actor.source_line,
                    "Unrecognized tokens preceding parameter list in actor declaration",
                )
        if (
            self.error_message_policy.actors_no_discard_by_default()
            and "[[flow_allow_discard]]" not in actor.attributes
        ):
            if actor.is_cancellable():
                actor.attributes.append("[[nodiscard]]")
        known_flow_attributes = {"[[flow_allow_discard]]"}
        for flow_attribute in [a for a in actor.attributes if a.startswith("[[flow_")]:
            if flow_attribute not in known_flow_attributes:
                raise ActorCompilerError(
                    actor.source_line, f"Unknown flow attribute {flow_attribute}"
                )
        actor.attributes = [a for a in actor.attributes if not a.startswith("[[flow_")]

    def parse_var_declaration(self, tokens: TokenRange) -> VarDeclaration:
        name, typeRange, initializer, constructorSyntax = self.parse_declaration(tokens)
        return VarDeclaration(
            name=name.value,
            type=self.str(self.normalize_whitespace(typeRange)),
            initializer=(
                ""
                if initializer is None
                else self.str(self.normalize_whitespace(initializer))
            ),
            initializer_constructor_syntax=constructorSyntax,
        )

    def parse_declaration(self, tokens: TokenRange):
        nonWhitespace = lambda t: not t.is_whitespace
        initializer = None
        beforeInitializer = tokens
        constructorSyntax = False
        equals = next(
            (
                t
                for t in AngleBracketParser.not_inside_angle_brackets(tokens)
                if t.value == "=" and t.paren_depth == tokens.first().paren_depth
            ),
            None,
        )
        if equals:
            beforeInitializer = self.range(tokens.begin_pos, equals.position)
            initializer = self.range(equals.position + 1, tokens.end_pos)
        else:
            paren = next(
                (
                    t
                    for t in AngleBracketParser.not_inside_angle_brackets(tokens)
                    if t.value == "("
                ),
                None,
            )
            if paren:
                constructorSyntax = True
                beforeInitializer = self.range(tokens.begin_pos, paren.position)
                initializer = self.range(paren.position + 1, tokens.end_pos).take_while(
                    lambda t, p=paren.paren_depth: t.paren_depth > p
                )
            else:
                brace = next(
                    (
                        t
                        for t in AngleBracketParser.not_inside_angle_brackets(tokens)
                        if t.value == "{"
                    ),
                    None,
                )
                if brace:
                    raise ActorCompilerError(
                        brace.source_line,
                        "Uniform initialization syntax is not currently supported for state variables (use '(' instead of '}' ?)",
                    )
        name = beforeInitializer.last(nonWhitespace)
        if beforeInitializer.begin_pos == name.position:
            raise ActorCompilerError(
                beforeInitializer.first().source_line, "Declaration has no type."
            )
        typeRange = self.range(beforeInitializer.begin_pos, name.position)
        return name, typeRange, initializer, constructorSyntax

    def normalize_whitespace(self, tokens: Iterable[Token]) -> Iterable[Token]:
        inWhitespace = False
        leading = True
        for tok in tokens:
            if not tok.is_whitespace:
                if inWhitespace and not leading:
                    yield Token(value=" ")
                inWhitespace = False
                yield tok
                leading = False
            else:
                inWhitespace = True

    def split_parameter_list(
        self, toks: TokenRange, delimiter: str
    ) -> Iterable[TokenRange]:
        if toks.begin_pos == toks.end_pos:
            return []
        ranges: List[TokenRange] = []
        while True:
            comma = next(
                (
                    t
                    for t in AngleBracketParser.not_inside_angle_brackets(toks)
                    if t.value == delimiter and t.paren_depth == toks.first().paren_depth
                ),
                None,
            )
            if comma is None:
                break
            ranges.append(self.range(toks.begin_pos, comma.position))
            toks = self.range(comma.position + 1, toks.end_pos)
        ranges.append(toks)
        return ranges

    def parse_loop_statement(self, toks: TokenRange) -> LoopStatement:
        return LoopStatement(body=self.parse_compound_statement(toks.consume("loop")))

    def parse_choose_statement(self, toks: TokenRange) -> ChooseStatement:
        return ChooseStatement(body=self.parse_compound_statement(toks.consume("choose")))

    def parse_when_statement(self, toks: TokenRange) -> WhenStatement:
        expr = (
            toks.consume("when")
            .skipWhile(lambda t: t.is_whitespace)
            .first()
            .ensure("Expected (", lambda t: t.value == "(")
            .get_matching_range_in(toks)
            .skipWhile(lambda t: t.is_whitespace)
        )
        return WhenStatement(
            wait=self.parse_wait_statement(expr),
            body=self.parse_compound_statement(self.range(expr.end_pos + 1, toks.end_pos)),
        )

    def parse_state_declaration(self, toks: TokenRange) -> StateDeclarationStatement:
        toks = toks.consume("state").Revskip_while(lambda t: t.value == ";")
        return StateDeclarationStatement(decl=self.parse_var_declaration(toks))

    def parse_return_statement(self, toks: TokenRange) -> ReturnStatement:
        toks = toks.consume("return").Revskip_while(lambda t: t.value == ";")
        return ReturnStatement(expression=self.str(self.normalize_whitespace(toks)))

    def parse_throw_statement(self, toks: TokenRange) -> ThrowStatement:
        toks = toks.consume("throw").Revskip_while(lambda t: t.value == ";")
        return ThrowStatement(expression=self.str(self.normalize_whitespace(toks)))

    def parse_wait_statement(self, toks: TokenRange) -> WaitStatement:
        ws = WaitStatement()
        ws.first_source_line = toks.first().source_line
        if toks.first().value == "state":
            ws.result_is_state = True
            toks = toks.consume("state")
        initializer = None
        if toks.first().value in ("wait", "waitNext"):
            initializer = toks.Revskip_while(lambda t: t.value == ";")
            ws.result = VarDeclaration(
                name="_",
                type="Void",
                initializer="",
                initializer_constructor_syntax=False,
            )
        else:
            name, typeRange, initializer, constructorSyntax = self.parse_declaration(
                toks.Revskip_while(lambda t: t.value == ";")
            )
            type_str = self.str(self.normalize_whitespace(typeRange))
            if type_str == "Void":
                raise ActorCompilerError(
                    ws.first_source_line,
                    "Assigning the result of a Void wait is not allowed.  Just use a standalone wait statement.",
                )
            ws.result = VarDeclaration(
                name=name.value,
                type=self.str(self.normalize_whitespace(typeRange)),
                initializer="",
                initializer_constructor_syntax=False,
            )
        if initializer is None:
            raise ActorCompilerError(
                ws.first_source_line,
                "Wait statement must be a declaration or standalone statement",
            )
        waitParams = (
            initializer.skipWhile(lambda t: t.is_whitespace)
            .consume(
                "Statement contains a wait, but is not a valid wait statement or a supported compound statement.1",
                lambda t: True if t.value in ("wait", "waitNext") else False,
            )
            .skipWhile(lambda t: t.is_whitespace)
            .first()
            .ensure("Expected (", lambda t: t.value == "(")
            .get_matching_range_in(initializer)
        )
        if (
            not self.range(waitParams.end_pos, initializer.end_pos)
            .consume(")")
            .all_match(lambda t: t.is_whitespace)
        ):
            raise ActorCompilerError(
                toks.first().source_line,
                "Statement contains a wait, but is not a valid wait statement or a supported compound statement.2",
            )
        ws.future_expression = self.str(self.normalize_whitespace(waitParams))
        ws.is_wait_next = "waitNext" in [t.value for t in initializer]
        return ws

    def parse_while_statement(self, toks: TokenRange) -> WhileStatement:
        expr = (
            toks.consume("while")
            .first(lambda t: not t.is_whitespace)
            .ensure("Expected (", lambda t: t.value == "(")
            .get_matching_range_in(toks)
        )
        return WhileStatement(
            expression=self.str(self.normalize_whitespace(expr)),
            body=self.parse_compound_statement(self.range(expr.end_pos + 1, toks.end_pos)),
        )

    def parse_for_statement(self, toks: TokenRange) -> Statement:
        head = (
            toks.consume("for")
            .first(lambda t: not t.is_whitespace)
            .ensure("Expected (", lambda t: t.value == "(")
            .get_matching_range_in(toks)
        )
        delim = [
            t
            for t in head
            if t.paren_depth == head.first().paren_depth
            and t.brace_depth == head.first().brace_depth
            and t.value == ";"
        ]
        if len(delim) == 2:
            init = self.range(head.begin_pos, delim[0].position)
            cond = self.range(delim[0].position + 1, delim[1].position)
            next_expr = self.range(delim[1].position + 1, head.end_pos)
            body = self.range(head.end_pos + 1, toks.end_pos)
            return ForStatement(
                init_expression=self.str(self.normalize_whitespace(init)),
                cond_expression=self.str(self.normalize_whitespace(cond)),
                next_expression=self.str(self.normalize_whitespace(next_expr)),
                body=self.parse_compound_statement(body),
            )
        delim = [
            t
            for t in head
            if t.paren_depth == head.first().paren_depth
            and t.brace_depth == head.first().brace_depth
            and t.value == ":"
        ]
        if len(delim) != 1:
            raise ActorCompilerError(
                head.first().source_line,
                "for statement must be 3-arg style or c++11 2-arg style",
            )
        return RangeForStatement(
            range_expression=self.str(
                self.normalize_whitespace(
                    self.range(delim[0].position + 1, head.end_pos).skipWhile(
                        lambda t: t.is_whitespace
                    )
                )
            ),
            range_decl=self.str(
                self.normalize_whitespace(
                    self.range(head.begin_pos, delim[0].position - 1).skipWhile(
                        lambda t: t.is_whitespace
                    )
                )
            ),
            body=self.parse_compound_statement(self.range(head.end_pos + 1, toks.end_pos)),
        )

    def parse_if_statement(self, toks: TokenRange) -> IfStatement:
        toks = toks.consume("if").skipWhile(lambda t: t.is_whitespace)
        constexpr = toks.first().value == "constexpr"
        if constexpr:
            toks = toks.consume("constexpr").skipWhile(lambda t: t.is_whitespace)
        expr = (
            toks.first(lambda t: not t.is_whitespace)
            .ensure("Expected (", lambda t: t.value == "(")
            .get_matching_range_in(toks)
        )
        return IfStatement(
            expression=self.str(self.normalize_whitespace(expr)),
            constexpr=constexpr,
            if_body=self.parse_compound_statement(self.range(expr.end_pos + 1, toks.end_pos)),
        )

    def parse_else_statement(self, toks: TokenRange, prev: Statement) -> None:
        if_stmt = prev
        while isinstance(if_stmt, IfStatement) and if_stmt.else_body is not None:
            if_stmt = if_stmt.else_body
        if not isinstance(if_stmt, IfStatement):
            raise ActorCompilerError(
                toks.first().source_line, "else without matching if"
            )
        if_stmt.else_body = self.parse_compound_statement(toks.consume("else"))

    def parse_try_statement(self, toks: TokenRange) -> TryStatement:
        return TryStatement(
            try_body=self.parse_compound_statement(toks.consume("try")),
            catches=[],
        )

    def parse_catch_statement(self, toks: TokenRange, prev: Statement) -> None:
        if not isinstance(prev, TryStatement):
            raise ActorCompilerError(
                toks.first().source_line, "catch without matching try"
            )
        expr = (
            toks.consume("catch")
            .first(lambda t: not t.is_whitespace)
            .ensure("Expected (", lambda t: t.value == "(")
            .get_matching_range_in(toks)
        )
        prev.catches.append(
            TryStatement.Catch(
                expression=self.str(self.normalize_whitespace(expr)),
                body=self.parse_compound_statement(self.range(expr.end_pos + 1, toks.end_pos)),
                first_source_line=expr.first().source_line,
            )
        )

    illegal_keywords = {"goto", "do", "finally", "__if_exists", "__if_not_exists"}

    def parse_compound_statement(self, toks: TokenRange) -> Statement:
        nonWhitespace = lambda t: not t.is_whitespace
        first = toks.first(nonWhitespace)
        if first.value == "{":
            inBraces = first.get_matching_range_in(toks)
            if (
                not self.range(inBraces.end_pos, toks.end_pos)
                .consume("}")
                .all_match(lambda t: t.is_whitespace)
            ):
                raise ActorCompilerError(
                    inBraces.last().source_line,
                    "Unexpected tokens after compound statement",
                )
            return self.parse_code_block(inBraces)
        statements: List[Statement] = []
        self.parse_statement(toks.skip(1), statements)
        return statements[0]

    def parse_code_block(self, toks: TokenRange) -> CodeBlock:
        statements: List[Statement] = []
        while True:
            delim = next(
                (
                    t
                    for t in toks
                    if t.paren_depth == toks.first().paren_depth
                    and t.brace_depth == toks.first().brace_depth
                    and t.value in (";", "}")
                ),
                None,
            )
            if delim is None:
                break
            self.parse_statement(self.range(toks.begin_pos, delim.position + 1), statements)
            toks = self.range(delim.position + 1, toks.end_pos)
        if not toks.all_match(lambda t: t.is_whitespace):
            raise ActorCompilerError(
                toks.first(lambda t: not t.is_whitespace).source_line,
                "Trailing unterminated statement in code block",
            )
        return CodeBlock(statements=statements)

    def parse_statement(self, toks: TokenRange, statements: List[Statement]) -> None:
        nonWhitespace = lambda t: not t.is_whitespace
        toks = toks.skipWhile(lambda t: t.is_whitespace)

        def Add(stmt: Statement) -> None:
            stmt.first_source_line = toks.first().source_line
            statements.append(stmt)

        first_val = toks.first().value
        if first_val == "loop":
            Add(self.parse_loop_statement(toks))
        elif first_val == "while":
            Add(self.parse_while_statement(toks))
        elif first_val == "for":
            Add(self.parse_for_statement(toks))
        elif first_val == "break":
            Add(BreakStatement())
        elif first_val == "continue":
            Add(ContinueStatement())
        elif first_val == "return":
            Add(self.parse_return_statement(toks))
        elif first_val == "{":
            Add(self.parse_compound_statement(toks))
        elif first_val == "if":
            Add(self.parse_if_statement(toks))
        elif first_val == "else":
            self.parse_else_statement(toks, statements[-1])
        elif first_val == "choose":
            Add(self.parse_choose_statement(toks))
        elif first_val == "when":
            Add(self.parse_when_statement(toks))
        elif first_val == "try":
            Add(self.parse_try_statement(toks))
        elif first_val == "catch":
            self.parse_catch_statement(toks, statements[-1])
        elif first_val == "throw":
            Add(self.parse_throw_statement(toks))
        else:
            if first_val in self.illegal_keywords:
                raise ActorCompilerError(
                    toks.first().source_line,
                    f"Statement '{first_val}' not supported in actors.",
                )
            if any(t.value in ("wait", "waitNext") for t in toks):
                Add(self.parse_wait_statement(toks))
            elif first_val == "state":
                Add(self.parse_state_declaration(toks))
            elif first_val == "switch" and any(t.value == "return" for t in toks):
                raise ActorCompilerError(
                    toks.first().source_line,
                    "Unsupported compound statement containing return.",
                )
            elif first_val.startswith("#"):
                raise ActorCompilerError(
                    toks.first().source_line,
                    f'Found "{first_val}". Preprocessor directives are not supported within ACTORs',
                )
            else:
                cleaned = toks.Revskip_while(lambda t: t.value == ";")
                if any(nonWhitespace(t) for t in cleaned):
                    Add(
                        PlainOldCodeStatement(
                            code=self.str(self.normalize_whitespace(cleaned)) + ";"
                        )
                    )

    def parse_class_context(self, toks: TokenRange) -> Tuple[bool, str]:
        name = ""
        if toks.begin_pos == toks.end_pos:
            return False, name

        nonWhitespace = lambda t: not t.is_whitespace
        while True:
            first = toks.first(nonWhitespace)
            if first.value == "[":
                contents = first.get_matching_range_in(toks)
                toks = self.range(contents.end_pos + 1, toks.end_pos)
            elif first.value == "alignas":
                toks = self.range(first.position + 1, toks.end_pos)
                first = toks.first(nonWhitespace)
                first.ensure("Expected ( after alignas", lambda t: t.value == "(")
                contents = first.get_matching_range_in(toks)
                toks = self.range(contents.end_pos + 1, toks.end_pos)
            else:
                break

        first = toks.first(nonWhitespace)
        if not self.identifier_pattern.match(first.value):
            return False, name

        while True:
            first.ensure(
                "Expected identifier", lambda t: self.identifier_pattern.match(t.value)
            )
            name += first.value
            toks = self.range(first.position + 1, toks.end_pos)
            next_token = toks.first(nonWhitespace)
            if next_token.value == "::":
                name += "::"
                toks = toks.skipWhile(lambda t: t.is_whitespace).skip(1)
            else:
                break
            first = toks.first(nonWhitespace)

        toks = toks.skipWhile(
            lambda t: t.is_whitespace or t.value in ("final", "explicit")
        )
        first = toks.first(nonWhitespace)
        if first.value in (":", "{"):
            return True, name
        return False, ""

    def str(self, tokens: Iterable[Token]) -> str:
        return "".join(tok.value for tok in tokens)

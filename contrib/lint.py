#! /usr/bin/env python3

"""
lint.py

Checks for wait clauses in when context.

In the following code

loop choose {
    when(wait(Event1)) {
        wait(delay(1.0));
        do_sth1();
    }
    when(wait(Event2)) {
        do_sth2();
    }
}

if Event1 is triggered first, then the whole loop will wait for 1 second before
react to anything else. This blocks the code react to Event2 and might not be
what expected. This script checks for the issue.

libclang is required to call this code.
"""

import argparse
import dataclasses
import logging
import os.path
import pathlib
import sys

from typing import List, Generator, Union

import clang.cindex

logger = logging.getLogger("linter")


def _setup_args():
    parser = argparse.ArgumentParser("Check for wait in choose wait clauses")
    parser.add_argument("files", type=str, nargs="*", help="Files to be scanned")
    parser.add_argument("--debug", action="store_true", help="Debug logging")
    parser.add_argument(
        "--clang-args",
        type=str,
        nargs="*",
        help="Additional arguments to clang interface",
    )

    return parser.parse_args()


def _setup_logger(level=logging.INFO):
    logger.setLevel(level)

    handler = logging.StreamHandler()
    handler.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)


@dataclasses.dataclass
class LinterIssue:
    file_name: str
    line: int
    error: str

    def to_readable(self) -> str:
        return f"[{self.file_name}:{self.line}] {self.error}"


@dataclasses.dataclass
class Token:
    file_name: str
    line: int
    column: int
    kind: clang.cindex.TokenKind
    spelling: str


def tokenizer(
    source_file_path: pathlib.Path, clang_args: List[str]
) -> Generator[Token, None, None]:
    indexer = clang.cindex.Index.create()
    parsed = indexer.parse(source_file_path, args=clang_args)

    for item in parsed.cursor.get_tokens():
        token = Token(
            file_name=item.location.file.name,
            line=item.location.line,
            column=item.location.column,
            kind=item.kind,
            spelling=item.spelling,
        )
        yield token


NONE_SCOPE = None
WHEN_KEYWORD = "when"
SCOPING_KEYWORD = ["loop", WHEN_KEYWORD, "choose"]
WAIT_KEYWORD = "wait"
SCOPING_BEGIN_PUNCTUATION = "{"
SCOPING_END_PUNCTUATION = "}"


class _ScopeLinter:
    def __init__(self):
        self._scope_stack: List[Union[None, Token]] = [NONE_SCOPE]
        self._inside_when_clause: int = 0

    def _debug_token(self, token: Token, message: str):
        logger.debug(
            f"[{token.file_name}:{token.line},${token.column}] [Token: {token.spelling}] {message}"
        )

    def _is_scoping_keyword(self, token: Token) -> bool:
        return (
            token.kind == clang.cindex.TokenKind.IDENTIFIER
            and token.spelling in SCOPING_KEYWORD
        )

    def _is_when_keyword(self, token: Token) -> bool:
        return (
            token.kind == clang.cindex.TokenKind.IDENTIFIER
            and token.spelling == WHEN_KEYWORD
        )

    def _is_wait_keyword(self, token: Token) -> bool:
        return (
            token.kind == clang.cindex.TokenKind.IDENTIFIER
            and token.spelling == WAIT_KEYWORD
        )

    def _is_scope_begin(self, token: Token) -> bool:
        return (
            token.kind == clang.cindex.TokenKind.PUNCTUATION
            and token.spelling == SCOPING_BEGIN_PUNCTUATION
        )

    def _is_scope_end(self, token: Token) -> bool:
        return (
            token.kind == clang.cindex.TokenKind.PUNCTUATION
            and token.spelling == SCOPING_END_PUNCTUATION
        )

    def _exit_scope(self, token: Token) -> Union[LinterIssue, None]:
        if len(self._scope_stack) == 0 or self._scope_stack[-1] is NONE_SCOPE:
            return LinterIssue(
                token.file_name,
                token.line,
                f"Meet {SCOPING_END_PUNCTUATION} outside a scope",
            )

        start_token = self._scope_stack[-1]
        assert isinstance(start_token, Token)
        if start_token.spelling != SCOPING_BEGIN_PUNCTUATION:
            return LinterIssue(
                token.file_name,
                token.line,
                f"No {SCOPING_BEGIN_PUNCTUATION} matching {SCOPING_END_PUNCTUATION}, found {start_token.spelling}",
            )
        self._debug_token(
            start_token, f"Exitting scope starting from line {start_token.line}"
        )
        self._scope_stack.pop()

        while len(self._scope_stack) > 1:
            keyword_token = self._scope_stack[-1]
            assert isinstance(keyword_token, Token)
            if keyword_token.spelling in SCOPING_KEYWORD:
                self._debug_token(
                    keyword_token, f"Dropping keyword {keyword_token.spelling}"
                )
                if self._is_when_keyword(keyword_token):
                    self._inside_when_clause -= 1
                    assert self._inside_when_clause >= 0
                self._scope_stack.pop()
            else:
                break

    def accept(self, token: Token) -> Union[LinterIssue, None]:
        if self._is_scope_begin(token):
            self._debug_token(token, "Entering scope")
            self._scope_stack.append(token)
            return None
        elif self._is_scope_end(token):
            self._debug_token(token, "Exitting scope")
            return self._exit_scope(token)
        elif self._is_scoping_keyword(token):
            self._debug_token(token, "Keyword found")
            self._scope_stack.append(token)
            if self._is_when_keyword(token):
                self._inside_when_clause += 1
            return None
        elif self._is_wait_keyword(token):
            self._debug_token(token, "Wait found")
            if (
                self._inside_when_clause > 0
                and self._scope_stack[-1].spelling == SCOPING_BEGIN_PUNCTUATION
            ):
                return LinterIssue(
                    token.file_name, token.line, "Found wait inside when clause"
                )
            return None
        else:
            return None

    def finalize(self) -> Union[LinterIssue, None]:
        if len(self._scope_stack) != 1:
            return LinterIssue(
                "", -1, f"File terminated with scope depth {len(self._scope_stack)}"
            )
        return None


def lint(source_file_path: pathlib.Path, clang_args: List[str]) -> List[LinterIssue]:
    linter: _ScopeLinter = _ScopeLinter()
    issues: List[LinterIssue] = []

    def _accept_issue(result: Union[LinterIssue, None]):
        if result is not None:
            issues.append(result)

    for token in tokenizer(source_file_path, clang_args):
        _accept_issue(linter.accept(token))

    _accept_issue(linter.finalize())

    return issues


def _main():
    args = _setup_args()
    _setup_logger(level=logging.INFO if not args.debug else logging.DEBUG)

    num_files: int = len(args.files)
    logger.debug(f"Total {num_files} files being processed")
    for source_file in args.files:
        if not os.path.exists(source_file):
            logger.warn(f"{source_file} does not exist")
            continue
        if not os.path.isfile(source_file):
            logger.warn(f"{source_file} is not a file")
            continue

        print(f"Processing file {source_file}:")
        issues = lint(pathlib.Path(source_file), args.clang_args)
        if len(issues) == 0:
            continue
        for issue in issues:
            print(f"[{issue.line}] {issue.error}")


if __name__ == "__main__":
    sys.exit(_main())

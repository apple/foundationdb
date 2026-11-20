import argparse
import enum
import pathlib
import sys
import textwrap
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable, List, Optional


class Scope(enum.Enum):
    NetworkOption = "NetworkOption"
    DatabaseOption = "DatabaseOption"
    TransactionOption = "TransactionOption"
    StreamingMode = "StreamingMode"
    MutationType = "MutationType"
    ConflictRangeType = "ConflictRangeType"
    ErrorPredicate = "ErrorPredicate"

    @property
    def description(self) -> str:
        return {
            Scope.NetworkOption: "NET_OPTION",
            Scope.DatabaseOption: "DB_OPTION",
            Scope.TransactionOption: "TR_OPTION",
            Scope.StreamingMode: "STREAMING_MODE",
            Scope.MutationType: "MUTATION_TYPE",
            Scope.ConflictRangeType: "CONFLICT_RANGE_TYPE",
            Scope.ErrorPredicate: "ERROR_PREDICATE",
        }[self]


class ParamType(enum.Enum):
    NoneType = "None"
    String = "String"
    Int = "Int"
    Bytes = "Bytes"


@dataclass
class Option:
    scope: Scope
    name: str
    code: int
    param_type: ParamType
    param_desc: Optional[str]
    comment: str
    hidden: bool
    persistent: bool
    sensitive: bool
    default_for: int

    def parameter_comment(self) -> str:
        return "Option takes no parameter" if self.param_desc is None else f"({self.param_type.value}) {self.param_desc}"

    def is_deprecated(self) -> bool:
        return self.comment.startswith("Deprecated")


HEADER_NOTICE_C = textwrap.dedent(
    """
    #ifndef FDB_C_OPTIONS_G_H
    #define FDB_C_OPTIONS_G_H
    #pragma once

    /*
     * FoundationDB C API
     *
     * This source file is part of the FoundationDB open source project
     *
     * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
     *
     * Licensed under the Apache License, Version 2.0 (the 'License');
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an 'AS IS' BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     * Do not include this file directly.
     */
    """
).strip()

HEADER_NOTICE_CPP = """\
#ifndef FDBCLIENT_FDBOPTIONS_G_H
#define FDBCLIENT_FDBOPTIONS_G_H
#pragma once

#include "fdbclient/FDBOptions.h"
""".strip()

LICENSE_PY = textwrap.dedent(
    """
    # FoundationDB Python API
    # Copyright (c) 2013-2026 Apple Inc.

    # Permission is hereby granted, free of charge, to any person obtaining a copy
    # of this software and associated documentation files (the "Software"), to deal
    # in the Software without restriction, including without limitation the rights
    # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    # copies of the Software, and to permit persons to whom the Software is
    # furnished to do so, subject to the following conditions:

    # The above copyright notice and this permission notice shall be included in
    # all copies or substantial portions of the Software.

    # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    # THE SOFTWARE.

    import types
    """
).strip()

LICENSE_RB = textwrap.dedent(
    """
    # FoundationDB Ruby API
    # Copyright (c) 2013-2026 Apple Inc.

    # Permission is hereby granted, free of charge, to any person obtaining a copy
    # of this software and associated documentation files (the "Software"), to deal
    # in the Software without restriction, including without limitation the rights
    # to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    # copies of the Software, and to permit persons to whom the Software is
    # furnished to do so, subject to the following conditions:

    # The above copyright notice and this permission notice shall be included in
    # all copies or substantial portions of the Software.

    # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    # FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    # AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    # LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    # OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
    # THE SOFTWARE.

    # Documentation for this API can be found at
    # https://apple.github.io/foundationdb/api-ruby.html

    module FDB
    """
).strip()

JAVA_SCOPE_DOCS = {
    Scope.NetworkOption: "A set of options that can be set globally for the {@link FDB FoundationDB API}.",
    Scope.DatabaseOption: "A set of options that can be set on a {@link Database}.",
    Scope.TransactionOption: "A set of options that can be set on a {@link Transaction}.",
    Scope.StreamingMode: (
        "Options that control the way the Java binding performs range reads. These options can be passed to "
        "{@link Transaction#getRange(byte[], byte[], int, boolean, StreamingMode) Transaction.getRange(...)}."
    ),
    Scope.MutationType: (
        "A set of operations that can be performed atomically on a database. These are used as parameters to "
        "{@link Transaction#mutate(MutationType, byte[], byte[])}."
    ),
    Scope.ConflictRangeType: "Conflict range types used internally by the C API.",
    Scope.ErrorPredicate: "Error code predicates for binding writers and non-standard layer implementers.",
}

JAVA_ENUM_SCOPES = {Scope.StreamingMode, Scope.MutationType, Scope.ConflictRangeType}
JAVA_NONPUBLIC_SCOPES = {Scope.ConflictRangeType}

JAVA_FDB_EXCEPTION_HEADER = """\
package com.apple.foundationdb;

import com.apple.foundationdb.async.CloneableException;

/**
 * An Error from the native layers of FoundationDB.  Each {@code FDBException} sets
 *  the {@code message} of the underlying Java {@link Exception}. FDB exceptions expose
 *  a number of functions including, for example, {@link #isRetryable()} that
 *  evaluate predicates on the internal FDB error. Most clients should use those methods
 *  in order to implement special handling for certain errors if their application
 *  requires it.
 *
 * <p>
 * Errors in FDB should generally be retried if they match the {@link #isRetryable()}
 *  predicate. In addition, as with any distributed system, certain classes of errors
 *  may fail in such a way that it is unclear whether the transaction succeeded (they
 *  {@link #isMaybeCommitted() may be committed} or not). To handle these cases, clients
 *  are generally advised to make their database operations idempotent and to place
 *  their operations within retry loops. The FDB Java API provides some default retry loops
 *  within the {@link Database} interface. See the discussion within the documentation of
 *  {@link Database#runAsync(Function) Database.runAsync()} for more details.
 *
 * @see com.apple.foundationdb.Transaction#onError(Throwable) Transaction.onError()
 * @see com.apple.foundationdb.Database#runAsync(Function) Database.runAsync()
 */
public class FDBException extends RuntimeException implements CloneableException {
    private static final long serialVersionUID = 1L;
    private final int code;

    /**
     * A general constructor.  Not for use by client code.
     *
     * @param message error message of this exception
     * @param code internal FDB error code of this exception
     */
    public FDBException(String message, int code) {
        super(message);
        this.code = code;
    }

    /**
     * Gets the code for this error. A list of common errors codes
     *  are published <a href="/foundationdb/api-error-codes.html">elsewhere within
     *  our documentation</a>.
     *
     * @return the internal FDB error code
     */
    public int getCode() {
        return code;
    }

    /**
     * Determine if this {@code FDBException} represents a success code from the native layer.
     *
     * @return {@code true} if this error represents success, {@code false} otherwise
     */
    public boolean isSuccess() {
        return getCode() == 0;
    }

    @Override
    public Exception retargetClone() {
        FDBException exception = new FDBException(getMessage(), code);
        exception.initCause(this);
        return exception;
    }

"""

JAVA_ENUM_FOOTER = """
    private final int code;

    {0}(int code) {{
        this.code = code;
    }}

    /**
     * Gets the FoundationDB native-level constant code for a {{@code {0}}}.
     *
     * @return the native code for a FoundationDB {{@code {0}}} constant.
     */
    public int code() {{
        return this.code;
    }}
}}
"""


def parse_options(xml_path: pathlib.Path, binding: str) -> List[Option]:
    tree = ET.parse(xml_path)
    options_root = tree.getroot()
    options: List[Option] = []
    for scope_elem in options_root.findall("Scope"):
        scope = Scope(scope_elem.get("name"))
        for opt_elem in scope_elem.findall("Option"):
            disable_on = opt_elem.get("disableOn")
            if disable_on:
                if binding in [item.strip() for item in disable_on.split(",") if item.strip()]:
                    continue
            param_type = ParamType(opt_elem.get("paramType", "None"))
            options.append(
                Option(
                    scope=scope,
                    name=opt_elem.get("name"),
                    code=int(opt_elem.get("code")),
                    param_type=param_type,
                    param_desc=opt_elem.get("paramDescription"),
                    comment=opt_elem.get("description", ""),
                    hidden=opt_elem.get("hidden") == "true",
                    persistent=opt_elem.get("persistent") == "true",
                    sensitive=opt_elem.get("sensitive") == "true",
                    default_for=int(opt_elem.get("defaultFor", "-1")),
                )
            )
    return options


def indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line if line else prefix for line in text.split("\n"))


def c_option_line(opt: Option, indent_str: str, prefix: str) -> str:
    parameter_comment = ""
    if opt.scope.name.endswith("Option"):
        hidden_note = (
            "This is a hidden parameter and should not be used directly by applications."
            if opt.hidden
            else ""
        )
        parameter_comment = f"{indent_str}/* Parameter: {opt.parameter_comment()} {hidden_note}*/\n"
    return f"{indent_str}/* {opt.comment} */\n{parameter_comment}{indent_str}{prefix}{opt.name.upper()}={opt.code}"


def write_c(file_path: pathlib.Path, options: Iterable[Option]) -> None:
    options = list(options)
    with file_path.open("w", newline="\n") as handle:
        handle.write(HEADER_NOTICE_C + "\n\n")
        for scope in Scope:
            scoped = [o for o in options if o.scope is scope]
            if not scoped:
                scoped = [
                    Option(scope, "DUMMY_DO_NOT_USE", -1, ParamType.NoneType, None,
                           "This option is only a placeholder for C compatibility and should not be used", False, False, False, -1)
                ]
            prefix = f"FDB_{scope.description}_"
            handle.write("typedef enum {\n")
            handle.write(",\n\n".join(c_option_line(o, "    ", prefix) for o in scoped))
            handle.write(f"\n}} FDB{scope.name};\n\n")
        handle.write("#endif\n")


def write_cpp(base_path: pathlib.Path, options: Iterable[Option]) -> None:
    options = list(options)
    # Keep the ".g" stem for generated C++ option artifacts
    header_path = base_path.parent / (base_path.name + ".h")
    source_path = base_path.parent / (base_path.name + ".cpp")

    def option_info(opt: Option, indent_level: str, struct_name: str) -> str:
        return (
            f"{indent_level}ADD_OPTION_INFO({struct_name}, {opt.name.upper()}, \"{opt.name.upper()}\", "
            f"\"{opt.comment}\", \"{opt.parameter_comment()}\", "
            f"{str(opt.param_desc is not None).lower()}, {str(opt.hidden).lower()}, "
            f"{str(opt.persistent).lower()}, {str(opt.sensitive).lower()}, {opt.default_for}, "
            f"FDBOptionInfo::ParamType::{opt.param_type.value})"
        )

    with header_path.open("w", newline="\n") as header:
        header.write(HEADER_NOTICE_CPP + "\n\n")
        for scope in Scope:
            scoped = [o for o in options if o.scope is scope]
            header.write(f"struct FDB{scope.name}s {{\n")
            header.write("\tfriend class FDBOptionInfoMap<FDB{0}s>;\n\n".format(scope.name))
            header.write("\tenum Option : int {\n")
            header.write(",\n\n".join(c_option_line(o, "\t\t", "") for o in scoped))
            header.write("\n\t};\n\n")
            header.write(f"\tstatic FDBOptionInfoMap<FDB{scope.name}s> optionInfo;\n\n")
            header.write("private:\n")
            header.write("\tstatic void init();\n")
            header.write("};\n\n")
        header.write("\n#endif\n")

    with source_path.open("w", newline="\n") as source:
        source.write('#include "fdbclient/FDBOptions.g.h"\n\n')
        for scope in Scope:
            scoped = [o for o in options if o.scope is scope]
            source.write(f"FDBOptionInfoMap<FDB{scope.name}s> FDB{scope.name}s::optionInfo;\n\n")
            source.write(f"void FDB{scope.name}s::init() {{\n")
            source.write("\n".join(option_info(o, "\t", f"FDB{scope.name}s") for o in scoped))
            source.write("\n}\n\n")


def write_python(file_path: pathlib.Path, options: Iterable[Option]) -> None:
    type_map = {
        ParamType.NoneType: "type(None)",
        ParamType.Int: "type(0)",
        ParamType.String: "type('')",
        ParamType.Bytes: "type(b'')",
    }
    with file_path.open("w", newline="\n") as handle:
        handle.write(LICENSE_PY + "\n\n")
        for scope in Scope:
            scoped = [o for o in options if o.scope is scope and not o.hidden]
            handle.write(f"{scope.name} = {{\n")
            lines = []
            for o in scoped:
                param_desc = "None" if o.param_desc is None else f"\"{o.param_desc}\""
                lines.append(
                    f"    \"{o.name}\" : ({o.code}, \"{o.comment}\", {type_map[o.param_type]}, {param_desc}),"
                )
            handle.write("\n".join(lines))
            handle.write("\n}\n\n")


def write_ruby(file_path: pathlib.Path, options: Iterable[Option]) -> None:
    type_map = {
        ParamType.NoneType: "nil",
        ParamType.Int: "0",
        ParamType.String: "''",
        ParamType.Bytes: "''",
    }
    with file_path.open("w", newline="\n") as handle:
        handle.write(LICENSE_RB + "\n")
        for scope in Scope:
            # ErrorPredicate is emitted as an empty hash, for parity with the C# generator.
            scoped = [o for o in options if o.scope is scope and not o.hidden and scope is not Scope.ErrorPredicate]
            handle.write(f"  @@{scope.name} = {{\n")
            lines = []
            for o in scoped:
                param_desc = "nil" if o.param_desc is None else f"\"{o.param_desc}\""
                lines.append(
                    f"    \"{o.name.upper()}\" => [{o.code}, \"{o.comment}\", {type_map[o.param_type]}, {param_desc}],"
                )
            handle.write("\n".join(lines))
            handle.write("\n  }\n\n")
        handle.write("end\n")


def _java_comment(indent_tabs: int, text: str) -> str:
    tabs = "\t" * indent_tabs
    body = "".join(f"\n{tabs} * {line.strip()}" for line in text.split("\n"))
    return f"{tabs}/**{body}\n{tabs} */"


def _java_replace_ticks(text: str) -> str:
    parts = text.split("``")
    if len(parts) % 2 == 0:
        raise ValueError(f"No closing double tick in: {text}")
    return "".join(f"{{@code {p}}}" if i % 2 else p for i, p in enumerate(parts))


def _java_camel_case(name: str) -> str:
    return "".join(s[:1].upper() + s[1:] for s in name.split("_"))


def _java_member_comment(o: Option, extra: str = "") -> str:
    comment = o.comment if o.comment.endswith(".") else o.comment + "."
    if o.param_desc is not None:
        comment += "\n\n@param value " + o.param_desc
    return _java_comment(1, _java_replace_ticks(comment + extra)) + "\n"


def _java_class_comment(scope: Scope, scoped: List[Option]) -> str:
    doc = JAVA_SCOPE_DOCS[scope]
    if not scoped:
        doc += "\n\nThere are currently no options available."
    return _java_comment(0, doc) + "\n"


def write_java(output_dir: pathlib.Path, options: Iterable[Option]) -> None:
    # Unlike the other writers, the output is a directory: one .java file per scope.
    java_types = {
        ParamType.Int: "long",
        ParamType.Bytes: "byte[]",
        ParamType.String: "String",
    }
    options = list(options)
    output_dir.mkdir(parents=True, exist_ok=True)

    for scope in Scope:
        scoped = [o for o in options if o.scope is scope]
        out: List[str] = []

        if scope in JAVA_ENUM_SCOPES:
            class_name = scope.name
            visibility = "" if scope in JAVA_NONPUBLIC_SCOPES else "public "
            out.append("package com.apple.foundationdb;\n\n")
            out.append(_java_class_comment(scope, scoped))
            out.append(f"{visibility}enum {class_name} {{\n")
            entries = []
            for o in scoped:
                if o.hidden:
                    continue
                comment = o.comment or "Currently undocumented."
                if not comment.endswith("."):
                    comment += "."
                deprecated = "\t@Deprecated\n" if o.is_deprecated() else ""
                entries.append(
                    f"{_java_comment(1, _java_replace_ticks(comment))}\n{deprecated}\t{o.name.upper()}({o.code})"
                )
            out.append(",\n\n".join(entries) + ";\n")
            out.append(JAVA_ENUM_FOOTER.format(class_name))
        else:
            # Documented options are listed first; sorted() is stable.
            scoped.sort(key=lambda o: o.comment == "")
            if scope is Scope.ErrorPredicate:
                class_name = "FDBException"
                out.append(JAVA_FDB_EXCEPTION_HEADER)
            else:
                class_name = scope.name + "s"
                out.append("package com.apple.foundationdb;\n\n")
                out.append(_java_class_comment(scope, scoped))
                out.append(f"public class {class_name} extends OptionsSet {{\n")
                out.append(f"\tpublic {class_name}( OptionConsumer consumer ) {{ super(consumer); }}\n")
            for o in scoped:
                if o.hidden:
                    continue
                out.append("\n")
                if scope is Scope.ErrorPredicate:
                    if o.comment:
                        out.append(_java_member_comment(
                            o, f"\n\n@return {{@code true}} if this {{@code FDBException}} is {{@code {o.name}}}"
                        ))
                    if o.is_deprecated():
                        out.append("\t@Deprecated\n")
                    out.append(
                        f"\tpublic boolean is{_java_camel_case(o.name)}() "
                        f"{{ return FDB.evalErrorPredicate({o.code}, this.code); }}\n"
                    )
                else:
                    if o.comment:
                        out.append(_java_member_comment(o))
                    if o.is_deprecated():
                        out.append("\t@Deprecated\n")
                    setter = "set" + _java_camel_case(o.name)
                    if o.param_type is ParamType.NoneType:
                        out.append(f"\tpublic void {setter}() {{ setOption({o.code}); }}\n")
                    else:
                        out.append(
                            f"\tpublic void {setter}({java_types[o.param_type]} value) "
                            f"{{ setOption({o.code}, value); }}\n"
                        )
            out.append("}\n")

        with (output_dir / f"{class_name}.java").open("w", newline="\n") as handle:
            handle.write("".join(out))


WRITERS = {
    "c": write_c,
    "cpp": write_cpp,
    "python": write_python,
    "ruby": write_ruby,
    "java": write_java,
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input")
    parser.add_argument("lang", choices=WRITERS.keys())
    parser.add_argument("output")
    args = parser.parse_args()

    options = parse_options(pathlib.Path(args.input), args.lang)
    writer = WRITERS[args.lang]
    writer(pathlib.Path(args.output), options)
    return 0


if __name__ == "__main__":
    sys.exit(main())

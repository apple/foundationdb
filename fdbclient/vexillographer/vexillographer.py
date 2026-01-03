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
     * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
    # Copyright (c) 2013-2017 Apple Inc.

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
    # Copyright (c) 2013-2017 Apple Inc.

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

LICENSE_JAVA = textwrap.dedent(
    """
    /*
     * FoundationDB Java API
     * Copyright (c) 2013-2024 Apple Inc.
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
    """
).strip()


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


def write_c(file_path: pathlib.Path, options: Iterable[Option]) -> None:
    def option_line(opt: Option, prefix: str) -> str:
        parameter_comment = ""
        if opt.scope.name.endswith("Option"):
            hidden_note = (
                "This is a hidden parameter and should not be used directly by applications."
                if opt.hidden
                else ""
            )
            parameter_comment = f"    /* Parameter: {opt.parameter_comment()} {hidden_note}*/\n"
        return f"{parameter_comment}    /* {opt.comment} */\n    {prefix}{opt.name.upper()}={opt.code}"

    with file_path.open("w", newline="\n") as handle:
        handle.write(HEADER_NOTICE_C + "\n")
        for scope in Scope:
            scoped = [o for o in options if o.scope is scope]
            if not scoped:
                scoped = [
                    Option(scope, "DUMMY_DO_NOT_USE", -1, ParamType.NoneType, None,
                           "This option is only a placeholder for C compatibility and should not be used", False, False, False, -1)
                ]
            prefix = f"FDB_{scope.description}_"
            handle.write("typedef enum {\n")
            handle.write(",\n\n".join(option_line(o, prefix) for o in scoped))
            handle.write(f"\n}} FDB{scope.name};\n\n")
        handle.write("#endif\n")


def write_cpp(base_path: pathlib.Path, options: Iterable[Option]) -> None:
    options = list(options)
    # Keep the ".g" stem for generated C++ option artifacts
    header_path = base_path.parent / (base_path.name + ".h")
    source_path = base_path.parent / (base_path.name + ".cpp")

    def option_enum(opt: Option, indent_level: str) -> str:
        return f"{indent_level}/* {opt.comment} */\n{indent_level}{opt.name.upper()}={opt.code}"

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
            header.write(",\n\n".join(option_enum(o, "\t\t") for o in scoped))
            header.write("\n\t};\n\n")
            header.write(f"\tstatic FDBOptionInfoMap<FDB{scope.name}s> optionInfo;\n\n")
            header.write("private:\n")
            header.write("\tstatic void init();\n")
            header.write("};\n\n")
        header.write("#endif\n")

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
        handle.write(LICENSE_PY + "\n")
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
            if scope is Scope.ErrorPredicate:
                continue
            scoped = [o for o in options if o.scope is scope and not o.hidden]
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


def write_java(file_path: pathlib.Path, options: Iterable[Option]) -> None:
    with file_path.open("w", newline="\n") as handle:
        handle.write(LICENSE_JAVA + "\n")
        handle.write("package com.apple.foundationdb;\n\n")
        handle.write("@SuppressWarnings(\"unused\")\n")
        handle.write("public class FDBOptions {\n")
        handle.write("    private FDBOptions() { }\n")

        # Enums
        for scope in Scope:
            scoped = [o for o in options if o.scope is scope]
            enum_name = scope.name
            handle.write(f"    public enum {enum_name} {{\n")
            handle.write(",\n".join(
                f"        {o.name}({o.code})" for o in scoped
            ))
            handle.write(";\n\n")
            handle.write("        private final int code;\n")
            handle.write(f"        {enum_name}(int c) {{ this.code = c; }}\n")
            handle.write("        public int code() { return this.code; }\n")
            handle.write("    }\n\n")

        # Options info helper
        handle.write("    static final class OptionInfo {\n")
        handle.write("        enum ParamType { NONE, INT, STRING, BYTES }\n")
        handle.write("        public final String description;\n")
        handle.write("        public final String parameterDescription;\n")
        handle.write("        public final boolean hasParameter;\n")
        handle.write("        public final boolean isDeprecated;\n")
        handle.write("        public final boolean isPersistent;\n")
        handle.write("        public final boolean isSensitive;\n")
        handle.write("        public final int defaultFor;\n")
        handle.write("        public final ParamType parameterType;\n")
        handle.write("        OptionInfo(String desc, String paramDesc, boolean hasParam, boolean deprecated, boolean persistent, boolean sensitive, int defaultFor, ParamType parameterType) {\n")
        handle.write("            this.description = desc;\n")
        handle.write("            this.parameterDescription = paramDesc;\n")
        handle.write("            this.hasParameter = hasParam;\n")
        handle.write("            this.isDeprecated = deprecated;\n")
        handle.write("            this.isPersistent = persistent;\n")
        handle.write("            this.isSensitive = sensitive;\n")
        handle.write("            this.defaultFor = defaultFor;\n")
        handle.write("            this.parameterType = parameterType;\n")
        handle.write("        }\n")
        handle.write("    }\n\n")

        # Maps
        handle.write("    static final class OptionInfoMap {\n")
        for scope in Scope:
            scoped = [o for o in options if o.scope is scope]
            map_entries_list = []
            for o in scoped:
                map_entries_list.append(
                    "            new OptionInfo(\"{}\", \"{}\", {}, {}, {}, {}, {}, OptionInfo.ParamType.{})".format(
                        o.comment,
                        o.parameter_comment(),
                        str(o.param_desc is not None).lower(),
                        str(o.is_deprecated()).lower(),
                        str(o.persistent).lower(),
                        str(o.sensitive).lower(),
                        o.default_for,
                        o.param_type.value.upper(),
                    )
                )
            map_entries = ",\n".join(map_entries_list)
            handle.write(f"        static final OptionInfo[] {scope.name} = new OptionInfo[] {{\n{map_entries}\n        }};\n\n")
        handle.write("    }\n")
        handle.write("}\n")


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

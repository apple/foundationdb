from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from .errors import ActorCompilerError
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
    Statement,
    StateDeclarationStatement,
    ThrowStatement,
    TryStatement,
    VarDeclaration,
    WaitStatement,
    WhenStatement,
    WhileStatement,
)


class Function:
    def __init__(
        self,
        name: str = "",
        return_type: str = "int",
        formal_parameters: Optional[Sequence[str]] = None,
    ) -> None:
        self.name = name
        self.return_type = return_type
        self.formal_parameters = list(formal_parameters or [])
        self.end_is_unreachable = False
        self.exception_parameter_is: Optional[str] = None
        self.public_name = False
        self.specifiers = ""
        self._indentation = ""
        self._body = io.StringIO()
        self.was_called = False
        self.overload: Optional["Function"] = None

    def set_overload(self, overload: "Function") -> None:
        self.overload = overload

    def pop_overload(self) -> Optional["Function"]:
        overload = self.overload
        self.overload = None
        return overload

    def add_overload(self, *formal_parameters: str) -> None:
        overload = Function(
            name=self.name,
            return_type=self.return_type,
            formal_parameters=formal_parameters,
        )
        overload.end_is_unreachable = self.end_is_unreachable
        overload._indentation = self._indentation
        self.set_overload(overload)

    def indent(self, change: int) -> None:
        if change > 0:
            self._indentation += "\t" * change
        elif change < 0:
            self._indentation = self._indentation[:change]
        if self.overload is not None:
            self.overload.indent(change)

    def write_line_unindented(self, text: str) -> None:
        self._body.write(f"{text}\n")
        if self.overload is not None:
            self.overload.write_line_unindented(text)

    def write_line(self, text: str, *args: object) -> None:
        if args:
            text = text.format(*args)
        self._body.write(f"{self._indentation}{text}\n")
        if self.overload is not None:
            self.overload.write_line(text)

    @property
    def body_text(self) -> str:
        value = self._body.getvalue()
        if self.overload is not None:
            _ = self.overload.body_text
        return value

    def use_by_name(self) -> str:
        self.was_called = True
        return self.name if self.public_name else f"a_{self.name}"

    def call(self, *parameters: str) -> str:
        params = ", ".join(parameters)
        return f"{self.use_by_name()}({params})"


class LiteralBreak(Function):
    def __init__(self) -> None:
        super().__init__(name="break!", return_type="")

    def call(self, *parameters: str) -> str:
        if parameters:
            raise ActorCompilerError(0, "LiteralBreak called with parameters!")
        self.was_called = True
        return "break"


class LiteralContinue(Function):
    def __init__(self) -> None:
        super().__init__(name="continue!", return_type="")

    def call(self, *parameters: str) -> str:
        if parameters:
            raise ActorCompilerError(0, "LiteralContinue called with parameters!")
        self.was_called = True
        return "continue"


@dataclass
class StateVar:
    type: str = ""
    name: str = ""
    initializer: Optional[str] = None
    initializer_constructor_syntax: bool = False
    source_line: int = 0


@dataclass
class CallbackVar:
    type: str = ""
    callback_group: int = 0
    source_line: int = 0


class Context:
    def __init__(
        self,
        target: Optional[Function] = None,
        next_func: Optional[Function] = None,
        break_f: Optional[Function] = None,
        continue_f: Optional[Function] = None,
        catch_f_err: Optional[Function] = None,
        try_loop_depth: int = 0,
    ) -> None:
        self.target = target
        self.next = next_func
        self.break_f = break_f
        self.continue_f = continue_f
        self.catch_f_err = catch_f_err
        self.try_loop_depth = try_loop_depth

    def unreachable(self) -> None:
        self.target = None

    def with_target(self, new_target: Function) -> "Context":
        return Context(
            target=new_target,
            break_f=self.break_f,
            continue_f=self.continue_f,
            catch_f_err=self.catch_f_err,
            try_loop_depth=self.try_loop_depth,
        )

    def loop_context(
        self,
        new_target: Function,
        break_f: Function,
        continue_f: Function,
        deltaLoopDepth: int,
    ) -> "Context":
        return Context(
            target=new_target,
            break_f=break_f,
            continue_f=continue_f,
            catch_f_err=self.catch_f_err,
            try_loop_depth=self.try_loop_depth + deltaLoopDepth,
        )

    def with_catch(self, new_catch: Function) -> "Context":
        return Context(
            target=self.target,
            break_f=self.break_f,
            continue_f=self.continue_f,
            catch_f_err=new_catch,
        )

    def clone(self) -> "Context":
        return Context(
            target=self.target,
            next_func=self.next,
            break_f=self.break_f,
            continue_f=self.continue_f,
            catch_f_err=self.catch_f_err,
            try_loop_depth=self.try_loop_depth,
        )

    def copy_from(self, other: "Context") -> None:
        self.target = other.target
        self.next = other.next
        self.break_f = other.break_f
        self.continue_f = other.continue_f
        self.catch_f_err = other.catch_f_err
        self.try_loop_depth = other.try_loop_depth


class DescrCompiler:
    def __init__(self, descr: "Descr", brace_depth: int) -> None:
        self.descr = descr
        self.member_indent_str = "\t" * brace_depth

    def write(self, writer) -> int:
        lines = 0
        indent = self.member_indent_str
        writer.write(
            f"{indent}template<> struct Descriptor<struct {self.descr.name}> {{\n"
        )
        writer.write(
            f'{indent}\tstatic StringRef typeName() {{ return "{self.descr.name}"_sr; }}\n'
        )
        writer.write(f"{indent}\ttypedef {self.descr.name} type;\n")
        lines += 3
        for dec in self.descr.body:
            writer.write(f"{indent}\tstruct {dec.name}Descriptor {{\n")
            writer.write(
                f'{indent}\t\tstatic StringRef name() {{ return "{dec.name}"_sr; }}\n'
            )
            writer.write(
                f'{indent}\t\tstatic StringRef typeName() {{ return "{dec.type}"_sr; }}\n'
            )
            writer.write(
                f'{indent}\t\tstatic StringRef comment() {{ return "{dec.comment}"_sr; }}\n'
            )
            writer.write(f"{indent}\t\ttypedef {dec.type} type;\n")
            writer.write(
                f"{indent}\t\tstatic inline type get({self.descr.name}& from);\n"
            )
            writer.write(f"{indent}\t}};\n")
            lines += 7
        writer.write(f"{indent}\ttypedef std::tuple<")
        first = True
        for dec in self.descr.body:
            if not first:
                writer.write(",")
            writer.write(f"{dec.name}Descriptor")
            first = False
        writer.write("> fields;\n")
        writer.write(
            f"{indent}\ttypedef make_index_sequence_impl<0, index_sequence<>, std::tuple_size<fields>::value>::type field_indexes;\n"
        )
        writer.write(f"{indent}}};\n")
        if self.descr.super_class_list:
            writer.write(
                f"{indent}struct {self.descr.name} : {self.descr.super_class_list} {{\n"
            )
        else:
            writer.write(f"{indent}struct {self.descr.name} {{\n")
        lines += 4
        for dec in self.descr.body:
            writer.write(f"{indent}\t{dec.type} {dec.name}; //{dec.comment}\n")
            lines += 1
        writer.write(f"{indent}}};\n")
        lines += 1
        for dec in self.descr.body:
            writer.write(
                f"{indent}{dec.type} Descriptor<{self.descr.name}>::{dec.name}Descriptor::get({self.descr.name}& from) {{ return from.{dec.name}; }}\n"
            )
            lines += 1
        return lines


class ActorCompiler:
    member_indent_str = "\t"
    loop_depth_0 = "int loopDepth=0"
    loop_depth = "int loopDepth"
    code_indent = 2
    used_class_names: set[str] = set()

    def __init__(
        self,
        actor: Actor,
        source_file: str,
        is_top_level: bool,
        line_numbers_enabled: bool,
        generate_probes: bool,
    ) -> None:
        self.actor = actor
        self.source_file = source_file
        self.is_top_level = is_top_level
        self.line_numbers_enabled = line_numbers_enabled
        self.generate_probes = generate_probes
        self.class_name = ""
        self.full_class_name = ""
        self.state_class_name = ""
        self.state: List[StateVar] = []
        self.callbacks: List[CallbackVar] = []
        self.choose_groups = 0
        self.when_count = 0
        self.this = ""
        self.uid_objects: Dict[Tuple[int, int], str] = {}
        self.functions: Dict[str, Function] = {}
        self.iterators: Dict[str, int] = {}
        self.find_state()

    def byte_to_long(self, data: bytes) -> int:
        result = 0
        for b in data:
            result += b
            result <<= 8
        return result & ((1 << 64) - 1)

    def get_uid_from_string(self, value: str) -> Tuple[int, int]:
        digest = hashlib.sha256(value.encode("utf-8")).digest()
        first = self.byte_to_long(digest[:8])
        second = self.byte_to_long(digest[8:16])
        return (first, second)

    def writeActorFunction(self, writer, full_return_type: str) -> None:
        self.writeTemplate(writer)
        self.line_number(writer, self.actor.source_line)
        for attribute in self.actor.attributes:
            writer.write(f"{attribute} ")
        if self.actor.is_static:
            writer.write("static ")
        namespace_prefix = (
            "" if self.actor.name_space is None else f"{self.actor.name_space}::"
        )
        params = ", ".join(self.parameter_list())
        writer.write(
            f"{full_return_type} {namespace_prefix}{self.actor.name}( {params} ) {{\n"
        )
        self.line_number(writer, self.actor.source_line)
        ctor_args = ", ".join(param.name for param in self.actor.parameters)
        new_actor = f"new {self.full_class_name}({ctor_args})"
        if self.actor.return_type is not None:
            writer.write(f"\treturn Future<{self.actor.return_type}>({new_actor});\n")
        else:
            writer.write(f"\t{new_actor};\n")
        writer.write("}\n")

    def writeActorClass(
        self, writer, full_state_class_name: str, body: Function
    ) -> None:
        writer.write(
            f"// This generated class is to be used only via {self.actor.name}()\n"
        )
        self.writeTemplate(writer)
        self.line_number(writer, self.actor.source_line)
        callback_bases = ", ".join(f"public {cb.type}" for cb in self.callbacks)
        if callback_bases:
            callback_bases += ", "
        writer.write(
            "class {0} final : public Actor<{2}>, {3}public FastAllocated<{1}>, public {4} {{\n".format(
                self.class_name,
                self.full_class_name,
                "void" if self.actor.return_type is None else self.actor.return_type,
                callback_bases,
                full_state_class_name,
            )
        )
        writer.write("public:\n")
        writer.write(f"\tusing FastAllocated<{self.full_class_name}>::operator new;\n")
        writer.write(f"\tusing FastAllocated<{self.full_class_name}>::operator delete;\n")
        actor_identifier_key = f"{self.source_file}:{self.actor.name}"
        uid = self.get_uid_from_string(actor_identifier_key)
        self.uid_objects[(uid[0], uid[1])] = actor_identifier_key
        writer.write(
            f"\tstatic constexpr ActorIdentifier __actorIdentifier = UID({uid[0]}UL, {uid[1]}UL);\n"
        )
        writer.write("\tActiveActorHelper activeActorHelper;\n")
        writer.write("#pragma clang diagnostic push\n")
        writer.write('#pragma clang diagnostic ignored "-Wdelete-non-virtual-dtor"\n')
        if self.actor.return_type is not None:
            writer.write("    void destroy() override {\n")
            writer.write("        activeActorHelper.~ActiveActorHelper();\n")
            writer.write(
                f"        static_cast<Actor<{self.actor.return_type}>*>(this)->~Actor();\n"
            )
            writer.write("        operator delete(this);\n")
            writer.write("    }\n")
        else:
            writer.write("    void destroy() {{\n")
            writer.write("        activeActorHelper.~ActiveActorHelper();\n")
            writer.write("        static_cast<Actor<void>*>(this)->~Actor();\n")
            writer.write("        operator delete(this);\n")
            writer.write("    }}\n")
        writer.write("#pragma clang diagnostic pop\n")
        for cb in self.callbacks:
            writer.write(f"friend struct {cb.type};\n")
        self.line_number(writer, self.actor.source_line)
        self.writeConstructor(body, writer, full_state_class_name)
        self.writeCancelFunc(writer)
        writer.write("};\n")

    def write(self, writer) -> None:
        full_return_type = (
            f"Future<{self.actor.return_type}>"
            if self.actor.return_type is not None
            else "void"
        )
        for i in range(1 << 16):
            class_name = "{3}{0}{1}Actor{2}".format(
                self.actor.name[:1].upper(),
                self.actor.name[1:],
                str(i) if i != 0 else "",
                (
                    self.actor.enclosing_class.replace("::", "_") + "_"
                    if self.actor.enclosing_class is not None
                    and self.actor.is_forward_declaration
                    else (
                        self.actor.name_space.replace("::", "_") + "_"
                        if self.actor.name_space is not None
                        else ""
                    )
                ),
            )
            if self.actor.is_forward_declaration:
                self.class_name = class_name
                break
            if class_name not in ActorCompiler.used_class_names:
                self.class_name = class_name
                ActorCompiler.used_class_names.add(class_name)
                break
        self.full_class_name = self.class_name + self.get_template_actuals()
        actor_class_formal = VarDeclaration(name=self.class_name, type="class")
        self.this = f"static_cast<{actor_class_formal.name}*>(this)"
        self.state_class_name = self.class_name + "State"
        full_state = self.state_class_name + self.get_template_actuals(
            VarDeclaration(type="class", name=self.full_class_name)
        )
        if self.actor.is_forward_declaration:
            for attribute in self.actor.attributes:
                writer.write(f"{attribute} ")
            if self.actor.is_static:
                writer.write("static ")
            ns = "" if self.actor.name_space is None else f"{self.actor.name_space}::"
            writer.write(
                f"{full_return_type} {ns}{self.actor.name}( {', '.join(self.parameter_list())} );\n"
            )
            if self.actor.enclosing_class is not None:
                writer.write(f"template <class> friend class {self.state_class_name};\n")
            return
        body = self.get_function("", "body", self.loop_depth_0)
        body_context = Context(
            target=body,
            catch_f_err=self.get_function(
                body.name, "Catch", "Error error", self.loop_depth_0
            ),
        )
        end_context = self.try_catch_compile(self.actor.body, body_context)
        if end_context.target is not None:
            if self.actor.return_type is None:
                self.compile_statement(
                    ReturnStatement(
                        first_source_line=self.actor.source_line, expression=""
                    ),
                    end_context,
                )
            else:
                raise ActorCompilerError(
                    self.actor.source_line,
                    "Actor {0} fails to return a value",
                    self.actor.name,
                )
        if self.actor.return_type is not None:
            body_context.catch_f_err.write_line(f"this->~{self.state_class_name}();")
            body_context.catch_f_err.write_line(
                f"{self.this}->sendErrorAndDelPromiseRef(error);"
            )
        else:
            body_context.catch_f_err.write_line(f"delete {self.this};")
        body_context.catch_f_err.write_line("loopDepth = 0;")
        if self.is_top_level and self.actor.name_space is None:
            writer.write("namespace {\n")
        writer.write(
            f"// This generated class is to be used only via {self.actor.name}()\n"
        )
        self.writeTemplate(writer, actor_class_formal)
        self.line_number(writer, self.actor.source_line)
        writer.write(f"class {self.state_class_name} {{\n")
        writer.write("public:\n")
        self.line_number(writer, self.actor.source_line)
        self.writeStateConstructor(writer)
        self.writeStateDestructor(writer)
        self.writeFunctions(writer)
        for st in self.state:
            self.line_number(writer, st.source_line)
            writer.write(f"\t{st.type} {st.name};\n")
        writer.write("};\n")
        self.writeActorClass(writer, full_state, body)
        if self.is_top_level and self.actor.name_space is None:
            writer.write("} // namespace\n")
        self.writeActorFunction(writer, full_return_type)
        if self.actor.test_case_parameters is not None:
            writer.write(
                f"ACTOR_TEST_CASE({self.actor.name}, {self.actor.test_case_parameters})\n"
            )

    thisAddress = "reinterpret_cast<unsigned long>(this)"

    def probe_enter(self, fun: Function, name: str, index: int = -1) -> None:
        if self.generate_probes:
            fun.write_line(
                'fdb_probe_actor_enter("{0}", {1}, {2});',
                name,
                self.thisAddress,
                index,
            )
        block_identifier = self.get_uid_from_string(fun.name)
        fun.write_line("#ifdef WITH_ACAC")
        fun.write_line(
            "static constexpr ActorBlockIdentifier __identifier = UID({0}UL, {1}UL);",
            block_identifier[0],
            block_identifier[1],
        )
        fun.write_line(
            "ActorExecutionContextHelper __helper(static_cast<{0}*>(this)->activeActorHelper.actorID, __identifier);",
            self.class_name,
        )
        fun.write_line("#endif // WITH_ACAC")

    def probe_exit(self, fun: Function, name: str, index: int = -1) -> None:
        if self.generate_probes:
            fun.write_line(
                'fdb_probe_actor_exit("{0}", {1}, {2});',
                name,
                self.thisAddress,
                index,
            )

    def probe_create(self, fun: Function, name: str) -> None:
        if self.generate_probes:
            fun.write_line(
                'fdb_probe_actor_create("{0}", {1});', name, self.thisAddress
            )

    def probe_destroy(self, fun: Function, name: str) -> None:
        if self.generate_probes:
            fun.write_line(
                'fdb_probe_actor_destroy("{0}", {1});',
                name,
                self.thisAddress,
            )

    def line_number(self, writer, source_line: int) -> None:
        if source_line == 0:
            raise ActorCompilerError(0, "Invalid source line (0)")
        if self.line_numbers_enabled:
            writer.write(
                '\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} "{1}"\n'.format(
                    source_line, self.source_file
                )
            )

    def line_numberFunction(self, func: Function, source_line: int) -> None:
        if source_line == 0:
            raise ActorCompilerError(0, "Invalid source line (0)")
        if self.line_numbers_enabled:
            func.write_line_unindented(
                '\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} "{1}"'.format(
                    source_line, self.source_file
                )
            )

    def try_catch(
        self,
        cx: Context,
        catch_function: Optional[Function],
        catch_loop_depth: int,
        action,
        use_loop_depth: bool = True,
    ) -> None:
        if catch_function is not None:
            cx.target.write_line("try {")
            cx.target.indent(+1)
        action()
        if catch_function is not None:
            cx.target.indent(-1)
            cx.target.write_line("}")
            cx.target.write_line("catch (Error& error) {")
            if use_loop_depth:
                cx.target.write_line(
                    "\tloopDepth = {0};",
                    catch_function.call(
                        "error", self.adjust_loop_depth(catch_loop_depth)
                    ),
                )
            else:
                cx.target.write_line("\t{0};", catch_function.call("error", "0"))
            cx.target.write_line("} catch (...) {")
            if use_loop_depth:
                cx.target.write_line(
                    "\tloopDepth = {0};",
                    catch_function.call(
                        "unknown_error()", self.adjust_loop_depth(catch_loop_depth)
                    ),
                )
            else:
                cx.target.write_line(
                    "\t{0};", catch_function.call("unknown_error()", "0")
                )
            cx.target.write_line("}")

    def try_catch_compile(self, block: CodeBlock, cx: Context) -> Context:
        result_holder = {"ctx": cx}

        def action() -> None:
            result_holder["ctx"] = self._try_catch_compile_body(block, cx)

        self.try_catch(cx, cx.catch_f_err, cx.try_loop_depth, action)
        return result_holder["ctx"]

    def _try_catch_compile_body(self, block: CodeBlock, cx: Context) -> Context:
        compiled = self.compile(block, cx, True)
        if compiled.target is not None:
            next_func = self.get_function(compiled.target.name, "cont", self.loop_depth)
            compiled.target.write_line("loopDepth = {0};", next_func.call("loopDepth"))
            compiled.target = next_func
            compiled.next = None
        return compiled

    def writeTemplate(self, writer, *extra_parameters: VarDeclaration) -> None:
        formals = list(self.actor.template_formals or []) + list(extra_parameters)
        if not formals:
            return
        self.line_number(writer, self.actor.source_line)
        writer.write(
            "template <{0}>\n".format(", ".join(f"{p.type} {p.name}" for p in formals))
        )

    def get_template_actuals(self, *extra_parameters: VarDeclaration) -> str:
        formals = list(self.actor.template_formals or []) + list(extra_parameters)
        if not formals:
            return ""
        return "<{0}>".format(", ".join(p.name for p in formals))

    def will_continue(self, stmt: Statement) -> bool:
        return any(
            isinstance(sub, (ChooseStatement, WaitStatement, TryStatement))
            for sub in self.flatten(stmt)
        )

    def as_code_block(self, statement: Statement) -> CodeBlock:
        if isinstance(statement, CodeBlock):
            return statement
        return CodeBlock(statements=[statement])

    def flatten(self, stmt: Optional[Statement]) -> Iterable[Statement]:
        if stmt is None:
            return []

        def _flatten(s: Statement) -> Iterable[Statement]:
            yield s
            if isinstance(s, LoopStatement):
                yield from self.flatten(s.body)
            elif isinstance(s, WhileStatement):
                yield from self.flatten(s.body)
            elif isinstance(s, ForStatement):
                yield from self.flatten(s.body)
            elif isinstance(s, RangeForStatement):
                yield from self.flatten(s.body)
            elif isinstance(s, CodeBlock):
                for child in s.statements:
                    yield from self.flatten(child)
            elif isinstance(s, IfStatement):
                yield from self.flatten(s.if_body)
                if s.else_body:
                    yield from self.flatten(s.else_body)
            elif isinstance(s, ChooseStatement):
                yield from self.flatten(s.body)
            elif isinstance(s, WhenStatement):
                if s.body:
                    yield from self.flatten(s.body)
            elif isinstance(s, TryStatement):
                yield from self.flatten(s.try_body)
                for c in s.catches:
                    yield from self.flatten(c.body)

        return list(_flatten(stmt))

    def find_state(self) -> None:
        self.state = [
            StateVar(
                source_line=self.actor.source_line,
                name=p.name,
                type=p.type,
                initializer=p.name,
            )
            for p in self.actor.parameters
        ]

    def adjust_loop_depth(self, subtract: int) -> str:
        if subtract == 0:
            return "loopDepth"
        return f"std::max(0, loopDepth - {subtract})"

    def parameter_list(self) -> List[str]:
        params = []
        for p in self.actor.parameters:
            if p.initializer:
                params.append(f"{p.type} const& {p.name} = {p.initializer}")
            else:
                params.append(f"{p.type} const& {p.name}")
        return params

    def get_function(
        self,
        base_name: str,
        add_name: str,
        *formal_parameters: str,
        overload_formal_parameters: Optional[Sequence[str]] = None,
    ) -> Function:
        if len(formal_parameters) == 1 and isinstance(
            formal_parameters[0], (list, tuple)
        ):
            params = list(formal_parameters[0])
        else:
            params = [p for p in formal_parameters if p]

        if add_name == "cont" and len(base_name) >= 5 and base_name[-5:-1] == "cont":
            proposed_name = base_name[:-1]
        else:
            proposed_name = base_name + add_name
        idx = 1
        while f"{proposed_name}{idx}" in self.functions:
            idx += 1
        func = Function(
            name=f"{proposed_name}{idx}",
            return_type="int",
            formal_parameters=params,
        )
        func.indent(self.code_indent)
        if overload_formal_parameters:
            func.add_overload(*overload_formal_parameters)
        self.functions[func.name] = func
        return func

    def writeFunctions(self, writer) -> None:
        for func in self.functions.values():
            body = func.body_text
            if body:
                self.writeFunction(writer, func, body)
            if func.overload:
                overload_body = func.overload.body_text
                if overload_body:
                    self.writeFunction(writer, func.overload, overload_body)

    def writeFunction(self, writer, func: Function, body: str) -> None:
        spec = "" if not func.specifiers else f" {func.specifiers}"
        trailing = " " if spec == "" else ""
        signature = (
            f"{self.member_indent_str}"
            f"{'' if func.return_type == '' else f'{func.return_type} '}"
            f"{func.use_by_name()}({','.join(func.formal_parameters)})"
            f"{spec}{trailing}\n"
        )
        writer.write(signature)
        if func.return_type != "":
            writer.write(f"{self.member_indent_str}{{\n")
        writer.write(body)
        writer.write("\n")
        if not func.end_is_unreachable:
            writer.write(f"{self.member_indent_str}\treturn loopDepth;\n")
        writer.write(f"{self.member_indent_str}}}\n")

    def writeCancelFunc(self, writer) -> None:
        if not self.actor.is_cancellable():
            return
        cancel_func = Function(
            name="cancel",
            return_type="void",
            formal_parameters=[],
        )
        cancel_func.end_is_unreachable = True
        cancel_func.public_name = True
        cancel_func.specifiers = "override"
        cancel_func.indent(self.code_indent)
        cancel_func.write_line("auto wait_state = this->actor_wait_state;")
        cancel_func.write_line("this->actor_wait_state = -1;")
        cancel_func.write_line("switch (wait_state) {")
        last_group = -1
        for cb in sorted(self.callbacks, key=lambda c: c.callback_group):
            if cb.callback_group != last_group:
                last_group = cb.callback_group
                cancel_func.write_line(
                    "case {0}: this->a_callback_error(({1}*)0, actor_cancelled()); break;",
                    cb.callback_group,
                    cb.type,
                )
        cancel_func.write_line("}")
        self.writeFunction(writer, cancel_func, cancel_func.body_text)

    def writeConstructor(
        self, body: Function, writer, full_state_class_name: str
    ) -> None:
        constructor = Function(
            name=self.class_name,
            return_type="",
            formal_parameters=self.parameter_list(),
        )
        constructor.end_is_unreachable = True
        constructor.public_name = True
        constructor.indent(self.code_indent)
        constructor.write_line(
            " : Actor<{0}>(),".format(
                "void" if self.actor.return_type is None else self.actor.return_type
            )
        )
        constructor.write_line(
            "   {0}({1}),".format(
                full_state_class_name,
                ", ".join(p.name for p in self.actor.parameters),
            )
        )
        constructor.write_line("   activeActorHelper(__actorIdentifier)")
        constructor.indent(-1)
        constructor.write_line("{")
        constructor.indent(+1)
        self.probe_enter(constructor, self.actor.name)
        constructor.write_line("#ifdef ENABLE_SAMPLING")
        constructor.write_line('this->lineage.setActorName("{0}");', self.actor.name)
        constructor.write_line("LineageScope _(&this->lineage);")
        constructor.write_line("#endif")
        constructor.write_line("this->{0};", body.call())
        self.probe_exit(constructor, self.actor.name)
        self.writeFunction(writer, constructor, constructor.body_text)

    def writeStateConstructor(self, writer) -> None:
        constructor = Function(
            name=self.state_class_name,
            return_type="",
            formal_parameters=self.parameter_list(),
        )
        constructor.end_is_unreachable = True
        constructor.public_name = True
        constructor.indent(self.code_indent)
        ini = None
        line = self.actor.source_line
        for state_var in self.state:
            if state_var.initializer is None:
                continue
            self.line_numberFunction(constructor, line)
            if ini is None:
                ini = " : "
            else:
                constructor.write_line(ini + ",")
                ini = "   "
            ini += f"{state_var.name}({state_var.initializer})"
            line = state_var.source_line
        self.line_numberFunction(constructor, line)
        if ini:
            constructor.write_line(ini)
        constructor.indent(-1)
        constructor.write_line("{")
        constructor.indent(+1)
        self.probe_create(constructor, self.actor.name)
        self.writeFunction(writer, constructor, constructor.body_text)

    def writeStateDestructor(self, writer) -> None:
        destructor = Function(
            name=f"~{self.state_class_name}",
            return_type="",
            formal_parameters=[],
        )
        destructor.end_is_unreachable = True
        destructor.public_name = True
        destructor.indent(self.code_indent)
        destructor.indent(-1)
        destructor.write_line("{")
        destructor.indent(+1)
        self.probe_destroy(destructor, self.actor.name)
        self.writeFunction(writer, destructor, destructor.body_text)

    def compile(
        self, block: CodeBlock, context: Context, ok_to_continue: bool = True
    ) -> Context:
        cx = context.clone()
        cx.next = None
        for stmt in block.statements:
            if cx.target is None:
                raise ActorCompilerError(stmt.first_source_line, "Unreachable code.")
            if cx.next is None:
                cx.next = self.get_function(cx.target.name, "cont", self.loop_depth)
            self.compile_statement(stmt, cx)
            if cx.next.was_called:
                if cx.target is None:
                    raise ActorCompilerError(
                        stmt.first_source_line, "Unreachable continuation called?"
                    )
                if not ok_to_continue:
                    raise ActorCompilerError(
                        stmt.first_source_line, "Unexpected continuation"
                    )
                cx.target = cx.next
                cx.next = None
        return cx

    def compile_statement(self, stmt: Statement, cx: Context) -> None:
        handler_name = f"_compile_{stmt.__class__.__name__}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            raise ActorCompilerError(
                stmt.first_source_line,
                "Statement type {0} not supported yet.",
                stmt.__class__.__name__,
            )
        handler(stmt, cx)

    def _is_top_level_state_decl(self, stmt: StateDeclarationStatement) -> bool:
        for candidate in self.actor.body.statements:
            if not isinstance(candidate, StateDeclarationStatement):
                break
            if candidate is stmt:
                return True
        return False

    def _compile_PlainOldCodeStatement(
        self, stmt: PlainOldCodeStatement, cx: Context
    ) -> None:
        self.line_numberFunction(cx.target, stmt.first_source_line)
        cx.target.write_line(stmt.code)

    def _compile_StateDeclarationStatement(
        self, stmt: StateDeclarationStatement, cx: Context
    ) -> None:
        if self._is_top_level_state_decl(stmt):
            self.state.append(
                StateVar(
                    source_line=stmt.first_source_line,
                    name=stmt.decl.name,
                    type=stmt.decl.type,
                    initializer=stmt.decl.initializer,
                    initializer_constructor_syntax=stmt.decl.initializer_constructor_syntax,
                )
            )
        else:
            self.state.append(
                StateVar(
                    source_line=stmt.first_source_line,
                    name=stmt.decl.name,
                    type=stmt.decl.type,
                    initializer=None,
                )
            )
            if stmt.decl.initializer is not None:
                self.line_numberFunction(cx.target, stmt.first_source_line)
                if (
                    stmt.decl.initializer_constructor_syntax
                    or stmt.decl.initializer == ""
                ):
                    cx.target.write_line(
                        "{0} = {1}({2});",
                        stmt.decl.name,
                        stmt.decl.type,
                        stmt.decl.initializer,
                    )
                else:
                    cx.target.write_line(
                        "{0} = {1};", stmt.decl.name, stmt.decl.initializer
                    )

    def get_iterator_name(self, cx: Context) -> str:
        name = f"RangeFor{cx.target.name}Iterator"
        self.iterators.setdefault(name, 0)
        idx = self.iterators[name]
        self.iterators[name] += 1
        return f"{name}{idx}"

    def emit_native_loop(
        self, source_line: int, head: str, body: Statement, cx: Context
    ) -> bool:
        self.line_numberFunction(cx.target, source_line)
        cx.target.write_line(head + " {")
        cx.target.indent(+1)
        literal_break = LiteralBreak()
        literal_continue = LiteralContinue()
        self.compile(
            self.as_code_block(body),
            cx.loop_context(cx.target, literal_break, literal_continue, 0),
            True,
        )
        cx.target.indent(-1)
        cx.target.write_line("}")
        return not literal_break.was_called

    def _compile_ForStatement(self, stmt: ForStatement, cx: Context) -> None:
        no_condition = stmt.cond_expression in ("", "true", "1")
        if not self.will_continue(stmt.body):
            if (
                self.emit_native_loop(
                    stmt.first_source_line,
                    f"for({stmt.init_expression};{stmt.cond_expression};{stmt.next_expression})",
                    stmt.body,
                    cx,
                )
                and no_condition
            ):
                cx.unreachable()
            return

        init_stmt = PlainOldCodeStatement(
            code=f"{stmt.init_expression};", first_source_line=stmt.first_source_line
        )
        self._compile_PlainOldCodeStatement(init_stmt, cx)

        if no_condition:
            full_body = stmt.body
        else:
            condition = IfStatement(
                expression=f"!({stmt.cond_expression})",
                if_body=BreakStatement(first_source_line=stmt.first_source_line),
                first_source_line=stmt.first_source_line,
            )
            full_body = CodeBlock(
                statements=[condition] + list(self.as_code_block(stmt.body).statements),
                first_source_line=stmt.first_source_line,
            )

        loopF = self.get_function(cx.target.name, "loopHead", self.loop_depth)
        loopBody = self.get_function(cx.target.name, "loopBody", self.loop_depth)
        break_f = self.get_function(cx.target.name, "break", self.loop_depth)
        continue_f = (
            loopF
            if stmt.next_expression == ""
            else self.get_function(cx.target.name, "continue", self.loop_depth)
        )

        loopF.write_line("int oldLoopDepth = ++loopDepth;")
        loopF.write_line(
            "while (loopDepth == oldLoopDepth) loopDepth = {0};",
            loopBody.call("loopDepth"),
        )

        endLoop = self.compile(
            self.as_code_block(full_body),
            cx.loop_context(loopBody, break_f, continue_f, +1),
            True,
        ).target

        if endLoop is not None and endLoop is not loopBody:
            if stmt.next_expression:
                self._compile_PlainOldCodeStatement(
                    PlainOldCodeStatement(
                        code=f"{stmt.next_expression};",
                        first_source_line=stmt.first_source_line,
                    ),
                    cx.with_target(endLoop),
                )
            endLoop.write_line("if (loopDepth == 0) return {0};", loopF.call("0"))

        cx.target.write_line("loopDepth = {0};", loopF.call("loopDepth"))

        if continue_f is not loopF and continue_f.was_called:
            self._compile_PlainOldCodeStatement(
                PlainOldCodeStatement(
                    code=f"{stmt.next_expression};", first_source_line=stmt.first_source_line
                ),
                cx.with_target(continue_f),
            )
            continue_f.write_line("if (loopDepth == 0) return {0};", loopF.call("0"))

        if break_f.was_called:
            self.try_catch(
                cx.with_target(break_f),
                cx.catch_f_err,
                cx.try_loop_depth,
                lambda: break_f.write_line("return {0};", cx.next.call("loopDepth")),
            )
        else:
            cx.unreachable()

    def _compile_RangeForStatement(self, stmt: RangeForStatement, cx: Context) -> None:
        if self.will_continue(stmt.body):
            container = next(
                (s for s in self.state if s.name == stmt.range_expression), None
            )
            if container is None:
                raise ActorCompilerError(
                    stmt.first_source_line,
                    "container of range-based for with continuation must be a state variable",
                )
            iterator_name = self.get_iterator_name(cx)
            self.state.append(
                StateVar(
                    source_line=stmt.first_source_line,
                    name=iterator_name,
                    type=f"decltype(std::begin(std::declval<{container.type}>()))",
                    initializer=None,
                )
            )
            equivalent = ForStatement(
                init_expression=f"{iterator_name} = std::begin({stmt.range_expression})",
                cond_expression=f"{iterator_name} != std::end({stmt.range_expression})",
                next_expression=f"++{iterator_name}",
                first_source_line=stmt.first_source_line,
                body=CodeBlock(
                    statements=[
                        PlainOldCodeStatement(
                            code=f"{stmt.range_decl} = *{iterator_name};",
                            first_source_line=stmt.first_source_line,
                        ),
                        stmt.body,
                    ],
                    first_source_line=stmt.first_source_line,
                ),
            )
            self._compile_ForStatement(equivalent, cx)
        else:
            self.emit_native_loop(
                stmt.first_source_line,
                f"for( {stmt.range_decl} : {stmt.range_expression} )",
                stmt.body,
                cx,
            )

    def _compile_WhileStatement(self, stmt: WhileStatement, cx: Context) -> None:
        equivalent = ForStatement(
            init_expression="",
            cond_expression=stmt.expression,
            next_expression="",
            body=stmt.body,
            first_source_line=stmt.first_source_line,
        )
        self._compile_ForStatement(equivalent, cx)

    def _compile_LoopStatement(self, stmt: LoopStatement, cx: Context) -> None:
        equivalent = ForStatement(
            init_expression="",
            cond_expression="",
            next_expression="",
            body=stmt.body,
            first_source_line=stmt.first_source_line,
        )
        self._compile_ForStatement(equivalent, cx)

    def _compile_BreakStatement(self, stmt: BreakStatement, cx: Context) -> None:
        if cx.break_f is None:
            raise ActorCompilerError(stmt.first_source_line, "break outside loop")
        if isinstance(cx.break_f, LiteralBreak):
            cx.target.write_line("{0};", cx.break_f.call())
        else:
            cx.target.write_line(
                "return {0}; // break", cx.break_f.call("loopDepth==0?0:loopDepth-1")
            )
        cx.unreachable()

    def _compile_ContinueStatement(self, stmt: ContinueStatement, cx: Context) -> None:
        if cx.continue_f is None:
            raise ActorCompilerError(stmt.first_source_line, "continue outside loop")
        if isinstance(cx.continue_f, LiteralContinue):
            cx.target.write_line("{0};", cx.continue_f.call())
        else:
            cx.target.write_line(
                "return {0}; // continue", cx.continue_f.call("loopDepth")
            )
        cx.unreachable()

    def _compile_CodeBlock(self, stmt: CodeBlock, cx: Context) -> None:
        cx.target.write_line("{")
        cx.target.indent(+1)
        end = self.compile(stmt, cx, True)
        cx.target.indent(-1)
        cx.target.write_line("}")
        if end.target is None:
            cx.unreachable()
        elif end.target is not cx.target:
            end.target.write_line("loopDepth = {0};", cx.next.call("loopDepth"))

    def _compile_ReturnStatement(self, stmt: ReturnStatement, cx: Context) -> None:
        self.line_numberFunction(cx.target, stmt.first_source_line)
        if (stmt.expression == "") != (self.actor.return_type is None):
            raise ActorCompilerError(
                stmt.first_source_line,
                "Return statement does not match actor declaration",
            )
        if self.actor.return_type is not None:
            if stmt.expression == "Never()":
                cx.target.write_line("this->~{0}();", self.state_class_name)
                cx.target.write_line("{0}->sendAndDelPromiseRef(Never());", self.this)
            else:
                cx.target.write_line(
                    "if (!{0}->SAV<{1}>::futures) {{ (void)({2}); this->~{3}(); {0}->destroy(); return 0; }}",
                    self.this,
                    self.actor.return_type,
                    stmt.expression,
                    self.state_class_name,
                )
                if any(s.name == stmt.expression for s in self.state):
                    cx.target.write_line(
                        "new (&{0}->SAV< {1} >::value()) {1}(std::move({2})); // state_var_RVO",
                        self.this,
                        self.actor.return_type,
                        stmt.expression,
                    )
                else:
                    cx.target.write_line(
                        "new (&{0}->SAV< {1} >::value()) {1}({2});",
                        self.this,
                        self.actor.return_type,
                        stmt.expression,
                    )
                cx.target.write_line("this->~{0}();", self.state_class_name)
                cx.target.write_line("{0}->finishSendAndDelPromiseRef();", self.this)
        else:
            cx.target.write_line("delete {0};", self.this)
        cx.target.write_line("return 0;")
        cx.unreachable()

    def _compile_ThrowStatement(self, stmt: ThrowStatement, cx: Context) -> None:
        self.line_numberFunction(cx.target, stmt.first_source_line)
        if stmt.expression == "":
            if cx.target.exception_parameter_is is not None:
                cx.target.write_line(
                    "return {0};",
                    cx.catch_f_err.call(
                        cx.target.exception_parameter_is,
                        self.adjust_loop_depth(cx.try_loop_depth),
                    ),
                )
            else:
                raise ActorCompilerError(
                    stmt.first_source_line,
                    "throw statement with no expression has no current exception in scope",
                )
        else:
            cx.target.write_line(
                "return {0};",
                cx.catch_f_err.call(
                    stmt.expression, self.adjust_loop_depth(cx.try_loop_depth)
                ),
            )
        cx.unreachable()

    def _compile_IfStatement(self, stmt: IfStatement, cx: Context) -> None:
        use_continuation = self.will_continue(stmt.if_body) or (
            stmt.else_body is not None and self.will_continue(stmt.else_body)
        )
        self.line_numberFunction(cx.target, stmt.first_source_line)
        constexpr = "constexpr " if stmt.constexpr else ""
        cx.target.write_line(f"if {constexpr}({stmt.expression})")
        cx.target.write_line("{")
        cx.target.indent(+1)
        if_target = self.compile(
            self.as_code_block(stmt.if_body), cx, use_continuation
        ).target
        if use_continuation and if_target is not None:
            if_target.write_line("loopDepth = {0};", cx.next.call("loopDepth"))
        cx.target.indent(-1)
        cx.target.write_line("}")
        else_target = None
        if stmt.else_body is not None or use_continuation:
            cx.target.write_line("else")
            cx.target.write_line("{")
            cx.target.indent(+1)
            else_target = cx.target
            if stmt.else_body is not None:
                else_target = self.compile(
                    self.as_code_block(stmt.else_body), cx, use_continuation
                ).target
            if use_continuation and else_target is not None:
                else_target.write_line("loopDepth = {0};", cx.next.call("loopDepth"))
            cx.target.indent(-1)
            cx.target.write_line("}")
        if if_target is None and stmt.else_body is not None and else_target is None:
            cx.unreachable()
        elif not cx.next.was_called and use_continuation:
            raise ActorCompilerError(
                stmt.first_source_line, "Internal error: IfStatement: next not called?"
            )

    def _compile_TryStatement(self, stmt: TryStatement, cx: Context) -> None:
        if len(stmt.catches) != 1:
            raise ActorCompilerError(
                stmt.first_source_line,
                "try statement must have exactly one catch clause",
            )
        reachable = False
        catch_clause = stmt.catches[0]
        catch_expression = catch_clause.expression.replace(" ", "")
        catch_param = ""
        if catch_expression != "...":
            if not catch_expression.startswith("Error&"):
                raise ActorCompilerError(
                    catch_clause.first_source_line,
                    "Only type 'Error' or '...' may be caught in an actor function",
                )
            catch_param = catch_expression[6:]
        if not catch_param:
            catch_param = "__current_error"
        catch_f_err = self.get_function(
            cx.target.name, "Catch", f"const Error& {catch_param}", self.loop_depth_0
        )
        catch_f_err.exception_parameter_is = catch_param
        end = self.try_catch_compile(
            self.as_code_block(stmt.try_body), cx.with_catch(catch_f_err)
        )
        if end.target is not None:
            reachable = True
            self.try_catch(
                end,
                cx.catch_f_err,
                cx.try_loop_depth,
                lambda: end.target.write_line(
                    "loopDepth = {0};", cx.next.call("loopDepth")
                ),
            )

        def handle_catch() -> None:
            nonlocal reachable
            cend = self._compile_try_catch_body(catch_clause, catch_f_err, cx)
            if cend.target is not None:
                reachable = True

        self.try_catch(
            cx.with_target(catch_f_err),
            cx.catch_f_err,
            cx.try_loop_depth,
            handle_catch,
        )
        if not reachable:
            cx.unreachable()

    def _compile_try_catch_body(
        self, catch_clause: TryStatement.Catch, catch_f_err: Function, cx: Context
    ) -> Context:
        cend = self.compile(
            self.as_code_block(catch_clause.body), cx.with_target(catch_f_err), True
        )
        if cend.target is not None:
            cend.target.write_line("loopDepth = {0};", cx.next.call("loopDepth"))
        return cend

    def _compile_WaitStatement(self, stmt: WaitStatement, cx: Context) -> None:
        equivalent = ChooseStatement(
            body=CodeBlock(
                statements=[
                    WhenStatement(
                        wait=stmt,
                        body=None,
                        first_source_line=stmt.first_source_line,
                    )
                ],
                first_source_line=stmt.first_source_line,
            ),
            first_source_line=stmt.first_source_line,
        )
        if not stmt.result_is_state:
            cx.next.formal_parameters = [
                f"{stmt.result.type} const& {stmt.result.name}",
                self.loop_depth,
            ]
            cx.next.add_overload(
                f"{stmt.result.type} && {stmt.result.name}", self.loop_depth
            )
        self._compile_ChooseStatement(equivalent, cx)

    def _compile_ChooseStatement(self, stmt: ChooseStatement, cx: Context) -> None:
        group = self.choose_groups + 1
        self.choose_groups = group
        codeblock = stmt.body if isinstance(stmt.body, CodeBlock) else None
        if codeblock is None:
            raise ActorCompilerError(
                stmt.first_source_line,
                "'choose' must be followed by a compound statement.",
            )
        choices = []
        for idx, choice_stmt in enumerate(codeblock.statements):
            if not isinstance(choice_stmt, WhenStatement):
                raise ActorCompilerError(
                    choice_stmt.first_source_line,
                    "only 'when' statements are valid in a 'choose' block.",
                )
            index = self.when_count + idx
            param_prefix = "__" if choice_stmt.wait.result_is_state else ""
            const_param = f"{choice_stmt.wait.result.type} const& {param_prefix}{choice_stmt.wait.result.name}"
            rvalue_param = f"{choice_stmt.wait.result.type} && {param_prefix}{choice_stmt.wait.result.name}"
            body_func = self.get_function(
                cx.target.name,
                "when",
                [const_param, self.loop_depth],
                overload_formal_parameters=[rvalue_param, self.loop_depth],
            )
            future_name = f"__when_expr_{index}"
            callback_template = (
                "ActorSingleCallback"
                if choice_stmt.wait.is_wait_next
                else "ActorCallback"
            )
            callback_type = f"{callback_template}< {self.full_class_name}, {index}, {choice_stmt.wait.result.type} >"
            callback_type_state = f"{callback_template}< {self.class_name}, {index}, {choice_stmt.wait.result.type} >"
            choices.append(
                {
                    "Stmt": choice_stmt,
                    "Group": group,
                    "Index": index,
                    "Body": body_func,
                    "Future": future_name,
                    "CallbackType": callback_type,
                    "CallbackTypeState": callback_type_state,
                }
            )
        self.when_count += len(choices)
        exit_func = self.get_function("exitChoose", "", [])
        exit_func.return_type = "void"
        exit_func.write_line(
            "if ({0}->actor_wait_state > 0) {0}->actor_wait_state = 0;", self.this
        )
        for choice in choices:
            exit_func.write_line(
                "{0}->{1}::remove();", self.this, choice["CallbackTypeState"]
            )
        exit_func.end_is_unreachable = True

        reachable = False
        for choice in choices:
            self.callbacks.append(
                CallbackVar(
                    source_line=choice["Stmt"].first_source_line,
                    callback_group=choice["Group"],
                    type=choice["CallbackType"],
                )
            )
            r = choice["Body"]
            if choice["Stmt"].wait.result_is_state:
                overload = r.pop_overload()
                decl_stmt = StateDeclarationStatement(
                    decl=VarDeclaration(
                        type=choice["Stmt"].wait.result.type,
                        name=choice["Stmt"].wait.result.name,
                        initializer=f"__{choice['Stmt'].wait.result.name}",
                        initializer_constructor_syntax=False,
                    ),
                    first_source_line=choice["Stmt"].first_source_line,
                )
                self._compile_StateDeclarationStatement(decl_stmt, cx.with_target(r))
                if overload is not None:
                    overload.write_line(
                        "{0} = std::move(__{0});",
                        choice["Stmt"].wait.result.name,
                    )
                    r.set_overload(overload)
            if choice["Stmt"].body is not None:
                r = self.compile(
                    self.as_code_block(choice["Stmt"].body), cx.with_target(r), True
                ).target
            if r is not None:
                reachable = True
                if len(cx.next.formal_parameters) == 1:
                    r.write_line("loopDepth = {0};", cx.next.call("loopDepth"))
                else:
                    overload = r.pop_overload()
                    r.write_line(
                        "loopDepth = {0};",
                        cx.next.call(choice["Stmt"].wait.result.name, "loopDepth"),
                    )
                    if overload is not None:
                        overload.write_line(
                            "loopDepth = {0};",
                            cx.next.call(
                                f"std::move({choice['Stmt'].wait.result.name})",
                                "loopDepth",
                            ),
                        )
                        r.set_overload(overload)

            cb_func = Function(
                name="callback_fire",
                return_type="void",
                formal_parameters=[
                    f"{choice['CallbackTypeState']}*",
                    f"{choice['Stmt'].wait.result.type} const& value",
                ],
            )
            cb_func.end_is_unreachable = True
            cb_func.add_overload(
                f"{choice['CallbackTypeState']}*",
                f"{choice['Stmt'].wait.result.type} && value",
            )
            cb_func.indent(self.code_indent)
            self.probe_enter(cb_func, self.actor.name, choice["Index"])
            cb_func.write_line("{0};", exit_func.call())
            overload_func = cb_func.pop_overload()

            def fire_body(target_func: Function, expr: str, choice_dict=choice) -> None:
                self.try_catch(
                    cx.with_target(target_func),
                    cx.catch_f_err,
                    cx.try_loop_depth,
                    lambda ch=choice_dict, tf=target_func, expression=expr: tf.write_line(
                        "{0};", ch["Body"].call(expression, "0")
                    ),
                    False,
                )

            fire_body(cb_func, "value")
            if overload_func is not None:
                fire_body(overload_func, "std::move(value)")
                cb_func.set_overload(overload_func)
            self.probe_exit(cb_func, self.actor.name, choice["Index"])
            self.functions[f"{cb_func.name}#{choice['Index']}"] = cb_func

            err_func = Function(
                name="callback_error",
                return_type="void",
                formal_parameters=[f"{choice['CallbackTypeState']}*", "Error err"],
            )
            err_func.end_is_unreachable = True
            err_func.indent(self.code_indent)
            self.probe_enter(err_func, self.actor.name, choice["Index"])
            err_func.write_line("{0};", exit_func.call())
            self.try_catch(
                cx.with_target(err_func),
                cx.catch_f_err,
                cx.try_loop_depth,
                lambda: err_func.write_line("{0};", cx.catch_f_err.call("err", "0")),
                False,
            )
            self.probe_exit(err_func, self.actor.name, choice["Index"])
            self.functions[f"{err_func.name}#{choice['Index']}"] = err_func

        first_choice = True
        for choice in choices:
            get_func = "pop" if choice["Stmt"].wait.is_wait_next else "get"
            self.line_numberFunction(cx.target, choice["Stmt"].wait.first_source_line)
            if choice["Stmt"].wait.is_wait_next:
                cx.target.write_line(
                    "auto {0} = {1};",
                    choice["Future"],
                    choice["Stmt"].wait.future_expression,
                )
                cx.target.write_line(
                    'static_assert(std::is_same<decltype({0}), FutureStream<{1}>>::value || std::is_same<decltype({0}), ThreadFutureStream<{1}>>::value, "invalid type");',
                    choice["Future"],
                    choice["Stmt"].wait.result.type,
                )
            else:
                cx.target.write_line(
                    "StrictFuture<{0}> {1} = {2};",
                    choice["Stmt"].wait.result.type,
                    choice["Future"],
                    choice["Stmt"].wait.future_expression,
                )
            if first_choice:
                first_choice = False
                self.line_numberFunction(cx.target, stmt.first_source_line)
                if self.actor.is_cancellable():
                    cx.target.write_line(
                        "if ({1}->actor_wait_state < 0) return {0};",
                        cx.catch_f_err.call(
                            "actor_cancelled()", self.adjust_loop_depth(cx.try_loop_depth)
                        ),
                        self.this,
                    )
            cx.target.write_line(
                "if ({0}.isReady()) {{ if ({0}.isError()) return {2}; else return {1}; }};",
                choice["Future"],
                choice["Body"].call(f"{choice['Future']}.{get_func}()", "loopDepth"),
                cx.catch_f_err.call(
                    f"{choice['Future']}.getError()",
                    self.adjust_loop_depth(cx.try_loop_depth),
                ),
            )
        cx.target.write_line("{1}->actor_wait_state = {0};", group, self.this)
        for choice in choices:
            self.line_numberFunction(cx.target, choice["Stmt"].wait.first_source_line)
            cx.target.write_line(
                "{0}.addCallbackAndClear(static_cast<{1}*>({2}));",
                choice["Future"],
                choice["CallbackTypeState"],
                self.this,
            )
        cx.target.write_line("loopDepth = 0;")
        if not reachable:
            cx.unreachable()

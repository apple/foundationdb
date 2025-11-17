from __future__ import annotations

import hashlib
import io
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from . import ActorCompilerError
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
        self.returnType = return_type
        self.formalParameters = list(formal_parameters or [])
        self.endIsUnreachable = False
        self.exceptionParameterIs: Optional[str] = None
        self.publicName = False
        self.specifiers = ""
        self._indentation = ""
        self._body = io.StringIO()
        self.wasCalled = False
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
            return_type=self.returnType,
            formal_parameters=formal_parameters,
        )
        overload.endIsUnreachable = self.endIsUnreachable
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
        self.wasCalled = True
        return self.name if self.publicName else f"a_{self.name}"

    def call(self, *parameters: str) -> str:
        params = ", ".join(parameters)
        return f"{self.use_by_name()}({params})"


class LiteralBreak(Function):
    def __init__(self) -> None:
        super().__init__(name="break!", return_type="")

    def call(self, *parameters: str) -> str:
        if parameters:
            raise ActorCompilerError(0, "LiteralBreak called with parameters!")
        self.wasCalled = True
        return "break"


class LiteralContinue(Function):
    def __init__(self) -> None:
        super().__init__(name="continue!", return_type="")

    def call(self, *parameters: str) -> str:
        if parameters:
            raise ActorCompilerError(0, "LiteralContinue called with parameters!")
        self.wasCalled = True
        return "continue"


@dataclass
class StateVar:
    type: str = ""
    name: str = ""
    initializer: Optional[str] = None
    initializerConstructorSyntax: bool = False
    SourceLine: int = 0


@dataclass
class CallbackVar:
    type: str = ""
    CallbackGroup: int = 0
    SourceLine: int = 0


class Context:
    def __init__(
        self,
        target: Optional[Function] = None,
        next_func: Optional[Function] = None,
        breakF: Optional[Function] = None,
        continueF: Optional[Function] = None,
        catchFErr: Optional[Function] = None,
        tryLoopDepth: int = 0,
    ) -> None:
        self.target = target
        self.next = next_func
        self.breakF = breakF
        self.continueF = continueF
        self.catchFErr = catchFErr
        self.tryLoopDepth = tryLoopDepth

    def unreachable(self) -> None:
        self.target = None

    def with_target(self, new_target: Function) -> "Context":
        return Context(
            target=new_target,
            breakF=self.breakF,
            continueF=self.continueF,
            catchFErr=self.catchFErr,
            tryLoopDepth=self.tryLoopDepth,
        )

    def loop_context(
        self,
        new_target: Function,
        breakF: Function,
        continueF: Function,
        deltaLoopDepth: int,
    ) -> "Context":
        return Context(
            target=new_target,
            breakF=breakF,
            continueF=continueF,
            catchFErr=self.catchFErr,
            tryLoopDepth=self.tryLoopDepth + deltaLoopDepth,
        )

    def with_catch(self, new_catch: Function) -> "Context":
        return Context(
            target=self.target,
            breakF=self.breakF,
            continueF=self.continueF,
            catchFErr=new_catch,
        )

    def clone(self) -> "Context":
        return Context(
            target=self.target,
            next_func=self.next,
            breakF=self.breakF,
            continueF=self.continueF,
            catchFErr=self.catchFErr,
            tryLoopDepth=self.tryLoopDepth,
        )

    def copy_from(self, other: "Context") -> None:
        self.target = other.target
        self.next = other.next
        self.breakF = other.breakF
        self.continueF = other.continueF
        self.catchFErr = other.catchFErr
        self.tryLoopDepth = other.tryLoopDepth


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
        if self.descr.superClassList:
            writer.write(
                f"{indent}struct {self.descr.name} : {self.descr.superClassList} {{\n"
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
    loopDepth0 = "int loopDepth=0"
    loopDepth = "int loopDepth"
    codeIndent = 2
    usedClassNames: set[str] = set()

    def __init__(
        self,
        actor: Actor,
        source_file: str,
        is_top_level: bool,
        line_numbers_enabled: bool,
        generate_probes: bool,
    ) -> None:
        self.actor = actor
        self.sourceFile = source_file
        self.isTopLevel = is_top_level
        self.LineNumbersEnabled = line_numbers_enabled
        self.generateProbes = generate_probes
        self.className = ""
        self.fullClassName = ""
        self.stateClassName = ""
        self.state: List[StateVar] = []
        self.callbacks: List[CallbackVar] = []
        self.chooseGroups = 0
        self.whenCount = 0
        self.This = ""
        self.uidObjects: Dict[Tuple[int, int], str] = {}
        self.functions: Dict[str, Function] = {}
        self.iterators: Dict[str, int] = {}
        self.FindState()

    def ByteToLong(self, data: bytes) -> int:
        result = 0
        for b in data:
            result += b
            result <<= 8
        return result & ((1 << 64) - 1)

    def GetUidFromString(self, value: str) -> Tuple[int, int]:
        digest = hashlib.sha256(value.encode("utf-8")).digest()
        first = self.ByteToLong(digest[:8])
        second = self.ByteToLong(digest[8:16])
        return (first, second)

    def WriteActorFunction(self, writer, full_return_type: str) -> None:
        self.WriteTemplate(writer)
        self.LineNumber(writer, self.actor.SourceLine)
        for attribute in self.actor.attributes:
            writer.write(f"{attribute} ")
        if self.actor.isStatic:
            writer.write("static ")
        namespace_prefix = (
            "" if self.actor.nameSpace is None else f"{self.actor.nameSpace}::"
        )
        params = ", ".join(self.ParameterList())
        writer.write(
            f"{full_return_type} {namespace_prefix}{self.actor.name}( {params} ) {{\n"
        )
        self.LineNumber(writer, self.actor.SourceLine)
        ctor_args = ", ".join(param.name for param in self.actor.parameters)
        new_actor = f"new {self.fullClassName}({ctor_args})"
        if self.actor.returnType is not None:
            writer.write(f"\treturn Future<{self.actor.returnType}>({new_actor});\n")
        else:
            writer.write(f"\t{new_actor};\n")
        writer.write("}\n")

    def WriteActorClass(
        self, writer, full_state_class_name: str, body: Function
    ) -> None:
        writer.write(
            f"// This generated class is to be used only via {self.actor.name}()\n"
        )
        self.WriteTemplate(writer)
        self.LineNumber(writer, self.actor.SourceLine)
        callback_bases = ", ".join(f"public {cb.type}" for cb in self.callbacks)
        if callback_bases:
            callback_bases += ", "
        writer.write(
            "class {0} final : public Actor<{2}>, {3}public FastAllocated<{1}>, public {4} {{\n".format(
                self.className,
                self.fullClassName,
                "void" if self.actor.returnType is None else self.actor.returnType,
                callback_bases,
                full_state_class_name,
            )
        )
        writer.write("public:\n")
        writer.write(f"\tusing FastAllocated<{self.fullClassName}>::operator new;\n")
        writer.write(f"\tusing FastAllocated<{self.fullClassName}>::operator delete;\n")
        actor_identifier_key = f"{self.sourceFile}:{self.actor.name}"
        uid = self.GetUidFromString(actor_identifier_key)
        self.uidObjects[(uid[0], uid[1])] = actor_identifier_key
        writer.write(
            f"\tstatic constexpr ActorIdentifier __actorIdentifier = UID({uid[0]}UL, {uid[1]}UL);\n"
        )
        writer.write("\tActiveActorHelper activeActorHelper;\n")
        writer.write("#pragma clang diagnostic push\n")
        writer.write('#pragma clang diagnostic ignored "-Wdelete-non-virtual-dtor"\n')
        if self.actor.returnType is not None:
            writer.write("    void destroy() override {\n")
            writer.write("        activeActorHelper.~ActiveActorHelper();\n")
            writer.write(
                f"        static_cast<Actor<{self.actor.returnType}>*>(this)->~Actor();\n"
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
        self.LineNumber(writer, self.actor.SourceLine)
        self.WriteConstructor(body, writer, full_state_class_name)
        self.WriteCancelFunc(writer)
        writer.write("};\n")

    def Write(self, writer) -> None:
        full_return_type = (
            f"Future<{self.actor.returnType}>"
            if self.actor.returnType is not None
            else "void"
        )
        for i in range(1 << 16):
            class_name = "{3}{0}{1}Actor{2}".format(
                self.actor.name[:1].upper(),
                self.actor.name[1:],
                str(i) if i != 0 else "",
                (
                    self.actor.enclosingClass.replace("::", "_") + "_"
                    if self.actor.enclosingClass is not None
                    and self.actor.isForwardDeclaration
                    else (
                        self.actor.nameSpace.replace("::", "_") + "_"
                        if self.actor.nameSpace is not None
                        else ""
                    )
                ),
            )
            if self.actor.isForwardDeclaration:
                self.className = class_name
                break
            if class_name not in ActorCompiler.usedClassNames:
                self.className = class_name
                ActorCompiler.usedClassNames.add(class_name)
                break
        self.fullClassName = self.className + self.GetTemplateActuals()
        actor_class_formal = VarDeclaration(name=self.className, type="class")
        self.This = f"static_cast<{actor_class_formal.name}*>(this)"
        self.stateClassName = self.className + "State"
        full_state = self.stateClassName + self.GetTemplateActuals(
            VarDeclaration(type="class", name=self.fullClassName)
        )
        if self.actor.isForwardDeclaration:
            for attribute in self.actor.attributes:
                writer.write(f"{attribute} ")
            if self.actor.isStatic:
                writer.write("static ")
            ns = "" if self.actor.nameSpace is None else f"{self.actor.nameSpace}::"
            writer.write(
                f"{full_return_type} {ns}{self.actor.name}( {', '.join(self.ParameterList())} );\n"
            )
            if self.actor.enclosingClass is not None:
                writer.write(f"template <class> friend class {self.stateClassName};\n")
            return
        body = self.getFunction("", "body", self.loopDepth0)
        body_context = Context(
            target=body,
            catchFErr=self.getFunction(
                body.name, "Catch", "Error error", self.loopDepth0
            ),
        )
        end_context = self.TryCatchCompile(self.actor.body, body_context)
        if end_context.target is not None:
            if self.actor.returnType is None:
                self.compile_statement(
                    ReturnStatement(
                        FirstSourceLine=self.actor.SourceLine, expression=""
                    ),
                    end_context,
                )
            else:
                raise ActorCompilerError(
                    self.actor.SourceLine,
                    "Actor {0} fails to return a value",
                    self.actor.name,
                )
        if self.actor.returnType is not None:
            body_context.catchFErr.write_line(f"this->~{self.stateClassName}();")
            body_context.catchFErr.write_line(
                f"{self.This}->sendErrorAndDelPromiseRef(error);"
            )
        else:
            body_context.catchFErr.write_line(f"delete {self.This};")
        body_context.catchFErr.write_line("loopDepth = 0;")
        if self.isTopLevel and self.actor.nameSpace is None:
            writer.write("namespace {\n")
        writer.write(
            f"// This generated class is to be used only via {self.actor.name}()\n"
        )
        self.WriteTemplate(writer, actor_class_formal)
        self.LineNumber(writer, self.actor.SourceLine)
        writer.write(f"class {self.stateClassName} {{\n")
        writer.write("public:\n")
        self.LineNumber(writer, self.actor.SourceLine)
        self.WriteStateConstructor(writer)
        self.WriteStateDestructor(writer)
        self.WriteFunctions(writer)
        for st in self.state:
            self.LineNumber(writer, st.SourceLine)
            writer.write(f"\t{st.type} {st.name};\n")
        writer.write("};\n")
        self.WriteActorClass(writer, full_state, body)
        if self.isTopLevel and self.actor.nameSpace is None:
            writer.write("} // namespace\n")
        self.WriteActorFunction(writer, full_return_type)
        if self.actor.testCaseParameters is not None:
            writer.write(
                f"ACTOR_TEST_CASE({self.actor.name}, {self.actor.testCaseParameters})\n"
            )

    thisAddress = "reinterpret_cast<unsigned long>(this)"

    def ProbeEnter(self, fun: Function, name: str, index: int = -1) -> None:
        if self.generateProbes:
            fun.write_line(
                'fdb_probe_actor_enter("{0}", {1}, {2});',
                name,
                self.thisAddress,
                index,
            )
        block_identifier = self.GetUidFromString(fun.name)
        fun.write_line("#ifdef WITH_ACAC")
        fun.write_line(
            "static constexpr ActorBlockIdentifier __identifier = UID({0}UL, {1}UL);",
            block_identifier[0],
            block_identifier[1],
        )
        fun.write_line(
            "ActorExecutionContextHelper __helper(static_cast<{0}*>(this)->activeActorHelper.actorID, __identifier);",
            self.className,
        )
        fun.write_line("#endif // WITH_ACAC")

    def ProbeExit(self, fun: Function, name: str, index: int = -1) -> None:
        if self.generateProbes:
            fun.write_line(
                'fdb_probe_actor_exit("{0}", {1}, {2});',
                name,
                self.thisAddress,
                index,
            )

    def ProbeCreate(self, fun: Function, name: str) -> None:
        if self.generateProbes:
            fun.write_line(
                'fdb_probe_actor_create("{0}", {1});', name, self.thisAddress
            )

    def ProbeDestroy(self, fun: Function, name: str) -> None:
        if self.generateProbes:
            fun.write_line(
                'fdb_probe_actor_destroy("{0}", {1});',
                name,
                self.thisAddress,
            )

    def LineNumber(self, writer, source_line: int) -> None:
        if source_line == 0:
            raise ActorCompilerError(0, "Invalid source line (0)")
        if self.LineNumbersEnabled:
            writer.write(
                '\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} "{1}"\n'.format(
                    source_line, self.sourceFile
                )
            )

    def LineNumberFunction(self, func: Function, source_line: int) -> None:
        if source_line == 0:
            raise ActorCompilerError(0, "Invalid source line (0)")
        if self.LineNumbersEnabled:
            func.write_line_unindented(
                '\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} "{1}"'.format(
                    source_line, self.sourceFile
                )
            )

    def TryCatch(
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
                        "error", self.AdjustLoopDepth(catch_loop_depth)
                    ),
                )
            else:
                cx.target.write_line("\t{0};", catch_function.call("error", "0"))
            cx.target.write_line("} catch (...) {")
            if use_loop_depth:
                cx.target.write_line(
                    "\tloopDepth = {0};",
                    catch_function.call(
                        "unknown_error()", self.AdjustLoopDepth(catch_loop_depth)
                    ),
                )
            else:
                cx.target.write_line(
                    "\t{0};", catch_function.call("unknown_error()", "0")
                )
            cx.target.write_line("}")

    def TryCatchCompile(self, block: CodeBlock, cx: Context) -> Context:
        result_holder = {"ctx": cx}

        def action() -> None:
            result_holder["ctx"] = self._try_catch_compile_body(block, cx)

        self.TryCatch(cx, cx.catchFErr, cx.tryLoopDepth, action)
        return result_holder["ctx"]

    def _try_catch_compile_body(self, block: CodeBlock, cx: Context) -> Context:
        compiled = self.Compile(block, cx, True)
        if compiled.target is not None:
            next_func = self.getFunction(compiled.target.name, "cont", self.loopDepth)
            compiled.target.write_line("loopDepth = {0};", next_func.call("loopDepth"))
            compiled.target = next_func
            compiled.next = None
        return compiled

    def WriteTemplate(self, writer, *extra_parameters: VarDeclaration) -> None:
        formals = list(self.actor.templateFormals or []) + list(extra_parameters)
        if not formals:
            return
        self.LineNumber(writer, self.actor.SourceLine)
        writer.write(
            "template <{0}>\n".format(", ".join(f"{p.type} {p.name}" for p in formals))
        )

    def GetTemplateActuals(self, *extra_parameters: VarDeclaration) -> str:
        formals = list(self.actor.templateFormals or []) + list(extra_parameters)
        if not formals:
            return ""
        return "<{0}>".format(", ".join(p.name for p in formals))

    def WillContinue(self, stmt: Statement) -> bool:
        return any(
            isinstance(sub, (ChooseStatement, WaitStatement, TryStatement))
            for sub in self.Flatten(stmt)
        )

    def AsCodeBlock(self, statement: Statement) -> CodeBlock:
        if isinstance(statement, CodeBlock):
            return statement
        return CodeBlock(statements=[statement])

    def Flatten(self, stmt: Optional[Statement]) -> Iterable[Statement]:
        if stmt is None:
            return []

        def _flatten(s: Statement) -> Iterable[Statement]:
            yield s
            if isinstance(s, LoopStatement):
                yield from self.Flatten(s.body)
            elif isinstance(s, WhileStatement):
                yield from self.Flatten(s.body)
            elif isinstance(s, ForStatement):
                yield from self.Flatten(s.body)
            elif isinstance(s, RangeForStatement):
                yield from self.Flatten(s.body)
            elif isinstance(s, CodeBlock):
                for child in s.statements:
                    yield from self.Flatten(child)
            elif isinstance(s, IfStatement):
                yield from self.Flatten(s.ifBody)
                if s.elseBody:
                    yield from self.Flatten(s.elseBody)
            elif isinstance(s, ChooseStatement):
                yield from self.Flatten(s.body)
            elif isinstance(s, WhenStatement):
                if s.body:
                    yield from self.Flatten(s.body)
            elif isinstance(s, TryStatement):
                yield from self.Flatten(s.tryBody)
                for c in s.catches:
                    yield from self.Flatten(c.body)

        return list(_flatten(stmt))

    def FindState(self) -> None:
        self.state = [
            StateVar(
                SourceLine=self.actor.SourceLine,
                name=p.name,
                type=p.type,
                initializer=p.name,
            )
            for p in self.actor.parameters
        ]

    def AdjustLoopDepth(self, subtract: int) -> str:
        if subtract == 0:
            return "loopDepth"
        return f"std::max(0, loopDepth - {subtract})"

    def ParameterList(self) -> List[str]:
        params = []
        for p in self.actor.parameters:
            if p.initializer:
                params.append(f"{p.type} const& {p.name} = {p.initializer}")
            else:
                params.append(f"{p.type} const& {p.name}")
        return params

    def getFunction(
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
        func.indent(self.codeIndent)
        if overload_formal_parameters:
            func.add_overload(*overload_formal_parameters)
        self.functions[func.name] = func
        return func

    def WriteFunctions(self, writer) -> None:
        for func in self.functions.values():
            body = func.body_text
            if body:
                self.WriteFunction(writer, func, body)
            if func.overload:
                overload_body = func.overload.body_text
                if overload_body:
                    self.WriteFunction(writer, func.overload, overload_body)

    def WriteFunction(self, writer, func: Function, body: str) -> None:
        spec = "" if not func.specifiers else f" {func.specifiers}"
        trailing = " " if spec == "" else ""
        signature = (
            f"{self.member_indent_str}"
            f"{'' if func.returnType == '' else f'{func.returnType} '}"
            f"{func.use_by_name()}({','.join(func.formalParameters)})"
            f"{spec}{trailing}\n"
        )
        writer.write(signature)
        if func.returnType != "":
            writer.write(f"{self.member_indent_str}{{\n")
        writer.write(body)
        writer.write("\n")
        if not func.endIsUnreachable:
            writer.write(f"{self.member_indent_str}\treturn loopDepth;\n")
        writer.write(f"{self.member_indent_str}}}\n")

    def WriteCancelFunc(self, writer) -> None:
        if not self.actor.IsCancellable():
            return
        cancel_func = Function(
            name="cancel",
            return_type="void",
            formal_parameters=[],
        )
        cancel_func.endIsUnreachable = True
        cancel_func.publicName = True
        cancel_func.specifiers = "override"
        cancel_func.indent(self.codeIndent)
        cancel_func.write_line("auto wait_state = this->actor_wait_state;")
        cancel_func.write_line("this->actor_wait_state = -1;")
        cancel_func.write_line("switch (wait_state) {")
        last_group = -1
        for cb in sorted(self.callbacks, key=lambda c: c.CallbackGroup):
            if cb.CallbackGroup != last_group:
                last_group = cb.CallbackGroup
                cancel_func.write_line(
                    "case {0}: this->a_callback_error(({1}*)0, actor_cancelled()); break;",
                    cb.CallbackGroup,
                    cb.type,
                )
        cancel_func.write_line("}")
        self.WriteFunction(writer, cancel_func, cancel_func.body_text)

    def WriteConstructor(
        self, body: Function, writer, full_state_class_name: str
    ) -> None:
        constructor = Function(
            name=self.className,
            return_type="",
            formal_parameters=self.ParameterList(),
        )
        constructor.endIsUnreachable = True
        constructor.publicName = True
        constructor.indent(self.codeIndent)
        constructor.write_line(
            " : Actor<{0}>(),".format(
                "void" if self.actor.returnType is None else self.actor.returnType
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
        self.ProbeEnter(constructor, self.actor.name)
        constructor.write_line("#ifdef ENABLE_SAMPLING")
        constructor.write_line('this->lineage.setActorName("{0}");', self.actor.name)
        constructor.write_line("LineageScope _(&this->lineage);")
        constructor.write_line("#endif")
        constructor.write_line("this->{0};", body.call())
        self.ProbeExit(constructor, self.actor.name)
        self.WriteFunction(writer, constructor, constructor.body_text)

    def WriteStateConstructor(self, writer) -> None:
        constructor = Function(
            name=self.stateClassName,
            return_type="",
            formal_parameters=self.ParameterList(),
        )
        constructor.endIsUnreachable = True
        constructor.publicName = True
        constructor.indent(self.codeIndent)
        ini = None
        line = self.actor.SourceLine
        for state_var in self.state:
            if state_var.initializer is None:
                continue
            self.LineNumberFunction(constructor, line)
            if ini is None:
                ini = " : "
            else:
                constructor.write_line(ini + ",")
                ini = "   "
            ini += f"{state_var.name}({state_var.initializer})"
            line = state_var.SourceLine
        self.LineNumberFunction(constructor, line)
        if ini:
            constructor.write_line(ini)
        constructor.indent(-1)
        constructor.write_line("{")
        constructor.indent(+1)
        self.ProbeCreate(constructor, self.actor.name)
        self.WriteFunction(writer, constructor, constructor.body_text)

    def WriteStateDestructor(self, writer) -> None:
        destructor = Function(
            name=f"~{self.stateClassName}",
            return_type="",
            formal_parameters=[],
        )
        destructor.endIsUnreachable = True
        destructor.publicName = True
        destructor.indent(self.codeIndent)
        destructor.indent(-1)
        destructor.write_line("{")
        destructor.indent(+1)
        self.ProbeDestroy(destructor, self.actor.name)
        self.WriteFunction(writer, destructor, destructor.body_text)

    def Compile(
        self, block: CodeBlock, context: Context, ok_to_continue: bool = True
    ) -> Context:
        cx = context.clone()
        cx.next = None
        for stmt in block.statements:
            if cx.target is None:
                raise ActorCompilerError(stmt.FirstSourceLine, "Unreachable code.")
            if cx.next is None:
                cx.next = self.getFunction(cx.target.name, "cont", self.loopDepth)
            self.compile_statement(stmt, cx)
            if cx.next.wasCalled:
                if cx.target is None:
                    raise ActorCompilerError(
                        stmt.FirstSourceLine, "Unreachable continuation called?"
                    )
                if not ok_to_continue:
                    raise ActorCompilerError(
                        stmt.FirstSourceLine, "Unexpected continuation"
                    )
                cx.target = cx.next
                cx.next = None
        return cx

    def compile_statement(self, stmt: Statement, cx: Context) -> None:
        handler_name = f"_compile_{stmt.__class__.__name__}"
        handler = getattr(self, handler_name, None)
        if handler is None:
            raise ActorCompilerError(
                stmt.FirstSourceLine,
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
        self.LineNumberFunction(cx.target, stmt.FirstSourceLine)
        cx.target.write_line(stmt.code)

    def _compile_StateDeclarationStatement(
        self, stmt: StateDeclarationStatement, cx: Context
    ) -> None:
        if self._is_top_level_state_decl(stmt):
            self.state.append(
                StateVar(
                    SourceLine=stmt.FirstSourceLine,
                    name=stmt.decl.name,
                    type=stmt.decl.type,
                    initializer=stmt.decl.initializer,
                    initializerConstructorSyntax=stmt.decl.initializerConstructorSyntax,
                )
            )
        else:
            self.state.append(
                StateVar(
                    SourceLine=stmt.FirstSourceLine,
                    name=stmt.decl.name,
                    type=stmt.decl.type,
                    initializer=None,
                )
            )
            if stmt.decl.initializer is not None:
                self.LineNumberFunction(cx.target, stmt.FirstSourceLine)
                if (
                    stmt.decl.initializerConstructorSyntax
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

    def getIteratorName(self, cx: Context) -> str:
        name = f"RangeFor{cx.target.name}Iterator"
        self.iterators.setdefault(name, 0)
        idx = self.iterators[name]
        self.iterators[name] += 1
        return f"{name}{idx}"

    def EmitNativeLoop(
        self, source_line: int, head: str, body: Statement, cx: Context
    ) -> bool:
        self.LineNumberFunction(cx.target, source_line)
        cx.target.write_line(head + " {")
        cx.target.indent(+1)
        literal_break = LiteralBreak()
        literal_continue = LiteralContinue()
        self.Compile(
            self.AsCodeBlock(body),
            cx.loop_context(cx.target, literal_break, literal_continue, 0),
            True,
        )
        cx.target.indent(-1)
        cx.target.write_line("}")
        return not literal_break.wasCalled

    def _compile_ForStatement(self, stmt: ForStatement, cx: Context) -> None:
        no_condition = stmt.condExpression in ("", "true", "1")
        if not self.WillContinue(stmt.body):
            if (
                self.EmitNativeLoop(
                    stmt.FirstSourceLine,
                    f"for({stmt.initExpression};{stmt.condExpression};{stmt.nextExpression})",
                    stmt.body,
                    cx,
                )
                and no_condition
            ):
                cx.unreachable()
            return

        init_stmt = PlainOldCodeStatement(
            code=f"{stmt.initExpression};", FirstSourceLine=stmt.FirstSourceLine
        )
        self._compile_PlainOldCodeStatement(init_stmt, cx)

        if no_condition:
            full_body = stmt.body
        else:
            condition = IfStatement(
                expression=f"!({stmt.condExpression})",
                ifBody=BreakStatement(FirstSourceLine=stmt.FirstSourceLine),
                FirstSourceLine=stmt.FirstSourceLine,
            )
            full_body = CodeBlock(
                statements=[condition] + list(self.AsCodeBlock(stmt.body).statements),
                FirstSourceLine=stmt.FirstSourceLine,
            )

        loopF = self.getFunction(cx.target.name, "loopHead", self.loopDepth)
        loopBody = self.getFunction(cx.target.name, "loopBody", self.loopDepth)
        breakF = self.getFunction(cx.target.name, "break", self.loopDepth)
        continueF = (
            loopF
            if stmt.nextExpression == ""
            else self.getFunction(cx.target.name, "continue", self.loopDepth)
        )

        loopF.write_line("int oldLoopDepth = ++loopDepth;")
        loopF.write_line(
            "while (loopDepth == oldLoopDepth) loopDepth = {0};",
            loopBody.call("loopDepth"),
        )

        endLoop = self.Compile(
            self.AsCodeBlock(full_body),
            cx.loop_context(loopBody, breakF, continueF, +1),
            True,
        ).target

        if endLoop is not None and endLoop is not loopBody:
            if stmt.nextExpression:
                self._compile_PlainOldCodeStatement(
                    PlainOldCodeStatement(
                        code=f"{stmt.nextExpression};",
                        FirstSourceLine=stmt.FirstSourceLine,
                    ),
                    cx.with_target(endLoop),
                )
            endLoop.write_line("if (loopDepth == 0) return {0};", loopF.call("0"))

        cx.target.write_line("loopDepth = {0};", loopF.call("loopDepth"))

        if continueF is not loopF and continueF.wasCalled:
            self._compile_PlainOldCodeStatement(
                PlainOldCodeStatement(
                    code=f"{stmt.nextExpression};", FirstSourceLine=stmt.FirstSourceLine
                ),
                cx.with_target(continueF),
            )
            continueF.write_line("if (loopDepth == 0) return {0};", loopF.call("0"))

        if breakF.wasCalled:
            self.TryCatch(
                cx.with_target(breakF),
                cx.catchFErr,
                cx.tryLoopDepth,
                lambda: breakF.write_line("return {0};", cx.next.call("loopDepth")),
            )
        else:
            cx.unreachable()

    def _compile_RangeForStatement(self, stmt: RangeForStatement, cx: Context) -> None:
        if self.WillContinue(stmt.body):
            container = next(
                (s for s in self.state if s.name == stmt.rangeExpression), None
            )
            if container is None:
                raise ActorCompilerError(
                    stmt.FirstSourceLine,
                    "container of range-based for with continuation must be a state variable",
                )
            iterator_name = self.getIteratorName(cx)
            self.state.append(
                StateVar(
                    SourceLine=stmt.FirstSourceLine,
                    name=iterator_name,
                    type=f"decltype(std::begin(std::declval<{container.type}>()))",
                    initializer=None,
                )
            )
            equivalent = ForStatement(
                initExpression=f"{iterator_name} = std::begin({stmt.rangeExpression})",
                condExpression=f"{iterator_name} != std::end({stmt.rangeExpression})",
                nextExpression=f"++{iterator_name}",
                FirstSourceLine=stmt.FirstSourceLine,
                body=CodeBlock(
                    statements=[
                        PlainOldCodeStatement(
                            code=f"{stmt.rangeDecl} = *{iterator_name};",
                            FirstSourceLine=stmt.FirstSourceLine,
                        ),
                        stmt.body,
                    ],
                    FirstSourceLine=stmt.FirstSourceLine,
                ),
            )
            self._compile_ForStatement(equivalent, cx)
        else:
            self.EmitNativeLoop(
                stmt.FirstSourceLine,
                f"for( {stmt.rangeDecl} : {stmt.rangeExpression} )",
                stmt.body,
                cx,
            )

    def _compile_WhileStatement(self, stmt: WhileStatement, cx: Context) -> None:
        equivalent = ForStatement(
            initExpression="",
            condExpression=stmt.expression,
            nextExpression="",
            body=stmt.body,
            FirstSourceLine=stmt.FirstSourceLine,
        )
        self._compile_ForStatement(equivalent, cx)

    def _compile_LoopStatement(self, stmt: LoopStatement, cx: Context) -> None:
        equivalent = ForStatement(
            initExpression="",
            condExpression="",
            nextExpression="",
            body=stmt.body,
            FirstSourceLine=stmt.FirstSourceLine,
        )
        self._compile_ForStatement(equivalent, cx)

    def _compile_BreakStatement(self, stmt: BreakStatement, cx: Context) -> None:
        if cx.breakF is None:
            raise ActorCompilerError(stmt.FirstSourceLine, "break outside loop")
        if isinstance(cx.breakF, LiteralBreak):
            cx.target.write_line("{0};", cx.breakF.call())
        else:
            cx.target.write_line(
                "return {0}; // break", cx.breakF.call("loopDepth==0?0:loopDepth-1")
            )
        cx.unreachable()

    def _compile_ContinueStatement(self, stmt: ContinueStatement, cx: Context) -> None:
        if cx.continueF is None:
            raise ActorCompilerError(stmt.FirstSourceLine, "continue outside loop")
        if isinstance(cx.continueF, LiteralContinue):
            cx.target.write_line("{0};", cx.continueF.call())
        else:
            cx.target.write_line(
                "return {0}; // continue", cx.continueF.call("loopDepth")
            )
        cx.unreachable()

    def _compile_CodeBlock(self, stmt: CodeBlock, cx: Context) -> None:
        cx.target.write_line("{")
        cx.target.indent(+1)
        end = self.Compile(stmt, cx, True)
        cx.target.indent(-1)
        cx.target.write_line("}")
        if end.target is None:
            cx.unreachable()
        elif end.target is not cx.target:
            end.target.write_line("loopDepth = {0};", cx.next.call("loopDepth"))

    def _compile_ReturnStatement(self, stmt: ReturnStatement, cx: Context) -> None:
        self.LineNumberFunction(cx.target, stmt.FirstSourceLine)
        if (stmt.expression == "") != (self.actor.returnType is None):
            raise ActorCompilerError(
                stmt.FirstSourceLine,
                "Return statement does not match actor declaration",
            )
        if self.actor.returnType is not None:
            if stmt.expression == "Never()":
                cx.target.write_line("this->~{0}();", self.stateClassName)
                cx.target.write_line("{0}->sendAndDelPromiseRef(Never());", self.This)
            else:
                cx.target.write_line(
                    "if (!{0}->SAV<{1}>::futures) {{ (void)({2}); this->~{3}(); {0}->destroy(); return 0; }}",
                    self.This,
                    self.actor.returnType,
                    stmt.expression,
                    self.stateClassName,
                )
                if any(s.name == stmt.expression for s in self.state):
                    cx.target.write_line(
                        "new (&{0}->SAV< {1} >::value()) {1}(std::move({2})); // state_var_RVO",
                        self.This,
                        self.actor.returnType,
                        stmt.expression,
                    )
                else:
                    cx.target.write_line(
                        "new (&{0}->SAV< {1} >::value()) {1}({2});",
                        self.This,
                        self.actor.returnType,
                        stmt.expression,
                    )
                cx.target.write_line("this->~{0}();", self.stateClassName)
                cx.target.write_line("{0}->finishSendAndDelPromiseRef();", self.This)
        else:
            cx.target.write_line("delete {0};", self.This)
        cx.target.write_line("return 0;")
        cx.unreachable()

    def _compile_ThrowStatement(self, stmt: ThrowStatement, cx: Context) -> None:
        self.LineNumberFunction(cx.target, stmt.FirstSourceLine)
        if stmt.expression == "":
            if cx.target.exceptionParameterIs is not None:
                cx.target.write_line(
                    "return {0};",
                    cx.catchFErr.call(
                        cx.target.exceptionParameterIs,
                        self.AdjustLoopDepth(cx.tryLoopDepth),
                    ),
                )
            else:
                raise ActorCompilerError(
                    stmt.FirstSourceLine,
                    "throw statement with no expression has no current exception in scope",
                )
        else:
            cx.target.write_line(
                "return {0};",
                cx.catchFErr.call(
                    stmt.expression, self.AdjustLoopDepth(cx.tryLoopDepth)
                ),
            )
        cx.unreachable()

    def _compile_IfStatement(self, stmt: IfStatement, cx: Context) -> None:
        use_continuation = self.WillContinue(stmt.ifBody) or (
            stmt.elseBody is not None and self.WillContinue(stmt.elseBody)
        )
        self.LineNumberFunction(cx.target, stmt.FirstSourceLine)
        constexpr = "constexpr " if stmt.constexpr else ""
        cx.target.write_line(f"if {constexpr}({stmt.expression})")
        cx.target.write_line("{")
        cx.target.indent(+1)
        if_target = self.Compile(
            self.AsCodeBlock(stmt.ifBody), cx, use_continuation
        ).target
        if use_continuation and if_target is not None:
            if_target.write_line("loopDepth = {0};", cx.next.call("loopDepth"))
        cx.target.indent(-1)
        cx.target.write_line("}")
        else_target = None
        if stmt.elseBody is not None or use_continuation:
            cx.target.write_line("else")
            cx.target.write_line("{")
            cx.target.indent(+1)
            else_target = cx.target
            if stmt.elseBody is not None:
                else_target = self.Compile(
                    self.AsCodeBlock(stmt.elseBody), cx, use_continuation
                ).target
            if use_continuation and else_target is not None:
                else_target.write_line("loopDepth = {0};", cx.next.call("loopDepth"))
            cx.target.indent(-1)
            cx.target.write_line("}")
        if if_target is None and stmt.elseBody is not None and else_target is None:
            cx.unreachable()
        elif not cx.next.wasCalled and use_continuation:
            raise ActorCompilerError(
                stmt.FirstSourceLine, "Internal error: IfStatement: next not called?"
            )

    def _compile_TryStatement(self, stmt: TryStatement, cx: Context) -> None:
        if len(stmt.catches) != 1:
            raise ActorCompilerError(
                stmt.FirstSourceLine,
                "try statement must have exactly one catch clause",
            )
        reachable = False
        catch_clause = stmt.catches[0]
        catch_expression = catch_clause.expression.replace(" ", "")
        catch_param = ""
        if catch_expression != "...":
            if not catch_expression.startswith("Error&"):
                raise ActorCompilerError(
                    catch_clause.FirstSourceLine,
                    "Only type 'Error' or '...' may be caught in an actor function",
                )
            catch_param = catch_expression[6:]
        if not catch_param:
            catch_param = "__current_error"
        catchFErr = self.getFunction(
            cx.target.name, "Catch", f"const Error& {catch_param}", self.loopDepth0
        )
        catchFErr.exceptionParameterIs = catch_param
        end = self.TryCatchCompile(
            self.AsCodeBlock(stmt.tryBody), cx.with_catch(catchFErr)
        )
        if end.target is not None:
            reachable = True
            self.TryCatch(
                end,
                cx.catchFErr,
                cx.tryLoopDepth,
                lambda: end.target.write_line(
                    "loopDepth = {0};", cx.next.call("loopDepth")
                ),
            )

        def handle_catch() -> None:
            nonlocal reachable
            cend = self._compile_try_catch_body(catch_clause, catchFErr, cx)
            if cend.target is not None:
                reachable = True

        self.TryCatch(
            cx.with_target(catchFErr),
            cx.catchFErr,
            cx.tryLoopDepth,
            handle_catch,
        )
        if not reachable:
            cx.unreachable()

    def _compile_try_catch_body(
        self, catch_clause: TryStatement.Catch, catchFErr: Function, cx: Context
    ) -> Context:
        cend = self.Compile(
            self.AsCodeBlock(catch_clause.body), cx.with_target(catchFErr), True
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
                        FirstSourceLine=stmt.FirstSourceLine,
                    )
                ],
                FirstSourceLine=stmt.FirstSourceLine,
            ),
            FirstSourceLine=stmt.FirstSourceLine,
        )
        if not stmt.resultIsState:
            cx.next.formalParameters = [
                f"{stmt.result.type} const& {stmt.result.name}",
                self.loopDepth,
            ]
            cx.next.add_overload(
                f"{stmt.result.type} && {stmt.result.name}", self.loopDepth
            )
        self._compile_ChooseStatement(equivalent, cx)

    def _compile_ChooseStatement(self, stmt: ChooseStatement, cx: Context) -> None:
        group = self.chooseGroups + 1
        self.chooseGroups = group
        codeblock = stmt.body if isinstance(stmt.body, CodeBlock) else None
        if codeblock is None:
            raise ActorCompilerError(
                stmt.FirstSourceLine,
                "'choose' must be followed by a compound statement.",
            )
        choices = []
        for idx, choice_stmt in enumerate(codeblock.statements):
            if not isinstance(choice_stmt, WhenStatement):
                raise ActorCompilerError(
                    choice_stmt.FirstSourceLine,
                    "only 'when' statements are valid in a 'choose' block.",
                )
            index = self.whenCount + idx
            param_prefix = "__" if choice_stmt.wait.resultIsState else ""
            const_param = f"{choice_stmt.wait.result.type} const& {param_prefix}{choice_stmt.wait.result.name}"
            rvalue_param = f"{choice_stmt.wait.result.type} && {param_prefix}{choice_stmt.wait.result.name}"
            body_func = self.getFunction(
                cx.target.name,
                "when",
                [const_param, self.loopDepth],
                overload_formal_parameters=[rvalue_param, self.loopDepth],
            )
            future_name = f"__when_expr_{index}"
            callback_template = (
                "ActorSingleCallback"
                if choice_stmt.wait.isWaitNext
                else "ActorCallback"
            )
            callback_type = f"{callback_template}< {self.fullClassName}, {index}, {choice_stmt.wait.result.type} >"
            callback_type_state = f"{callback_template}< {self.className}, {index}, {choice_stmt.wait.result.type} >"
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
        self.whenCount += len(choices)
        exit_func = self.getFunction("exitChoose", "", [])
        exit_func.returnType = "void"
        exit_func.write_line(
            "if ({0}->actor_wait_state > 0) {0}->actor_wait_state = 0;", self.This
        )
        for choice in choices:
            exit_func.write_line(
                "{0}->{1}::remove();", self.This, choice["CallbackTypeState"]
            )
        exit_func.endIsUnreachable = True

        reachable = False
        for choice in choices:
            self.callbacks.append(
                CallbackVar(
                    SourceLine=choice["Stmt"].FirstSourceLine,
                    CallbackGroup=choice["Group"],
                    type=choice["CallbackType"],
                )
            )
            r = choice["Body"]
            if choice["Stmt"].wait.resultIsState:
                overload = r.pop_overload()
                decl_stmt = StateDeclarationStatement(
                    decl=VarDeclaration(
                        type=choice["Stmt"].wait.result.type,
                        name=choice["Stmt"].wait.result.name,
                        initializer=f"__{choice['Stmt'].wait.result.name}",
                        initializerConstructorSyntax=False,
                    ),
                    FirstSourceLine=choice["Stmt"].FirstSourceLine,
                )
                self._compile_StateDeclarationStatement(decl_stmt, cx.with_target(r))
                if overload is not None:
                    overload.write_line(
                        "{0} = std::move(__{0});",
                        choice["Stmt"].wait.result.name,
                    )
                    r.set_overload(overload)
            if choice["Stmt"].body is not None:
                r = self.Compile(
                    self.AsCodeBlock(choice["Stmt"].body), cx.with_target(r), True
                ).target
            if r is not None:
                reachable = True
                if len(cx.next.formalParameters) == 1:
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
            cb_func.endIsUnreachable = True
            cb_func.add_overload(
                f"{choice['CallbackTypeState']}*",
                f"{choice['Stmt'].wait.result.type} && value",
            )
            cb_func.indent(self.codeIndent)
            self.ProbeEnter(cb_func, self.actor.name, choice["Index"])
            cb_func.write_line("{0};", exit_func.call())
            overload_func = cb_func.pop_overload()

            def fire_body(target_func: Function, expr: str, choice_dict=choice) -> None:
                self.TryCatch(
                    cx.with_target(target_func),
                    cx.catchFErr,
                    cx.tryLoopDepth,
                    lambda ch=choice_dict, tf=target_func, expression=expr: tf.write_line(
                        "{0};", ch["Body"].call(expression, "0")
                    ),
                    False,
                )

            fire_body(cb_func, "value")
            if overload_func is not None:
                fire_body(overload_func, "std::move(value)")
                cb_func.set_overload(overload_func)
            self.ProbeExit(cb_func, self.actor.name, choice["Index"])
            self.functions[f"{cb_func.name}#{choice['Index']}"] = cb_func

            err_func = Function(
                name="callback_error",
                return_type="void",
                formal_parameters=[f"{choice['CallbackTypeState']}*", "Error err"],
            )
            err_func.endIsUnreachable = True
            err_func.indent(self.codeIndent)
            self.ProbeEnter(err_func, self.actor.name, choice["Index"])
            err_func.write_line("{0};", exit_func.call())
            self.TryCatch(
                cx.with_target(err_func),
                cx.catchFErr,
                cx.tryLoopDepth,
                lambda: err_func.write_line("{0};", cx.catchFErr.call("err", "0")),
                False,
            )
            self.ProbeExit(err_func, self.actor.name, choice["Index"])
            self.functions[f"{err_func.name}#{choice['Index']}"] = err_func

        first_choice = True
        for choice in choices:
            get_func = "pop" if choice["Stmt"].wait.isWaitNext else "get"
            self.LineNumberFunction(cx.target, choice["Stmt"].wait.FirstSourceLine)
            if choice["Stmt"].wait.isWaitNext:
                cx.target.write_line(
                    "auto {0} = {1};",
                    choice["Future"],
                    choice["Stmt"].wait.futureExpression,
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
                    choice["Stmt"].wait.futureExpression,
                )
            if first_choice:
                first_choice = False
                self.LineNumberFunction(cx.target, stmt.FirstSourceLine)
                if self.actor.IsCancellable():
                    cx.target.write_line(
                        "if ({1}->actor_wait_state < 0) return {0};",
                        cx.catchFErr.call(
                            "actor_cancelled()", self.AdjustLoopDepth(cx.tryLoopDepth)
                        ),
                        self.This,
                    )
            cx.target.write_line(
                "if ({0}.isReady()) {{ if ({0}.isError()) return {2}; else return {1}; }};",
                choice["Future"],
                choice["Body"].call(f"{choice['Future']}.{get_func}()", "loopDepth"),
                cx.catchFErr.call(
                    f"{choice['Future']}.getError()",
                    self.AdjustLoopDepth(cx.tryLoopDepth),
                ),
            )
        cx.target.write_line("{1}->actor_wait_state = {0};", group, self.This)
        for choice in choices:
            self.LineNumberFunction(cx.target, choice["Stmt"].wait.FirstSourceLine)
            cx.target.write_line(
                "{0}.addCallbackAndClear(static_cast<{1}*>({2}));",
                choice["Future"],
                choice["CallbackTypeState"],
                self.This,
            )
        cx.target.write_line("loopDepth = 0;")
        if not reachable:
            cx.unreachable()

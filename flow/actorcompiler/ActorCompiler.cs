/*
 * ActorCompiler.cs
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Security.Cryptography;

namespace actorcompiler
{
    class TypeSwitch<R>
    {
        object value;
        R result;
        bool ok = false;
        public TypeSwitch(object t) { this.value = t; }
        public TypeSwitch<R> Case<T>(Func<T, R> f)
            where T : class
        {
            if (!ok)
            {
                var t = value as T;
                if (t != null)
                {
                    result = f(t);
                    ok = true;
                }
            }
            return this;
        }
        public R Return()
        {
            if (!ok) throw new Exception("Typeswitch didn't match.");
            return result;
        }
    }

    class Context
    {
        public Function target;
        public Function next;
        public Function breakF;
        public Function continueF;
        public Function catchFErr;        // Catch function taking Error
        public int tryLoopDepth = 0;    // The number of (loopDepth-increasing) loops entered inside the innermost try (thus, that will be exited by a throw)

        public void unreachable() { target = null; }

        // Usually we just change the target of a context
        public Context WithTarget(Function newTarget) { return new Context { target = newTarget, breakF = breakF, continueF = continueF, next = null, catchFErr = catchFErr, tryLoopDepth = tryLoopDepth }; }

        // When entering a loop, we have to provide new break and continue functions
        public Context LoopContext(Function newTarget, Function breakF, Function continueF, int deltaLoopDepth) { return new Context { target = newTarget, breakF = breakF, continueF = continueF, next = null, catchFErr = catchFErr, tryLoopDepth = tryLoopDepth + deltaLoopDepth }; }

        public Context WithCatch(Function newCatchFErr) { return new Context { target = target, breakF = breakF, continueF = continueF, next = null, catchFErr = newCatchFErr, tryLoopDepth = 0 }; }

        public Context Clone() { return new Context { target = target, next = next, breakF = breakF, continueF = continueF, catchFErr = catchFErr, tryLoopDepth = tryLoopDepth }; }
    };
    
    class Function
    {
        public string name;
        public string returnType;
        public string[] formalParameters;
        public bool endIsUnreachable = false;
        public string exceptionParameterIs = null;
        public bool publicName = false;
        public string specifiers;
        string indentation;
        StreamWriter body;
        public bool wasCalled { get; protected set; }
        public Function overload = null;

        public Function()
        {
            body = new StreamWriter(new MemoryStream());
        }

        public void setOverload(Function overload) {
            this.overload = overload;
        }

        public Function popOverload() {
            Function result = this.overload;
            this.overload = null;
            return result;
        }

        public void addOverload(params string[] formalParameters) {
            setOverload(
                new Function {
                    name = name,
                    returnType = returnType,
                    endIsUnreachable = endIsUnreachable,
                    formalParameters = formalParameters,
                    indentation = indentation
                }
            );
        }

        public void Indent(int change)
        {
            for(int i=0; i<change; i++) indentation += '\t';
            if (change < 0) indentation = indentation.Substring(-change);
            if (overload != null) {
                overload.Indent(change);
            }
        }

        public void WriteLineUnindented(string s)
        {
            body.WriteLine(s);
            if (overload != null) {
                overload.WriteLineUnindented(s);
            }
        }
        public void WriteLine(string line)
        {
            body.Write(indentation);
            body.WriteLine(line);
            if (overload != null) {
                overload.WriteLine(line);
            }
        }
        public void WriteLine(string line, params object[] args)
        {
            body.Write(indentation);
            body.WriteLine(line, args);
            if (overload != null) {
                overload.WriteLine(line, args);
            }
        }

        public string BodyText
        {
            get
            {
                body.Flush();
                body.BaseStream.Position = 0;
                return new StreamReader(body.BaseStream).ReadToEnd();
            }
        }
        public string useByName()
        {
            wasCalled = true;
            if (publicName)
                return name;
            else
                return "a_" + name;
        }
        public virtual string call(params string[] parameters)
        {
            return useByName() + "(" + string.Join(", ", parameters) + ")";
        }

        /* A (C++) continuation function for point P in the (actor) control flow graph
         *      int fP( [params,] int loopDepth = 0 );
         * has the following responsibilities:
         *      (A) If loopDepth==0, run the actor beginning at point P until
         *          (1) it waits, in which case set an appropriate callback and return 0, or
         *          (2) it returns, in which case destroy the actor and return 0.
         *      (B) If loopDepth>0, run the actor beginning at point P until
         *          (1) it waits, in which case set an appropriate callback and return 0, or
         *          (2) it returns, in which case destroy the actor and return 0, or
         *          (3) it reaches the bottom of the Nth innermost loop containing P, in which case 
         *                  return max(0, the given loopDepth - N)    (N=0 for the innermost loop, N=1 for the next innermost, etc)
         * 
         * Examples:
         *      Source:
         *          loop
         *              [P]
         *              loop
         *                  [P']
         *                  loop
         *                      [P'']
         *                      break
         *                  [Q']
         *                  break
         *              [Q]
         * 
         *      fP(1) should execute everything from [P] to [Q] and then return 1 (since [Q] is at the bottom of the 0th innermost loop containing [P])
         *          fP'(2) should execute everything from [P'] to [Q] and then return 1 (since [Q] is at the bottom of the 1st innermost loop containing [P'])
         *              fP''(3) should execute everything from [P''] to [Q] and then return 1 (since [Q] is at the bottom of the 2nd innermost loop containing [P''])
         *                  fQ'(2) should execute everything from [Q'] to [Q] and then return 1 (since [Q] is at the bottom of the 1st innermost loop containing [Q'])
         *                      fQ(1) should return 1 (since [Q] is at the bottom of the 0th innermost loop containing [Q])
         */
    };

    class LiteralBreak : Function
    {
        public LiteralBreak() { name = "break!"; }
        public override string call(params string[] parameters)
        {
            wasCalled = true;
            if (parameters.Length != 0) throw new Exception("LiteralBreak called with parameters!");
            return "break";
        }
    };

    class LiteralContinue : Function
    {
        public LiteralContinue() { name = "continue!"; }
        public override string call(params string[] parameters)
        {
            wasCalled = true;
            if (parameters.Length != 0) throw new Exception("LiteralContinue called with parameters!");
            return "continue";
        }
    };

    class StateVar : VarDeclaration
    {
        public int SourceLine;
    };

    class CallbackVar : StateVar
    {
        public int CallbackGroup;
    }

    class DescrCompiler
    {
        Descr descr;
        string memberIndentStr;

        public DescrCompiler(Descr descr, int braceDepth)
        {
            this.descr = descr;
            this.memberIndentStr = new string('\t', braceDepth);
        }
        public void Write(TextWriter writer, out int lines)
        {
            lines = 0;

            writer.WriteLine(memberIndentStr + "template<> struct Descriptor<struct {0}> {{", descr.name);
            writer.WriteLine(memberIndentStr + "\tstatic StringRef typeName() {{ return \"{0}\"_sr; }}", descr.name);
            writer.WriteLine(memberIndentStr + "\ttypedef {0} type;", descr.name);
            lines += 3;

            foreach (var dec in descr.body)
            {
                writer.WriteLine(memberIndentStr + "\tstruct {0}Descriptor {{", dec.name);
                writer.WriteLine(memberIndentStr + "\t\tstatic StringRef name() {{ return \"{0}\"_sr; }}", dec.name);
                writer.WriteLine(memberIndentStr + "\t\tstatic StringRef typeName() {{ return \"{0}\"_sr; }}", dec.type);
                writer.WriteLine(memberIndentStr + "\t\tstatic StringRef comment() {{ return \"{0}\"_sr; }}", dec.comment);
                writer.WriteLine(memberIndentStr + "\t\ttypedef {0} type;", dec.type);
                writer.WriteLine(memberIndentStr + "\t\tstatic inline type get({0}& from);", descr.name);
                writer.WriteLine(memberIndentStr + "\t};");
                lines += 7;
            }

            writer.Write(memberIndentStr + "\ttypedef std::tuple<");
            bool FirstDesc = true;
            foreach (var dec in descr.body)
            {
                if (!FirstDesc)
                    writer.Write(",");
                writer.Write("{0}Descriptor", dec.name);
                FirstDesc = false;
            }
            writer.Write("> fields;\n");
            writer.WriteLine(memberIndentStr + "\ttypedef make_index_sequence_impl<0, index_sequence<>, std::tuple_size<fields>::value>::type field_indexes;");
            writer.WriteLine(memberIndentStr + "};");
            if(descr.superClassList != null)
                writer.WriteLine(memberIndentStr + "struct {0} : {1} {{", descr.name, descr.superClassList);
            else
                writer.WriteLine(memberIndentStr + "struct {0} {{", descr.name);
            lines += 4;

            foreach (var dec in descr.body)
            {
                writer.WriteLine(memberIndentStr + "\t{0} {1}; //{2}", dec.type, dec.name, dec.comment);
                lines++;
            }

            writer.WriteLine(memberIndentStr + "};");
            lines++;

            foreach (var dec in descr.body)
            {
                writer.WriteLine(memberIndentStr + "{0} Descriptor<{1}>::{2}Descriptor::get({1}& from) {{ return from.{2}; }}", dec.type, descr.name, dec.name);
                lines++;
            }

        }
    }

    class ActorCompiler
    {
        Actor actor;
        string className, fullClassName, stateClassName;
        string sourceFile;
        List<StateVar> state;
        List<CallbackVar> callbacks = new List<CallbackVar>();
        bool isTopLevel;
        const string loopDepth0 = "int loopDepth=0";
        const string loopDepth = "int loopDepth";
        const int codeIndent = +2;
        const string memberIndentStr = "\t";
        static HashSet<string> usedClassNames = new HashSet<string>();
        bool LineNumbersEnabled;
        int chooseGroups = 0, whenCount = 0;
        string This;
        bool generateProbes;
        public Dictionary<(ulong, ulong), string> uidObjects { get; private set; }

        public ActorCompiler(Actor actor, string sourceFile, bool isTopLevel, bool lineNumbersEnabled, bool generateProbes)
        {
            this.actor = actor;
            this.sourceFile = sourceFile;
            this.isTopLevel = isTopLevel;
            this.LineNumbersEnabled = lineNumbersEnabled;
            this.generateProbes = generateProbes;
            this.uidObjects = new Dictionary<(ulong, ulong), string>();

            FindState();
        }

        private ulong ByteToLong(byte[] bytes) {
            // NOTE: Always assume big endian.
            ulong result = 0;
            foreach(var b in bytes) {
                result += b;
                result <<= 8;
            }
            return result;
        }

        // Generates the identifier for the ACTOR
        private Tuple<ulong, ulong> GetUidFromString(string str) {
            byte[] sha256Hash = SHA256.Create().ComputeHash(Encoding.UTF8.GetBytes(str));
            byte[] first = sha256Hash.Take(8).ToArray();
            byte[] second = sha256Hash.Skip(8).Take(8).ToArray();
            return new Tuple<ulong, ulong>(ByteToLong(first), ByteToLong(second));
        }

        // Writes the function that returns the Actor object
        private void WriteActorFunction(TextWriter writer, string fullReturnType) {
            WriteTemplate(writer);
            LineNumber(writer, actor.SourceLine);
            foreach (string attribute in actor.attributes) {
                writer.Write(attribute + " ");
            }
            if (actor.isStatic) writer.Write("static ");
            writer.WriteLine("{0} {3}{1}( {2} ) {{", fullReturnType, actor.name, string.Join(", ", ParameterList()), actor.nameSpace==null ? "" : actor.nameSpace + "::");
            LineNumber(writer, actor.SourceLine);

            string newActor = string.Format("new {0}({1})",
                    fullClassName,
                    string.Join(", ", actor.parameters.Select(p => p.name).ToArray()));

            if (actor.returnType != null)
                writer.WriteLine("\treturn Future<{1}>({0});", newActor, actor.returnType);
            else
                writer.WriteLine("\t{0};", newActor);
            writer.WriteLine("}");
        }

        // Writes the class of the Actor object
        private void WriteActorClass(TextWriter writer, string fullStateClassName, Function body) {
            // The final actor class mixes in the State class, the Actor base class and all callback classes
            writer.WriteLine("// This generated class is to be used only via {0}()", actor.name);
            WriteTemplate(writer);
            LineNumber(writer, actor.SourceLine);

            string callback_base_classes = string.Join(", ", callbacks.Select(c=>string.Format("public {0}", c.type)));
            if (callback_base_classes != "") callback_base_classes += ", ";
            writer.WriteLine("class {0} final : public Actor<{2}>, {3}public FastAllocated<{1}>, public {4} {{",
                className,
                fullClassName,
                actor.returnType == null ? "void" : actor.returnType,
                callback_base_classes,
                fullStateClassName
                );
            writer.WriteLine("public:");
            writer.WriteLine("\tusing FastAllocated<{0}>::operator new;", fullClassName);
            writer.WriteLine("\tusing FastAllocated<{0}>::operator delete;", fullClassName);

            var actorIdentifierKey = this.sourceFile + ":" + this.actor.name;
            var actorIdentifier = GetUidFromString(actorIdentifierKey);
            uidObjects.Add((actorIdentifier.Item1, actorIdentifier.Item2), actorIdentifierKey);
            // NOTE UL is required as a u64 postfix for large integers, otherwise Clang would complain
            writer.WriteLine("\tstatic constexpr ActorIdentifier __actorIdentifier = UID({0}UL, {1}UL);", actorIdentifier.Item1, actorIdentifier.Item2);
            writer.WriteLine("\tActiveActorHelper activeActorHelper;");

            writer.WriteLine("#pragma clang diagnostic push");
            writer.WriteLine("#pragma clang diagnostic ignored \"-Wdelete-non-virtual-dtor\"");
            if (actor.returnType != null)
                writer.WriteLine(@"    void destroy() override {{
        activeActorHelper.~ActiveActorHelper();
        static_cast<Actor<{0}>*>(this)->~Actor();
        operator delete(this);
    }}", actor.returnType);
            else
                writer.WriteLine(@"    void destroy() {{
        activeActorHelper.~ActiveActorHelper();
        static_cast<Actor<void>*>(this)->~Actor();
        operator delete(this);
    }}");
            writer.WriteLine("#pragma clang diagnostic pop");

            foreach (var cb in callbacks)
                writer.WriteLine("friend struct {0};", cb.type);

            LineNumber(writer, actor.SourceLine);
            WriteConstructor(body, writer, fullStateClassName);
            WriteCancelFunc(writer);
            writer.WriteLine("};");

        }

        public void Write(TextWriter writer)
        {
            string fullReturnType =
                actor.returnType != null ? string.Format("Future<{0}>", actor.returnType)
                : "void";
            for (int i = 0; ; i++)
            {
                className = string.Format("{3}{0}{1}Actor{2}",
                    actor.name.Substring(0, 1).ToUpper(),
                    actor.name.Substring(1),
                    i != 0 ? i.ToString() : "",
                      actor.enclosingClass != null && actor.isForwardDeclaration ? actor.enclosingClass.Replace("::", "_") + "_"
                    : actor.nameSpace != null                                    ? actor.nameSpace.Replace("::", "_") + "_"
                    : "");
                if (actor.isForwardDeclaration || usedClassNames.Add(className))
                    break;
            }

            // e.g. SimpleTimerActor
            fullClassName = className + GetTemplateActuals();
            var actorClassFormal = new VarDeclaration { name = className, type = "class" };
            This = string.Format("static_cast<{0}*>(this)", actorClassFormal.name);
            // e.g. SimpleTimerActorState
            stateClassName = className + "State";
            // e.g. SimpleTimerActorState<SimpleTimerActor>
            var fullStateClassName = stateClassName + GetTemplateActuals(new VarDeclaration { type = "class", name = fullClassName });

            if (actor.isForwardDeclaration) {
                foreach (string attribute in actor.attributes) {
                    writer.Write(attribute + " ");
                }
                if (actor.isStatic) writer.Write("static ");
                writer.WriteLine("{0} {3}{1}( {2} );", fullReturnType, actor.name, string.Join(", ", ParameterList()), actor.nameSpace==null ? "" : actor.nameSpace + "::");
                if (actor.enclosingClass != null) {
                    writer.WriteLine("template <class> friend class {0};", stateClassName);
                }
                return;
            }

            var body = getFunction("", "body", loopDepth0);
            var bodyContext = new Context { 
                target = body,
                catchFErr = getFunction(body.name, "Catch", "Error error", loopDepth0),
            };

            var endContext = TryCatchCompile(actor.body, bodyContext);

            if (endContext.target != null)
            {
                if (actor.returnType == null)
                    CompileStatement(new ReturnStatement { FirstSourceLine = actor.SourceLine, expression = "" }, endContext );
                else
                    throw new Error(actor.SourceLine, "Actor {0} fails to return a value", actor.name);
            }

            if (actor.returnType != null)
            {
                bodyContext.catchFErr.WriteLine("this->~{0}();", stateClassName);
                bodyContext.catchFErr.WriteLine("{0}->sendErrorAndDelPromiseRef(error);", This);
            }
            else
            {
                bodyContext.catchFErr.WriteLine("delete {0};", This);
            }
            bodyContext.catchFErr.WriteLine("loopDepth = 0;");

            if (isTopLevel && actor.nameSpace == null) writer.WriteLine("namespace {");

            // The "State" class contains all state and user code, to make sure that state names are accessible to user code but
            // inherited members of Actor, Callback etc are not.
            writer.WriteLine("// This generated class is to be used only via {0}()", actor.name);
            WriteTemplate(writer, actorClassFormal);
            LineNumber(writer, actor.SourceLine);
            writer.WriteLine("class {0} {{", stateClassName);
            writer.WriteLine("public:");
            LineNumber(writer, actor.SourceLine);
            WriteStateConstructor(writer);
            WriteStateDestructor(writer);
            WriteFunctions(writer);
            foreach (var st in state)
            {
                LineNumber(writer, st.SourceLine);
                writer.WriteLine("\t{0} {1};", st.type, st.name);
            }
            writer.WriteLine("};");

            WriteActorClass(writer, fullStateClassName, body);

            if (isTopLevel && actor.nameSpace == null) writer.WriteLine("} // namespace"); // namespace

            WriteActorFunction(writer, fullReturnType);

            if (actor.testCaseParameters != null)
            {
                writer.WriteLine("ACTOR_TEST_CASE({0}, {1})", actor.name, actor.testCaseParameters);
            }

            Console.WriteLine("\tCompiled ACTOR {0} (line {1})", actor.name, actor.SourceLine);
        }

        const string thisAddress = "reinterpret_cast<unsigned long>(this)";

        void ProbeEnter(Function fun, string name, int index = -1) {
            if (generateProbes) {
                fun.WriteLine("fdb_probe_actor_enter(\"{0}\", {1}, {2});", name, thisAddress, index);
            }
            var blockIdentifier = GetUidFromString(fun.name);
            fun.WriteLine("#ifdef WITH_ACAC");
            fun.WriteLine("static constexpr ActorBlockIdentifier __identifier = UID({0}UL, {1}UL);", blockIdentifier.Item1, blockIdentifier.Item2);
            fun.WriteLine("ActorExecutionContextHelper __helper(static_cast<{0}*>(this)->activeActorHelper.actorID, __identifier);", className);
            fun.WriteLine("#endif // WITH_ACAC");
        }

        void ProbeExit(Function fun, string name, int index = -1) {
            if (generateProbes) {
                fun.WriteLine("fdb_probe_actor_exit(\"{0}\", {1}, {2});", name, thisAddress, index);
            }
        }

        void ProbeCreate(Function fun, string name) {
            if (generateProbes) {
                fun.WriteLine("fdb_probe_actor_create(\"{0}\", {1});", name, thisAddress);
            }
        }

        void ProbeDestroy(Function fun, string name) {
            if (generateProbes) {
                fun.WriteLine("fdb_probe_actor_destroy(\"{0}\", {1});", name, thisAddress);
            }
        }

        void LineNumber(TextWriter writer, int SourceLine)
        {
            if(SourceLine == 0)
            {
                throw new Exception("Internal error: Invalid source line (0)");
            }
            if (LineNumbersEnabled)
                writer.WriteLine("\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} \"{1}\"", SourceLine, sourceFile);
        }
        void LineNumber(Function writer, int SourceLine)
        {
            if(SourceLine == 0)
            {
                throw new Exception("Internal error: Invalid source line (0)");
            }
            if (LineNumbersEnabled)
                writer.WriteLineUnindented( string.Format("\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} \"{1}\"", SourceLine, sourceFile) );
        }

        void TryCatch(Context cx, Function catchFErr, int catchLoopDepth, Action action, bool useLoopDepth = true)
        {
            if (catchFErr!=null)
            {
                cx.target.WriteLine("try {");
                cx.target.Indent(+1);
            }
            action();
            if (catchFErr!=null)
            {
                cx.target.Indent(-1);
                cx.target.WriteLine("}");

                cx.target.WriteLine("catch (Error& error) {");
                if (useLoopDepth)
                    cx.target.WriteLine("\tloopDepth = {0};", catchFErr.call("error", AdjustLoopDepth(catchLoopDepth)));
                else
                    cx.target.WriteLine("\t{0};", catchFErr.call("error", "0"));
                cx.target.WriteLine("} catch (...) {");
                if (useLoopDepth)
                    cx.target.WriteLine("\tloopDepth = {0};", catchFErr.call("unknown_error()", AdjustLoopDepth(catchLoopDepth)));
                else
                    cx.target.WriteLine("\t{0};", catchFErr.call("unknown_error()", "0"));
                cx.target.WriteLine("}");
            }
        }
        Context TryCatchCompile(CodeBlock block, Context cx)
        {
            TryCatch(cx, cx.catchFErr, cx.tryLoopDepth, () => { 
                cx = Compile(block, cx, true);
                if (cx.target != null)
                {
                    var next = getFunction(cx.target.name, "cont", loopDepth);
                    cx.target.WriteLine("loopDepth = {0};", next.call("loopDepth"));
                    cx.target = next;
                    cx.next = null;
                }
            });
            return cx;
        }

        void WriteTemplate(TextWriter writer, params VarDeclaration[] extraParameters)
        {
            var formals = (actor.templateFormals!=null ? actor.templateFormals.AsEnumerable() : Enumerable.Empty<VarDeclaration>())
                .Concat(extraParameters)
                .ToArray();

            if (formals.Length==0) return;
            LineNumber(writer, actor.SourceLine);
            writer.WriteLine("template <{0}>",
                string.Join(", ", formals.Select(
                    p => string.Format("{0} {1}", p.type, p.name)
                        ).ToArray()));
        }
        string GetTemplateActuals(params VarDeclaration[] extraParameters)
        {
            var formals = (actor.templateFormals != null ? actor.templateFormals.AsEnumerable() : Enumerable.Empty<VarDeclaration>())
                .Concat(extraParameters)
                .ToArray();

            if (formals.Length == 0) return "";
            else return "<" +
                string.Join(", ", formals.Select(
                    p => p.name
                        ).ToArray())
                        + ">";
        }

        bool WillContinue(Statement stmt)
        {
            return Flatten(stmt).Any(
                st => (st is ChooseStatement || st is WaitStatement || st is TryStatement));
        }

        CodeBlock AsCodeBlock(Statement statement)
        {
            // SOMEDAY: Is this necessary?  Maybe we should just be compiling statements?
            var cb = statement as CodeBlock;
            if (cb != null) return cb;
            return new CodeBlock { statements = new Statement[] { statement } };
        }

        void CompileStatement(PlainOldCodeStatement stmt, Context cx)
        {
            LineNumber(cx.target,stmt.FirstSourceLine);
            cx.target.WriteLine(stmt.code);
        }
        void CompileStatement(StateDeclarationStatement stmt, Context cx)
        {
            // if this state declaration is at the very top of the actor body
            if (actor.body.statements.Select(x => x as StateDeclarationStatement).TakeWhile(x => x != null).Any(x => x == stmt))
            {
                // Initialize the state in the constructor, not here
                state.Add(new StateVar { SourceLine = stmt.FirstSourceLine, name = stmt.decl.name, type = stmt.decl.type, 
                    initializer = stmt.decl.initializer, initializerConstructorSyntax = stmt.decl.initializerConstructorSyntax } );
            }
            else
            {
                // State variables declared elsewhere must have a default constructor
                state.Add(new StateVar { SourceLine = stmt.FirstSourceLine, name = stmt.decl.name, type = stmt.decl.type, initializer = null });
                if (stmt.decl.initializer != null)
                {
                    LineNumber(cx.target, stmt.FirstSourceLine);
                    if (stmt.decl.initializerConstructorSyntax || stmt.decl.initializer=="")
                        cx.target.WriteLine("{0} = {1}({2});", stmt.decl.name, stmt.decl.type, stmt.decl.initializer);
                    else
                        cx.target.WriteLine("{0} = {1};", stmt.decl.name, stmt.decl.initializer);
                }
            }
        }

        void CompileStatement(ForStatement stmt, Context cx)
        {
            // for( initExpression; condExpression; nextExpression ) body;

            bool noCondition = stmt.condExpression == "" || stmt.condExpression == "true" || stmt.condExpression == "1";

            if (!WillContinue(stmt.body))
            {
                // We can write this loop without anything fancy, because there are no wait statements in it
                if (EmitNativeLoop(stmt.FirstSourceLine, "for(" + stmt.initExpression + ";" + stmt.condExpression + ";" + stmt.nextExpression+")", stmt.body, cx)
                    && noCondition )
                    cx.unreachable();
            }
            else
            {
                // First compile the initExpression
                CompileStatement( new PlainOldCodeStatement {code = stmt.initExpression + ";", FirstSourceLine = stmt.FirstSourceLine}, cx );
                
                // fullBody = { if (!(condExpression)) break; body; }
                Statement fullBody = noCondition ? stmt.body : 
                        new CodeBlock
                        {
                            statements = new Statement[] { 
                                new IfStatement
                                {
                                    expression = "!(" + stmt.condExpression + ")",
                                    ifBody = new BreakStatement { FirstSourceLine = stmt.FirstSourceLine },
                                    FirstSourceLine = stmt.FirstSourceLine
                                }
                            }.Concat(AsCodeBlock(stmt.body).statements).ToArray(),
                            FirstSourceLine = stmt.FirstSourceLine
                        };

                Function loopF = getFunction(cx.target.name, "loopHead", loopDepth);
                Function loopBody = getFunction(cx.target.name, "loopBody", loopDepth);
                Function breakF = getFunction(cx.target.name, "break", loopDepth);
                Function continueF = stmt.nextExpression == "" ? loopF : getFunction(cx.target.name, "continue", loopDepth);

                // TODO: Could we use EmitNativeLoop() here?

                loopF.WriteLine("int oldLoopDepth = ++loopDepth;");
                loopF.WriteLine("while (loopDepth == oldLoopDepth) loopDepth = {0};", loopBody.call("loopDepth"));

                Function endLoop = Compile(AsCodeBlock(fullBody), cx.LoopContext( loopBody, breakF, continueF, +1 ), true).target;
                if (endLoop != null && endLoop != loopBody) {
                    if (stmt.nextExpression != "")
                        CompileStatement( new PlainOldCodeStatement {code = stmt.nextExpression + ";", FirstSourceLine = stmt.FirstSourceLine}, cx.WithTarget(endLoop) );
                    endLoop.WriteLine("if (loopDepth == 0) return {0};", loopF.call("0"));
                }

                cx.target.WriteLine("loopDepth = {0};", loopF.call("loopDepth"));

                if (continueF != loopF && continueF.wasCalled) {
                    CompileStatement( new PlainOldCodeStatement {code = stmt.nextExpression + ";", FirstSourceLine = stmt.FirstSourceLine}, cx.WithTarget(continueF) );
                    continueF.WriteLine("if (loopDepth == 0) return {0};", loopF.call("0"));
                }

                if (breakF.wasCalled)
                    TryCatch(cx.WithTarget(breakF), cx.catchFErr, cx.tryLoopDepth, 
                        () => { breakF.WriteLine("return {0};", cx.next.call("loopDepth")); });
                else
                    cx.unreachable();
            }
        }

        Dictionary<string, int> iterators = new Dictionary<string, int>();

        string getIteratorName(Context cx)
        {
            string name = "RangeFor" + cx.target.name + "Iterator";
            if (!iterators.ContainsKey(name))
                iterators[name] = 0;
            return string.Format("{0}{1}", name, iterators[name]++);
        }

        void CompileStatement(RangeForStatement stmt, Context cx)
        {
            // If stmt does not contain a wait statement, rewrite the original c++11 range-based for loop
            // If there is a wait, we need to rewrite the loop as:
            //    for(a:b) c; ==> for(__iter=std::begin(b); __iter!=std::end(b); ++__iter) { a = *__iter; c; }
            // where __iter is stored as a state variable

            if (WillContinue(stmt.body))
            {
                StateVar container = state.FirstOrDefault(s => s.name == stmt.rangeExpression);
                if (container == null)
                {
                    throw new Error(stmt.FirstSourceLine, "container of range-based for with continuation must be a state variable");
                }

                var iter = getIteratorName(cx);
                state.Add(new StateVar { SourceLine = stmt.FirstSourceLine, name = iter, type = "decltype(std::begin(std::declval<" + container.type + ">()))", initializer = null });
                var equivalent = new ForStatement {
                    initExpression = iter + " = std::begin(" + stmt.rangeExpression + ")",
                    condExpression = iter + " != std::end(" + stmt.rangeExpression + ")",
                    nextExpression = "++" + iter,
                    FirstSourceLine = stmt.FirstSourceLine,
                    body = new CodeBlock
                    {
                        statements = new Statement[] {
                            new PlainOldCodeStatement { FirstSourceLine = stmt.FirstSourceLine, code = stmt.rangeDecl + " = *" + iter + ";" },
                            stmt.body
                        }
                    }
                };
                CompileStatement(equivalent, cx);
            }
            else
            {
                EmitNativeLoop(stmt.FirstSourceLine, "for( " + stmt.rangeDecl + " : " + stmt.rangeExpression + " )", stmt.body, cx);
            }
        }

        void CompileStatement(WhileStatement stmt, Context cx)
        {
            // Compile while (x) { y } as for(;x;) { y }
            var equivalent = new ForStatement
            {
                condExpression = stmt.expression,
                body = stmt.body,
                FirstSourceLine = stmt.FirstSourceLine,
            };

            CompileStatement(equivalent, cx);
        }

        void CompileStatement(LoopStatement stmt, Context cx)
        {
            // Compile loop { body } as for(;;;) { body }
            var equivalent = new ForStatement
            {
                body = stmt.body,
                FirstSourceLine = stmt.FirstSourceLine
            };
            CompileStatement(equivalent, cx);
        }

        // Writes out a loop in native C++ (with no continuation passing)
        // Returns true if the loop is known to have no normal exit (is unreachable)
        private bool EmitNativeLoop(int sourceLine, string head, Statement body, Context cx)
        {
            LineNumber(cx.target, sourceLine);
            cx.target.WriteLine(head + " {");
            cx.target.Indent(+1);
            var literalBreak = new LiteralBreak();           
            Compile(AsCodeBlock(body), cx.LoopContext(cx.target, literalBreak, new LiteralContinue(), 0), true);
            cx.target.Indent(-1);
            cx.target.WriteLine("}");
            return !literalBreak.wasCalled;
        }

        void CompileStatement(ChooseStatement stmt, Context cx)
        {
            int group = ++this.chooseGroups;
            //string cbGroup = "ChooseGroup" + getFunction(cx.target.name,"W").name;  // SOMEDAY

            var codeblock = stmt.body as CodeBlock;
            if (codeblock == null)
                throw new Error(stmt.FirstSourceLine, "'choose' must be followed by a compound statement.");
            var choices = codeblock.statements
                .OfType<WhenStatement>()
                .Select( (ch,i) => new { 
                    Stmt = ch, 
                    Group = group, 
                    Index = this.whenCount+i,
                    Body = getFunction(cx.target.name, "when", 
                                           new string[] { string.Format("{0} const& {2}{1}", ch.wait.result.type, ch.wait.result.name, ch.wait.resultIsState?"__":""), loopDepth },
                                           new string[] { string.Format("{0} && {2}{1}", ch.wait.result.type, ch.wait.result.name, ch.wait.resultIsState?"__":""), loopDepth }
                                       ),
                    Future = string.Format("__when_expr_{0}", this.whenCount + i),
                    CallbackType = string.Format("{3}< {0}, {1}, {2} >", fullClassName, this.whenCount + i, ch.wait.result.type, ch.wait.isWaitNext ? "ActorSingleCallback" : "ActorCallback"),
                    CallbackTypeInStateClass = string.Format("{3}< {0}, {1}, {2} >", className, this.whenCount + i, ch.wait.result.type, ch.wait.isWaitNext ? "ActorSingleCallback" : "ActorCallback")
                })
                .ToArray();
            this.whenCount += choices.Length;
            if (choices.Length != codeblock.statements.Length)
                throw new Error(codeblock.statements.First(x=>!(x is WhenStatement)).FirstSourceLine, "only 'when' statements are valid in an 'choose' block.");

            var exitFunc = getFunction("exitChoose", "");
            exitFunc.returnType = "void";
            exitFunc.WriteLine("if ({0}->actor_wait_state > 0) {0}->actor_wait_state = 0;", This);
            foreach(var ch in choices)
                exitFunc.WriteLine("{0}->{1}::remove();", This, ch.CallbackTypeInStateClass);
            exitFunc.endIsUnreachable = true;

            //state.Add(new StateVar { SourceLine = stmt.FirstSourceLine, type = "CallbackGroup", name = cbGroup, callbackCatchFErr = cx.catchFErr });
            bool reachable = false;
            foreach(var ch in choices) {
                callbacks.Add(new CallbackVar
                {
                    SourceLine = ch.Stmt.FirstSourceLine,
                    CallbackGroup = ch.Group, 
                    type = ch.CallbackType
                });
                var r = ch.Body;
                if (ch.Stmt.wait.resultIsState)
                {
                    Function overload = r.popOverload();
                    CompileStatement(new StateDeclarationStatement
                    {
                        FirstSourceLine = ch.Stmt.FirstSourceLine,
                        decl = new VarDeclaration { 
                            type = ch.Stmt.wait.result.type, 
                            name = ch.Stmt.wait.result.name, 
                            initializer = "__" + ch.Stmt.wait.result.name,
                            initializerConstructorSyntax = false 
                        }
                    }, cx.WithTarget(r));
                    if (overload != null)
                    {
                        overload.WriteLine("{0} = std::move(__{0});", ch.Stmt.wait.result.name);
                        r.setOverload(overload);
                    }
                }
                if (ch.Stmt.body != null)
                {
                    r = Compile(AsCodeBlock(ch.Stmt.body), cx.WithTarget(r), true).target;
                }
                if (r != null)
                {
                    reachable = true;
                    if (cx.next.formalParameters.Length == 1)
                        r.WriteLine("loopDepth = {0};", cx.next.call("loopDepth"));
                    else {
                        Function overload = r.popOverload();
                        r.WriteLine("loopDepth = {0};", cx.next.call(ch.Stmt.wait.result.name, "loopDepth"));
                        if (overload != null) {
                            overload.WriteLine("loopDepth = {0};", cx.next.call(string.Format("std::move({0})", ch.Stmt.wait.result.name), "loopDepth"));
                            r.setOverload(overload);
                        }
                    }
                }

                var cbFunc = new Function { 
                    name = "callback_fire",
                    returnType = "void",
                    formalParameters = new string[] { 
                        ch.CallbackTypeInStateClass + "*",
                        ch.Stmt.wait.result.type + " const& value"
                    },
                    endIsUnreachable = true
                };
                cbFunc.addOverload(ch.CallbackTypeInStateClass + "*", ch.Stmt.wait.result.type + " && value");
                functions.Add(string.Format("{0}#{1}", cbFunc.name, ch.Index), cbFunc);
                cbFunc.Indent(codeIndent);
                ProbeEnter(cbFunc, actor.name, ch.Index);
                cbFunc.WriteLine("{0};", exitFunc.call());

                Function _overload = cbFunc.popOverload();
                TryCatch(cx.WithTarget(cbFunc), cx.catchFErr, cx.tryLoopDepth, () => {
                    cbFunc.WriteLine("{0};", ch.Body.call("value", "0"));
                }, false);
                if (_overload != null) {
                    TryCatch(cx.WithTarget(_overload), cx.catchFErr, cx.tryLoopDepth, () => {
                        _overload.WriteLine("{0};", ch.Body.call("std::move(value)", "0"));
                    }, false);
                    cbFunc.setOverload(_overload);
                }
                ProbeExit(cbFunc, actor.name, ch.Index);

                var errFunc = new Function
                {
                    name = "callback_error",
                    returnType = "void",
                    formalParameters = new string[] { 
                        ch.CallbackTypeInStateClass + "*",
                        "Error err"
                    },
                    endIsUnreachable = true
                };
                functions.Add(string.Format("{0}#{1}", errFunc.name, ch.Index), errFunc);
                errFunc.Indent(codeIndent);
                ProbeEnter(errFunc, actor.name, ch.Index);
                errFunc.WriteLine("{0};", exitFunc.call());
                TryCatch(cx.WithTarget(errFunc), cx.catchFErr, cx.tryLoopDepth, () =>
                {
                    errFunc.WriteLine("{0};", cx.catchFErr.call("err", "0"));
                }, false);
                ProbeExit(errFunc, actor.name, ch.Index);
            }

            bool firstChoice = true;
            foreach (var ch in choices)
            {
                string getFunc = ch.Stmt.wait.isWaitNext ? "pop" : "get";
                LineNumber(cx.target, ch.Stmt.wait.FirstSourceLine);
                cx.target.WriteLine("{2}<{3}> {0} = {1};", ch.Future, ch.Stmt.wait.futureExpression, ch.Stmt.wait.isWaitNext ? "FutureStream" : "StrictFuture", ch.Stmt.wait.result.type);

                if (firstChoice)
                {
                    // Do this check only after evaluating the expression for the first wait expression, so that expression cannot be short circuited by cancellation.
                    // So wait( expr() ) will always evaluate `expr()`, but choose { when ( wait(success( expr2() )) {} } need
                    // not evaluate `expr2()`.
                    firstChoice = false;
                    LineNumber(cx.target, stmt.FirstSourceLine);
                    if (actor.IsCancellable())
                        cx.target.WriteLine("if ({1}->actor_wait_state < 0) return {0};", cx.catchFErr.call("actor_cancelled()", AdjustLoopDepth(cx.tryLoopDepth)), This);
                }

                cx.target.WriteLine("if ({0}.isReady()) {{ if ({0}.isError()) return {2}; else return {1}; }};", ch.Future, ch.Body.call(ch.Future + "." + getFunc + "()", "loopDepth"), cx.catchFErr.call(ch.Future + ".getError()", AdjustLoopDepth(cx.tryLoopDepth)));
            }
            cx.target.WriteLine("{1}->actor_wait_state = {0};", group, This);
            foreach (var ch in choices)
            {
                LineNumber(cx.target, ch.Stmt.wait.FirstSourceLine);
                cx.target.WriteLine("{0}.addCallbackAndClear(static_cast<{1}*>({2}));", ch.Future, ch.CallbackTypeInStateClass, This);
            }
            cx.target.WriteLine("loopDepth = 0;");//cx.target.WriteLine("return 0;");

            if (!reachable) cx.unreachable();
        }
        void CompileStatement(BreakStatement stmt, Context cx)
        {
            if (cx.breakF == null)
                throw new Error(stmt.FirstSourceLine, "break outside loop");
            if (cx.breakF is LiteralBreak)
                cx.target.WriteLine("{0};", cx.breakF.call());
            else
            {
                cx.target.WriteLine("return {0}; // break", cx.breakF.call("loopDepth==0?0:loopDepth-1"));
            }
            cx.unreachable();
        }
        void CompileStatement(ContinueStatement stmt, Context cx)
        {
            if (cx.continueF == null)
                throw new Error(stmt.FirstSourceLine, "continue outside loop");
            if (cx.continueF is LiteralContinue)
                cx.target.WriteLine("{0};", cx.continueF.call());
            else
                cx.target.WriteLine("return {0}; // continue", cx.continueF.call("loopDepth"));
            cx.unreachable();
        }
        void CompileStatement(WaitStatement stmt, Context cx)
        {
            var equiv = new ChooseStatement
            {
                body = new CodeBlock
                {
                    statements = new Statement[] {
                        new WhenStatement {
                            wait = stmt,
                            body = null,
                            FirstSourceLine = stmt.FirstSourceLine,
                        }
                    },
                    FirstSourceLine = stmt.FirstSourceLine
                },
                FirstSourceLine = stmt.FirstSourceLine
            };
            if (!stmt.resultIsState) {
                cx.next.formalParameters = new string[] {
                    string.Format("{0} const& {1}", stmt.result.type, stmt.result.name), 
                    loopDepth };
                cx.next.addOverload(
                    string.Format("{0} && {1}", stmt.result.type, stmt.result.name),
                    loopDepth);
            }
            CompileStatement(equiv, cx);
        }
        void CompileStatement(CodeBlock stmt, Context cx)
        {
            cx.target.WriteLine("{");
            cx.target.Indent(+1);
            var end = Compile(stmt, cx, true);
            cx.target.Indent(-1);
            cx.target.WriteLine("}");
            if (end.target == null)
                cx.unreachable();
            else if (end.target != cx.target)
                end.target.WriteLine("loopDepth = {0};", cx.next.call("loopDepth"));
        }
        void CompileStatement(ReturnStatement stmt, Context cx)
        {
            LineNumber(cx.target, stmt.FirstSourceLine);
            if ((stmt.expression == "") != (actor.returnType == null))
                throw new Error(stmt.FirstSourceLine, "Return statement does not match actor declaration");
            if (actor.returnType != null)
            {
                if (stmt.expression == "Never()")
                {
                    // `return Never();` destroys state immediately but never returns to the caller
                    cx.target.WriteLine("this->~{0}();", stateClassName);
                    cx.target.WriteLine("{0}->sendAndDelPromiseRef(Never());", This);
                }
                else
                {
                    // Short circuit if there are no futures outstanding, but still evaluate the expression
                    // if it has side effects
                    cx.target.WriteLine("if (!{0}->SAV<{1}>::futures) {{ (void)({2}); this->~{3}(); {0}->destroy(); return 0; }}", This, actor.returnType, stmt.expression, stateClassName);
                    // Build the return value directly in SAV<T>::value_storage
                    // If the expression is exactly the name of a state variable, std::move() it
                    if (state.Exists(s => s.name == stmt.expression))
                    {
                        cx.target.WriteLine("new (&{0}->SAV< {1} >::value()) {1}(std::move({2})); // state_var_RVO", This, actor.returnType, stmt.expression);
                    }
                    else
                    {
                        cx.target.WriteLine("new (&{0}->SAV< {1} >::value()) {1}({2});", This, actor.returnType, stmt.expression);
                    }
                    // Destruct state
                    cx.target.WriteLine("this->~{0}();", stateClassName);
                    // Tell SAV<T> to return the value we already constructed in value_storage
                    cx.target.WriteLine("{0}->finishSendAndDelPromiseRef();", This);
                }
            } else
                cx.target.WriteLine("delete {0};", This);
            cx.target.WriteLine("return 0;");
            cx.unreachable();
        }
        void CompileStatement(IfStatement stmt, Context cx)
        {
            bool useContinuation = WillContinue(stmt.ifBody) || WillContinue(stmt.elseBody);

            LineNumber(cx.target, stmt.FirstSourceLine);
            cx.target.WriteLine("if {1}({0})", stmt.expression, stmt.constexpr ? "constexpr " : "");
            cx.target.WriteLine("{");
            cx.target.Indent(+1);
            Function ifTarget = Compile(AsCodeBlock(stmt.ifBody), cx, useContinuation).target;
            if (useContinuation && ifTarget != null)
                ifTarget.WriteLine("loopDepth = {0};", cx.next.call("loopDepth"));
            cx.target.Indent(-1);
            cx.target.WriteLine("}");
            Function elseTarget = null;
            if (stmt.elseBody != null || useContinuation)
            {
                cx.target.WriteLine("else");
                cx.target.WriteLine("{");
                cx.target.Indent(+1);
                elseTarget = cx.target;
                if (stmt.elseBody != null)
                {
                    elseTarget = Compile(AsCodeBlock(stmt.elseBody), cx, useContinuation).target;
                }
                if (useContinuation && elseTarget != null)
                    elseTarget.WriteLine("loopDepth = {0};", cx.next.call("loopDepth"));
                cx.target.Indent(-1);
                cx.target.WriteLine("}");
            }
            if (ifTarget == null && stmt.elseBody != null && elseTarget == null)
                cx.unreachable();
            else if (!cx.next.wasCalled && useContinuation) 
                throw new Exception("Internal error: IfStatement: next not called?");
        }
        void CompileStatement(TryStatement stmt, Context cx)
        {
            bool reachable = false;

            if (stmt.catches.Count != 1) throw new Error(stmt.FirstSourceLine, "try statement must have exactly one catch clause");
            var c = stmt.catches[0];
            string catchErrorParameterName = "";
            if (c.expression != "...") {
                string exp = c.expression.Replace(" ","");
                if (!exp.StartsWith("Error&"))
                    throw new Error(c.FirstSourceLine, "Only type 'Error' or '...' may be caught in an actor function");
                catchErrorParameterName = exp.Substring(6);
            }
            if (catchErrorParameterName == "") catchErrorParameterName = "__current_error";

            var catchFErr = getFunction(cx.target.name, "Catch", "const Error& " + catchErrorParameterName, loopDepth0);
            catchFErr.exceptionParameterIs = catchErrorParameterName;
            var end = TryCatchCompile(AsCodeBlock(stmt.tryBody), cx.WithCatch(catchFErr));
            if (end.target != null) reachable = true;

            if (end.target!=null)
                TryCatch(end, cx.catchFErr, cx.tryLoopDepth, () =>
                    end.target.WriteLine("loopDepth = {0};", cx.next.call("loopDepth")));

            // Now to write the catch function
            TryCatch(cx.WithTarget(catchFErr), cx.catchFErr, cx.tryLoopDepth, () =>
                {
                    var cend = Compile(AsCodeBlock(c.body), cx.WithTarget(catchFErr), true);
                    if (cend.target != null) cend.target.WriteLine("loopDepth = {0};", cx.next.call("loopDepth"));
                    if (cend.target != null) reachable = true;
                });

            if (!reachable) cx.unreachable();
        }
        void CompileStatement(ThrowStatement stmt, Context cx)
        {
            LineNumber(cx.target, stmt.FirstSourceLine);
            
            if (stmt.expression == "")
            {
                if (cx.target.exceptionParameterIs != null)
                    cx.target.WriteLine("return {0};", cx.catchFErr.call(cx.target.exceptionParameterIs, AdjustLoopDepth( cx.tryLoopDepth )));
                else
                    throw new Error(stmt.FirstSourceLine, "throw statement with no expression has no current exception in scope");
            }
            else
                cx.target.WriteLine("return {0};", cx.catchFErr.call(stmt.expression, AdjustLoopDepth( cx.tryLoopDepth )));
            cx.unreachable();
        }
        void CompileStatement(Statement stmt, Context cx)
        {
            // Use reflection for double dispatch.  SOMEDAY: Use a Dictionary<string,Action<Statement,Context>> and expression trees to memoize
            var method = typeof(ActorCompiler).GetMethod("CompileStatement", 
                System.Reflection.BindingFlags.NonPublic|System.Reflection.BindingFlags.Instance|System.Reflection.BindingFlags.ExactBinding, 
                null, new Type[] { stmt.GetType(), typeof(Context) }, null);
            if (method == null)
                throw new Error(stmt.FirstSourceLine, "Statement type {0} not supported yet.", stmt.GetType().Name);
            try
            {
                method.Invoke(this, new object[] { stmt, cx });
            }
            catch (System.Reflection.TargetInvocationException e)
            {
                if (!(e.InnerException is Error))
                    Console.Error.WriteLine("\tHit error <{0}> for statement type {1} at line {2}\n\tStack Trace:\n{3}", 
                        e.InnerException.Message, stmt.GetType().Name, stmt.FirstSourceLine, e.InnerException.StackTrace);
                throw e.InnerException;
            }
        }

        // Compile returns a new context based on the one that is passed in, but (unlike CompileStatement)
        //   does not modify its parameter
        // The target of the returned context is null if the end of the CodeBlock is unreachable (otherwise
        //   it is the target Function to which the end of the CodeBlock was written)
        Context Compile(CodeBlock block, Context context, bool okToContinue=true)
        {
            var cx = context.Clone(); cx.next = null;
            foreach (var stmt in block.statements)
            {
                if (cx.target == null)
                {
                    throw new Error(stmt.FirstSourceLine, "Unreachable code.");
                    //Console.Error.WriteLine("\t(WARNING) Unreachable code at line {0}.", stmt.FirstSourceLine);
                    //break;
                }
                if (cx.next == null)
                    cx.next = getFunction(cx.target.name, "cont", loopDepth);
                CompileStatement(stmt, cx);
                if (cx.next.wasCalled)
                {
                    if (cx.target == null) throw new Exception("Unreachable continuation called?");
                    if (!okToContinue) throw new Exception("Unexpected continuation");
                    cx.target = cx.next;
                    cx.next = null;
                }
            }
            return cx;
        }

        Dictionary<string, Function> functions = new Dictionary<string, Function>();

        void WriteFunctions(TextWriter writer)
        {
            foreach (var func in functions.Values)
            {
                string body = func.BodyText;
                if (body.Length != 0)
                {
                    WriteFunction(writer, func, body);
                }
                if (func.overload != null)
                {
                    string overloadBody = func.overload.BodyText;
                    if (overloadBody.Length != 0)
                    {
                        WriteFunction(writer, func.overload, overloadBody);
                    }
                }
            }
        }

        private static void WriteFunction(TextWriter writer, Function func, string body)
        {
            writer.WriteLine(memberIndentStr + "{0}{1}({2}){3}",
                func.returnType == "" ? "" : func.returnType + " ", 
                func.useByName(),
                string.Join(",", func.formalParameters),
                func.specifiers == "" ? "" : " " + func.specifiers);
            if (func.returnType != "")
                writer.WriteLine(memberIndentStr + "{");
            writer.WriteLine(body);
            if (!func.endIsUnreachable)
                writer.WriteLine(memberIndentStr + "\treturn loopDepth;");
            writer.WriteLine(memberIndentStr + "}");
        }

        Function getFunction(string baseName, string addName, string[] formalParameters, string[] overloadFormalParameters)
        {
            string proposedName;
            if (addName == "cont" && baseName.Length>=5 && baseName.Substring(baseName.Length - 5, 4) == "cont")
                proposedName = baseName.Substring(0, baseName.Length - 1);
            else
                proposedName = baseName + addName;

            int i = 0;
            while (functions.ContainsKey(string.Format("{0}{1}", proposedName, ++i))) ;

            var f = new Function { 
                name = string.Format("{0}{1}", proposedName, i),
                returnType = "int",
                formalParameters = formalParameters
            };
            if (overloadFormalParameters != null) {
                f.addOverload(overloadFormalParameters);
            }
            f.Indent(codeIndent);
            functions.Add(f.name, f);
            return f;
        }

        Function getFunction(string baseName, string addName, params string[] formalParameters)
        {
            return getFunction(baseName, addName, formalParameters, null);
        }

        string[] ParameterList()
        {
            return actor.parameters.Select(p =>
            {
                // SOMEDAY: pass small built in types by value
                if (p.initializer != "")
                    return string.Format("{0} const& {1} = {2}", p.type, p.name, p.initializer);
                else
                    return string.Format("{0} const& {1}", p.type, p.name);
            }).ToArray();
        }
        void WriteCancelFunc(TextWriter writer)
        {
            if (actor.IsCancellable())
            {
                Function cancelFunc = new Function
                {
                    name = "cancel",
                    returnType = "void",
                    formalParameters = new string[] {},
                    endIsUnreachable = true,
                    publicName = true,
                    specifiers = "override"
                };
                cancelFunc.Indent(codeIndent);
                cancelFunc.WriteLine("auto wait_state = this->actor_wait_state;");
                cancelFunc.WriteLine("this->actor_wait_state = -1;");
                cancelFunc.WriteLine("switch (wait_state) {");
                int lastGroup = -1;
                foreach (var cb in callbacks.OrderBy(cb => cb.CallbackGroup))
                    if (cb.CallbackGroup != lastGroup)
                    {
                        lastGroup = cb.CallbackGroup;
                        cancelFunc.WriteLine("case {0}: this->a_callback_error(({1}*)0, actor_cancelled()); break;", cb.CallbackGroup, cb.type);
                    }
                cancelFunc.WriteLine("}");
                WriteFunction(writer, cancelFunc, cancelFunc.BodyText);
            }
        }

        void WriteConstructor(Function body, TextWriter writer, string fullStateClassName)
        {
            Function constructor = new Function
            {
                name = className,
                returnType = "",
                formalParameters = ParameterList(),
                endIsUnreachable = true,
                publicName = true
            };

            // Initializes class member variables
            constructor.Indent(codeIndent);
            constructor.WriteLine( " : Actor<" + (actor.returnType == null ? "void" : actor.returnType) + ">()," );
            constructor.WriteLine( "   {0}({1}),", fullStateClassName, string.Join(", ", actor.parameters.Select(p => p.name)));
            constructor.WriteLine( "   activeActorHelper(__actorIdentifier)");
            constructor.Indent(-1);

            constructor.WriteLine("{");
            constructor.Indent(+1);

            ProbeEnter(constructor, actor.name);

            constructor.WriteLine("#ifdef ENABLE_SAMPLING");
            constructor.WriteLine("this->lineage.setActorName(\"{0}\");", actor.name);
            constructor.WriteLine("LineageScope _(&this->lineage);");
            // constructor.WriteLine("getCurrentLineage()->modify(&StackLineage::actorName) = \"{0}\"_sr;", actor.name);
            constructor.WriteLine("#endif");

            constructor.WriteLine("this->{0};", body.call());

            ProbeExit(constructor, actor.name);

            WriteFunction(writer, constructor, constructor.BodyText);
        }

        void WriteStateConstructor(TextWriter writer)
        {
            Function constructor = new Function
            {
                name = stateClassName,
                returnType = "",
                formalParameters = ParameterList(),
                endIsUnreachable = true,
                publicName = true
            };
            constructor.Indent(codeIndent);
            string ini = null;
            int line = actor.SourceLine;
            var initializers = state.AsEnumerable();
            foreach (var s in initializers)
                if (s.initializer != null)
                {
                    LineNumber(constructor, line);
                    if (ini != null)
                    {
                        constructor.WriteLine(ini + ",");
                        ini = "   ";
                    }
                    else
                    {
                        ini = " : ";
                    }

                    ini += string.Format("{0}({1})", s.name, s.initializer);
                    line = s.SourceLine;
                }
            LineNumber(constructor, line);
            if (ini != null)
                constructor.WriteLine(ini);
            constructor.Indent(-1);
            constructor.WriteLine("{");
            constructor.Indent(+1);
            ProbeCreate(constructor, actor.name);
            WriteFunction(writer, constructor, constructor.BodyText);
        }

        void WriteStateDestructor(TextWriter writer) {
            Function destructor = new Function
            {
                name = String.Format("~{0}", stateClassName),
                returnType = "",
                formalParameters = new string[0],
                endIsUnreachable = true,
                publicName = true,
            };
            destructor.Indent(codeIndent);
            destructor.Indent(-1);
            destructor.WriteLine("{");
            destructor.Indent(+1);
            ProbeDestroy(destructor, actor.name);
            WriteFunction(writer, destructor, destructor.BodyText);
        }

        IEnumerable<Statement> Flatten(Statement stmt)
        {
            if (stmt == null) return new Statement[] { };
            var fl = new TypeSwitch<IEnumerable<Statement>>(stmt)
                .Case<LoopStatement>(s => Flatten(s.body))
                .Case<WhileStatement>(s => Flatten(s.body))
                .Case<ForStatement>(s => Flatten(s.body))
                .Case<RangeForStatement>(s => Flatten(s.body))
                .Case<CodeBlock>(s => s.statements.SelectMany(t=>Flatten(t)))
                .Case<IfStatement>( s => Flatten(s.ifBody).Concat(Flatten(s.elseBody)) )
                .Case<ChooseStatement>( s => Flatten(s.body) )
                .Case<WhenStatement>( s => Flatten(s.body) )
                .Case<TryStatement>( s => Flatten(s.tryBody).Concat( s.catches.SelectMany(c=>Flatten(c.body)) ) )
                .Case<Statement>(s => Enumerable.Empty<Statement>())
                .Return();
            return new Statement[]{stmt}.Concat(fl);
        }

        void FindState()
        {
            state = actor.parameters
                .Select( 
                    p=>new StateVar { SourceLine = actor.SourceLine, name=p.name, type=p.type, initializer=p.name, initializerConstructorSyntax=false } )
                .ToList();
        }

        // Generate an expression equivalent to max(0, loopDepth-subtract) for the given constant subtract
        string AdjustLoopDepth(int subtract)
        {
            if (subtract == 0)
                return "loopDepth";
            else
                return string.Format("std::max(0, loopDepth - {0})", subtract);
        }
    }
}

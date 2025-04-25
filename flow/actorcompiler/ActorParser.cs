/*
 * ActorParser.cs
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
using System.Text.RegularExpressions;

namespace actorcompiler
{
    class Error : Exception
    {
        public int SourceLine { get; private set; }

        public Error(int SourceLine, string format, params object[] args)
            : base(string.Format(format, args))
        {
            this.SourceLine = SourceLine;
        }
    };

    class ErrorMessagePolicy
    {
        public bool DisableDiagnostics = false;

        public void HandleActorWithoutWait(String sourceFile, Actor actor)
        {
            if (!DisableDiagnostics && !actor.isTestCase)
            {
                // TODO(atn34): Once cmake is the only build system we can make this an error instead of a warning.
                Console.Error.WriteLine(
                    "{0}:{1}: warning: ACTOR {2} does not contain a wait() statement",
                    sourceFile,
                    actor.SourceLine,
                    actor.name
                );
            }
        }

        public bool ActorsNoDiscardByDefault()
        {
            return !DisableDiagnostics;
        }
    }

    class Token
    {
        public string Value;
        public int Position;
        public int SourceLine;
        public int BraceDepth;
        public int ParenDepth;
        public bool IsWhitespace
        {
            get
            {
                return Value == " "
                    || Value == "\n"
                    || Value == "\r"
                    || Value == "\r\n"
                    || Value == "\t"
                    || Value.StartsWith("//")
                    || Value.StartsWith("/*");
            }
        }

        public override string ToString()
        {
            return Value;
        }

        public Token Assert(string error, Func<Token, bool> pred)
        {
            if (!pred(this))
                throw new Error(SourceLine, error);
            return this;
        }

        public TokenRange GetMatchingRangeIn(TokenRange range)
        {
            Func<Token, bool> pred;
            int dir;
            switch (Value)
            {
                case "(":
                    pred = t => t.Value != ")" || t.ParenDepth != ParenDepth;
                    dir = +1;
                    break;
                case ")":
                    pred = t => t.Value != "(" || t.ParenDepth != ParenDepth;
                    dir = -1;
                    break;
                case "{":
                    pred = t => t.Value != "}" || t.BraceDepth != BraceDepth;
                    dir = +1;
                    break;
                case "}":
                    pred = t => t.Value != "{" || t.BraceDepth != BraceDepth;
                    dir = -1;
                    break;
                case "<":
                    return new TokenRange(
                        range.GetAllTokens(),
                        Position + 1,
                        AngleBracketParser
                            .NotInsideAngleBrackets(
                                new TokenRange(range.GetAllTokens(), Position, range.End)
                            )
                            .Skip(1) // skip the "<", which is considered "outside"
                            .First() // get the ">", which is likewise "outside"
                            .Position
                    );
                case "[":
                    return new TokenRange(
                        range.GetAllTokens(),
                        Position + 1,
                        BracketParser
                            .NotInsideBrackets(
                                new TokenRange(range.GetAllTokens(), Position, range.End)
                            )
                            .Skip(1) // skip the "[", which is considered "outside"
                            .First() // get the "]", which is likewise "outside"
                            .Position
                    );
                default:
                    throw new NotSupportedException("Can't match this token!");
            }
            TokenRange r;
            if (dir == -1)
            {
                r = new TokenRange(range.GetAllTokens(), range.Begin, Position).RevTakeWhile(pred);
                if (r.Begin == range.Begin)
                    throw new Error(SourceLine, "Syntax error: Unmatched " + Value);
            }
            else
            {
                r = new TokenRange(range.GetAllTokens(), Position + 1, range.End).TakeWhile(pred);
                if (r.End == range.End)
                    throw new Error(SourceLine, "Syntax error: Unmatched " + Value);
            }
            return r;
        }
    };

    class TokenRange : IEnumerable<Token>
    {
        public TokenRange(Token[] tokens, int beginPos, int endPos)
        {
            if (beginPos > endPos)
                throw new InvalidOperationException("Invalid TokenRange");
            this.tokens = tokens;
            this.beginPos = beginPos;
            this.endPos = endPos;
        }

        public bool IsEmpty
        {
            get { return beginPos == endPos; }
        }
        public int Begin
        {
            get { return beginPos; }
        }
        public int End
        {
            get { return endPos; }
        }

        public Token First()
        {
            if (beginPos == endPos)
                throw new InvalidOperationException("Empty TokenRange");
            return tokens[beginPos];
        }

        public Token Last()
        {
            if (beginPos == endPos)
                throw new InvalidOperationException("Empty TokenRange");
            return tokens[endPos - 1];
        }

        public Token Last(Func<Token, bool> pred)
        {
            for (int i = endPos - 1; i >= beginPos; i--)
                if (pred(tokens[i]))
                    return tokens[i];
            throw new Exception("Matching token not found");
        }

        public TokenRange Skip(int count)
        {
            return new TokenRange(tokens, beginPos + count, endPos);
        }

        public TokenRange Consume(string value)
        {
            First().Assert("Expected " + value, t => t.Value == value);
            return Skip(1);
        }

        public TokenRange Consume(string error, Func<Token, bool> pred)
        {
            First().Assert(error, pred);
            return Skip(1);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public IEnumerator<Token> GetEnumerator()
        {
            for (int i = beginPos; i < endPos; i++)
                yield return tokens[i];
        }

        public TokenRange SkipWhile(Func<Token, bool> pred)
        {
            for (int e = beginPos; e < endPos; e++)
                if (!pred(tokens[e]))
                    return new TokenRange(tokens, e, endPos);
            return new TokenRange(tokens, endPos, endPos);
        }

        public TokenRange TakeWhile(Func<Token, bool> pred)
        {
            for (int e = beginPos; e < endPos; e++)
                if (!pred(tokens[e]))
                    return new TokenRange(tokens, beginPos, e);
            return new TokenRange(tokens, beginPos, endPos);
        }

        public TokenRange RevTakeWhile(Func<Token, bool> pred)
        {
            for (int e = endPos - 1; e >= beginPos; e--)
                if (!pred(tokens[e]))
                    return new TokenRange(tokens, e + 1, endPos);
            return new TokenRange(tokens, beginPos, endPos);
        }

        public TokenRange RevSkipWhile(Func<Token, bool> pred)
        {
            for (int e = endPos - 1; e >= beginPos; e--)
                if (!pred(tokens[e]))
                    return new TokenRange(tokens, beginPos, e + 1);
            return new TokenRange(tokens, beginPos, beginPos);
        }

        public Token[] GetAllTokens()
        {
            return tokens;
        }

        public int Length
        {
            get { return endPos - beginPos; }
        }

        Token[] tokens;
        int beginPos;
        int endPos;
    };

    static class BracketParser
    {
        public static IEnumerable<Token> NotInsideBrackets(IEnumerable<Token> tokens)
        {
            int BracketDepth = 0;
            int? BasePD = null;
            foreach (var tok in tokens)
            {
                if (BasePD == null)
                    BasePD = tok.ParenDepth;
                if (tok.ParenDepth == BasePD && tok.Value == "]")
                    BracketDepth--;
                if (BracketDepth == 0)
                    yield return tok;
                if (tok.ParenDepth == BasePD && tok.Value == "[")
                    BracketDepth++;
            }
        }
    };

    static class AngleBracketParser
    {
        public static IEnumerable<Token> NotInsideAngleBrackets(IEnumerable<Token> tokens)
        {
            int AngleDepth = 0;
            int? BasePD = null;
            foreach (var tok in tokens)
            {
                if (BasePD == null)
                    BasePD = tok.ParenDepth;
                if (tok.ParenDepth == BasePD && tok.Value == ">")
                    AngleDepth--;
                if (AngleDepth == 0)
                    yield return tok;
                if (tok.ParenDepth == BasePD && tok.Value == "<")
                    AngleDepth++;
            }
        }
    };

    class ActorParser
    {
        public bool LineNumbersEnabled = true;

        Token[] tokens;
        string sourceFile;
        ErrorMessagePolicy errorMessagePolicy;
        public bool generateProbes;
        public List<ActorInformation> uidObjects;

        public ActorParser(
            string text,
            string sourceFile,
            ErrorMessagePolicy errorMessagePolicy,
            bool generateProbes
        )
        {
            this.sourceFile = sourceFile;
            this.errorMessagePolicy = errorMessagePolicy;
            this.generateProbes = generateProbes;
            this.uidObjects = new List<ActorInformation>();
            tokens = Tokenize(text).Select(t => new Token { Value = t }).ToArray();
            CountParens();
            //if (sourceFile.EndsWith(".h")) LineNumbersEnabled = false;
            //Console.WriteLine("{0} chars -> {1} tokens", text.Length, tokens.Length);
            //showTokens();
        }

        class ClassContext
        {
            public string name;
            public int inBlocks;
        }

        private bool ParseClassContext(TokenRange toks, out string name)
        {
            name = "";
            if (toks.Begin == toks.End)
            {
                return false;
            }

            // http://nongnu.org/hcb/#attribute-specifier-seq
            Token first;
            while (true)
            {
                first = toks.First(NonWhitespace);
                if (first.Value == "[")
                {
                    var contents = first.GetMatchingRangeIn(toks);
                    toks = range(contents.End + 1, toks.End);
                }
                else if (first.Value == "alignas")
                {
                    toks = range(first.Position + 1, toks.End);
                    first = toks.First(NonWhitespace);
                    first.Assert("Expected ( after alignas", t => t.Value == "(");
                    var contents = first.GetMatchingRangeIn(toks);
                    toks = range(contents.End + 1, toks.End);
                }
                else
                {
                    break;
                }
            }

            // http://nongnu.org/hcb/#class-head-name
            first = toks.First(NonWhitespace);
            if (!identifierPattern.Match(first.Value).Success)
            {
                return false;
            }
            while (true)
            {
                first.Assert("Expected identifier", t => identifierPattern.Match(t.Value).Success);
                name += first.Value;
                toks = range(first.Position + 1, toks.End);
                if (toks.First(NonWhitespace).Value == "::")
                {
                    name += "::";
                    toks = toks.SkipWhile(Whitespace).Skip(1);
                }
                else
                {
                    break;
                }
                first = toks.First(NonWhitespace);
            }
            // http://nongnu.org/hcb/#class-virt-specifier-seq
            toks = toks.SkipWhile(t =>
                Whitespace(t) || t.Value == "final" || t.Value == "explicit"
            );

            first = toks.First(NonWhitespace);
            if (first.Value == ":" || first.Value == "{")
            {
                // At this point we've confirmed that this is a class.
                return true;
            }
            return false;
        }

        public void Write(System.IO.TextWriter writer, string destFileName)
        {
            writer.NewLine = "\n";
            writer.WriteLine("#define POST_ACTOR_COMPILER 1");
            int outLine = 1;
            if (LineNumbersEnabled)
            {
                writer.WriteLine("#line {0} \"{1}\"", tokens[0].SourceLine, sourceFile);
                outLine++;
            }
            int inBlocks = 0;
            Stack<ClassContext> classContextStack = new Stack<ClassContext>();
            for (int i = 0; i < tokens.Length; i++)
            {
                if (tokens[0].SourceLine == 0)
                {
                    throw new Exception("Internal error: Invalid source line (0)");
                }
                if (tokens[i].Value == "ACTOR" || tokens[i].Value == "SWIFT_ACTOR" || tokens[i].Value == "TEST_CASE")
                {
                    var actor = ParseActor(i, out int end);
                    if (classContextStack.Count > 0)
                    {
                        actor.enclosingClass = String.Join(
                            "::",
                            classContextStack.Reverse().Select(t => t.name)
                        );
                    }
                    var actorWriter = new System.IO.StringWriter();
                    actorWriter.NewLine = "\n";
                    var actorCompiler = new ActorCompiler(
                        actor,
                        sourceFile,
                        inBlocks == 0,
                        LineNumbersEnabled,
                        generateProbes
                    );
                    actorCompiler.Write(actorWriter);
                    this.uidObjects.AddRange(actorCompiler.uidObjects);

                    string[] actorLines = actorWriter.ToString().Split('\n');

                    bool hasLineNumber = false;
                    bool hadLineNumber = true;
                    foreach (var line in actorLines)
                    {
                        if (LineNumbersEnabled)
                        {
                            bool isLineNumber = line.Contains("#line");
                            if (isLineNumber)
                                hadLineNumber = true;
                            if (!isLineNumber && !hasLineNumber && hadLineNumber)
                            {
                                writer.WriteLine(
                                    "\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t#line {0} \"{1}\"",
                                    outLine + 1,
                                    destFileName
                                );
                                outLine++;
                                hadLineNumber = false;
                            }
                            hasLineNumber = isLineNumber;
                        }
                        writer.WriteLine(line.TrimEnd('\n', '\r'));
                        outLine++;
                    }

                    i = end;
                    if (i != tokens.Length && LineNumbersEnabled)
                    {
                        writer.WriteLine("#line {0} \"{1}\"", tokens[i].SourceLine, sourceFile);
                        outLine++;
                    }
                }
                else if (tokens[i].Value == "DESCR")
                {
                    int end;
                    var descr = ParseDescr(i, out end);
                    int lines;
                    new DescrCompiler(descr, tokens[i].BraceDepth).Write(writer, out lines);
                    i = end;
                    outLine += lines;
                    if (i != tokens.Length && LineNumbersEnabled)
                    {
                        writer.WriteLine("#line {0} \"{1}\"", tokens[i].SourceLine, sourceFile);
                        outLine++;
                    }
                }
                else if (
                    tokens[i].Value == "class"
                    || tokens[i].Value == "struct"
                    || tokens[i].Value == "union"
                )
                {
                    writer.Write(tokens[i].Value);
                    string name;
                    if (ParseClassContext(range(i + 1, tokens.Length), out name))
                    {
                        classContextStack.Push(
                            new ClassContext { name = name, inBlocks = inBlocks }
                        );
                    }
                }
                else
                {
                    if (tokens[i].Value == "{")
                    {
                        inBlocks++;
                    }
                    else if (tokens[i].Value == "}")
                    {
                        inBlocks--;
                        if (
                            classContextStack.Count > 0
                            && classContextStack.Peek().inBlocks == inBlocks
                        )
                        {
                            classContextStack.Pop();
                        }
                    }
                    writer.Write(tokens[i].Value);
                    outLine += tokens[i].Value.Count(c => c == '\n');
                }
            }
        }

        IEnumerable<TokenRange> SplitParameterList(TokenRange toks, string delimiter)
        {
            if (toks.Begin == toks.End)
                yield break;
            while (true)
            {
                Token comma = AngleBracketParser
                    .NotInsideAngleBrackets(toks)
                    .FirstOrDefault(t =>
                        t.Value == delimiter && t.ParenDepth == toks.First().ParenDepth
                    );
                if (comma == null)
                    break;
                yield return range(toks.Begin, comma.Position);
                toks = range(comma.Position + 1, toks.End);
            }
            yield return toks;
        }

        IEnumerable<Token> NormalizeWhitespace(IEnumerable<Token> tokens)
        {
            bool inWhitespace = false;
            bool leading = true;
            foreach (var tok in tokens)
            {
                if (!tok.IsWhitespace)
                {
                    if (inWhitespace && !leading)
                        yield return new Token { Value = " " };
                    inWhitespace = false;
                    yield return tok;
                    leading = false;
                }
                else
                {
                    inWhitespace = true;
                }
            }
        }

        void ParseDeclaration(
            TokenRange tokens,
            out Token name,
            out TokenRange type,
            out TokenRange initializer,
            out bool constructorSyntax
        )
        {
            initializer = null;
            TokenRange beforeInitializer = tokens;
            constructorSyntax = false;

            Token equals = AngleBracketParser
                .NotInsideAngleBrackets(tokens)
                .FirstOrDefault(t => t.Value == "=" && t.ParenDepth == tokens.First().ParenDepth);
            if (equals != null)
            {
                // type name = initializer;
                beforeInitializer = range(tokens.Begin, equals.Position);
                initializer = range(equals.Position + 1, tokens.End);
            }
            else
            {
                Token paren = AngleBracketParser
                    .NotInsideAngleBrackets(tokens)
                    .FirstOrDefault(t => t.Value == "(");
                if (paren != null)
                {
                    // type name(initializer);
                    constructorSyntax = true;
                    beforeInitializer = range(tokens.Begin, paren.Position);
                    initializer = range(paren.Position + 1, tokens.End)
                        .TakeWhile(t => t.ParenDepth > paren.ParenDepth);
                }
                else
                {
                    Token brace = AngleBracketParser
                        .NotInsideAngleBrackets(tokens)
                        .FirstOrDefault(t => t.Value == "{");
                    if (brace != null)
                    {
                        // type name{initializer};
                        throw new Error(
                            brace.SourceLine,
                            "Uniform initialization syntax is not currently supported for state variables (use '(' instead of '}}' ?)"
                        );
                    }
                }
            }
            name = beforeInitializer.Last(NonWhitespace);
            if (beforeInitializer.Begin == name.Position)
                throw new Error(beforeInitializer.First().SourceLine, "Declaration has no type.");
            type = range(beforeInitializer.Begin, name.Position);
        }

        VarDeclaration ParseVarDeclaration(TokenRange tokens)
        {
            Token name;
            TokenRange type,
                initializer;
            bool constructorSyntax;
            ParseDeclaration(tokens, out name, out type, out initializer, out constructorSyntax);
            return new VarDeclaration
            {
                name = name.Value,
                type = str(NormalizeWhitespace(type)),
                initializer = initializer == null ? "" : str(NormalizeWhitespace(initializer)),
                initializerConstructorSyntax = constructorSyntax
            };
        }

        readonly Func<Token, bool> Whitespace = (Token t) => t.IsWhitespace;
        readonly Func<Token, bool> NonWhitespace = (Token t) => !t.IsWhitespace;

        void ParseDescrHeading(Descr descr, TokenRange toks)
        {
            toks.First(NonWhitespace).Assert("non-struct DESCR!", t => t.Value == "struct");
            toks = toks.SkipWhile(Whitespace).Skip(1).SkipWhile(Whitespace);

            var colon = toks.FirstOrDefault(t => t.Value == ":");
            if (colon != null)
            {
                descr.superClassList = str(range(colon.Position + 1, toks.End)).Trim();
                toks = range(toks.Begin, colon.Position);
            }
            descr.name = str(toks).Trim();
        }

        void ParseTestCaseHeading(Actor actor, TokenRange toks)
        {
            actor.isStatic = true;

            // The parameter(s) to the TEST_CASE macro are opaque to the actor compiler
            TokenRange paramRange = toks.Last(NonWhitespace)
                .Assert(
                    "Unexpected tokens after test case parameter list.",
                    t => t.Value == ")" && t.ParenDepth == toks.First().ParenDepth
                )
                .GetMatchingRangeIn(toks);
            actor.testCaseParameters = str(paramRange);

            actor.name = "flowTestCase" + toks.First().SourceLine;
            actor.parameters = new VarDeclaration[]
            {
                new VarDeclaration
                {
                    name = "params",
                    type = "UnitTestParameters",
                    initializer = "",
                    initializerConstructorSyntax = false
                }
            };
            actor.returnType = "Void";
        }

        void ParseActorHeading(Actor actor, TokenRange toks)
        {
            var template = toks.First(NonWhitespace);
            if (template.Value == "template")
            {
                var templateParams = range(template.Position + 1, toks.End)
                    .First(NonWhitespace)
                    .Assert("Invalid template declaration", t => t.Value == "<")
                    .GetMatchingRangeIn(toks);

                actor.templateFormals = SplitParameterList(templateParams, ",")
                    .Select(p => ParseVarDeclaration(p)) //< SOMEDAY: ?
                    .ToArray();

                toks = range(templateParams.End + 1, toks.End);
            }
            var attribute = toks.First(NonWhitespace);
            while (attribute.Value == "[")
            {
                var attributeContents = attribute.GetMatchingRangeIn(toks);

                var asArray = attributeContents.ToArray();
                if (
                    asArray.Length < 2
                    || asArray[0].Value != "["
                    || asArray[asArray.Length - 1].Value != "]"
                )
                {
                    throw new Error(actor.SourceLine, "Invalid attribute: Expected [[...]]");
                }
                actor.attributes.Add("[" + str(NormalizeWhitespace(attributeContents)) + "]");
                toks = range(attributeContents.End + 1, toks.End);

                attribute = toks.First(NonWhitespace);
            }

            var staticKeyword = toks.First(NonWhitespace);
            if (staticKeyword.Value == "static")
            {
                actor.isStatic = true;
                toks = range(staticKeyword.Position + 1, toks.End);
            }

            var uncancellableKeyword = toks.First(NonWhitespace);
            if (uncancellableKeyword.Value == "UNCANCELLABLE")
            {
                actor.SetUncancellable();
                toks = range(uncancellableKeyword.Position + 1, toks.End);
            }

            // Find the parameter list
            TokenRange paramRange = toks.Last(NonWhitespace)
                .Assert(
                    "Unexpected tokens after actor parameter list.",
                    t => t.Value == ")" && t.ParenDepth == toks.First().ParenDepth
                )
                .GetMatchingRangeIn(toks);
            actor.parameters = SplitParameterList(paramRange, ",")
                .Select(p => ParseVarDeclaration(p))
                .ToArray();

            var name = range(toks.Begin, paramRange.Begin - 1).Last(NonWhitespace);
            actor.name = name.Value;

            // SOMEDAY: refactor?
            var returnType = range(toks.First().Position + 1, name.Position).SkipWhile(Whitespace);
            var retToken = returnType.First();
            if (retToken.Value == "Future")
            {
                var ofType = returnType
                    .Skip(1)
                    .First(NonWhitespace)
                    .Assert("Expected <", tok => tok.Value == "<")
                    .GetMatchingRangeIn(returnType);
                actor.returnType = str(NormalizeWhitespace(ofType));
                toks = range(ofType.End + 1, returnType.End);
            }
            else if (
                retToken.Value == "void" /* && !returnType.Skip(1).Any(NonWhitespace)*/
            )
            {
                actor.returnType = null;
                toks = returnType.Skip(1);
            }
            else
                throw new Error(actor.SourceLine, "Actor apparently does not return Future<T>");

            toks = toks.SkipWhile(Whitespace);
            if (!toks.IsEmpty)
            {
                if (toks.Last().Value == "::")
                {
                    actor.nameSpace = str(range(toks.Begin, toks.End - 1));
                }
                else
                {
                    Console.WriteLine(
                        "Tokens: '{0}' {1} '{2}'",
                        str(toks),
                        toks.Count(),
                        toks.Last().Value
                    );
                    throw new Error(
                        actor.SourceLine,
                        "Unrecognized tokens preceding parameter list in actor declaration"
                    );
                }
            }
            if (
                errorMessagePolicy.ActorsNoDiscardByDefault()
                && !actor.attributes.Contains("[[flow_allow_discard]]")
            )
            {
                if (actor.IsCancellable())
                {
                    actor.attributes.Add("[[nodiscard]]");
                }
            }
            HashSet<string> knownFlowAttributes = new HashSet<string>();
            knownFlowAttributes.Add("[[flow_allow_discard]]");
            foreach (var flowAttribute in actor.attributes.Where(a => a.StartsWith("[[flow_")))
            {
                if (!knownFlowAttributes.Contains(flowAttribute))
                {
                    throw new Error(actor.SourceLine, "Unknown flow attribute {0}", flowAttribute);
                }
            }
            actor.attributes = actor.attributes.Where(a => !a.StartsWith("[[flow_")).ToList();
        }

        LoopStatement ParseLoopStatement(TokenRange toks)
        {
            return new LoopStatement { body = ParseCompoundStatement(toks.Consume("loop")) };
        }

        ChooseStatement ParseChooseStatement(TokenRange toks)
        {
            return new ChooseStatement { body = ParseCompoundStatement(toks.Consume("choose")) };
        }

        WhenStatement ParseWhenStatement(TokenRange toks)
        {
            var expr = toks.Consume("when")
                .SkipWhile(Whitespace)
                .First()
                .Assert("Expected (", t => t.Value == "(")
                .GetMatchingRangeIn(toks)
                .SkipWhile(Whitespace);

            return new WhenStatement
            {
                wait = ParseWaitStatement(expr),
                body = ParseCompoundStatement(range(expr.End + 1, toks.End))
            };
        }

        StateDeclarationStatement ParseStateDeclaration(TokenRange toks)
        {
            toks = toks.Consume("state").RevSkipWhile(t => t.Value == ";");
            return new StateDeclarationStatement { decl = ParseVarDeclaration(toks) };
        }

        ReturnStatement ParseReturnStatement(TokenRange toks)
        {
            toks = toks.Consume("return").RevSkipWhile(t => t.Value == ";");
            return new ReturnStatement { expression = str(NormalizeWhitespace(toks)) };
        }

        ThrowStatement ParseThrowStatement(TokenRange toks)
        {
            toks = toks.Consume("throw").RevSkipWhile(t => t.Value == ";");
            return new ThrowStatement { expression = str(NormalizeWhitespace(toks)) };
        }

        WaitStatement ParseWaitStatement(TokenRange toks)
        {
            WaitStatement ws = new WaitStatement();
            ws.FirstSourceLine = toks.First().SourceLine;
            if (toks.First().Value == "state")
            {
                ws.resultIsState = true;
                toks = toks.Consume("state");
            }
            TokenRange initializer;
            if (toks.First().Value == "wait" || toks.First().Value == "waitNext")
            {
                initializer = toks.RevSkipWhile(t => t.Value == ";");
                ws.result = new VarDeclaration
                {
                    name = "_",
                    type = "Void",
                    initializer = "",
                    initializerConstructorSyntax = false
                };
            }
            else
            {
                Token name;
                TokenRange type;
                bool constructorSyntax;
                ParseDeclaration(
                    toks.RevSkipWhile(t => t.Value == ";"),
                    out name,
                    out type,
                    out initializer,
                    out constructorSyntax
                );

                string typestring = str(NormalizeWhitespace(type));
                if (typestring == "Void")
                {
                    throw new Error(
                        ws.FirstSourceLine,
                        "Assigning the result of a Void wait is not allowed.  Just use a standalone wait statement."
                    );
                }

                ws.result = new VarDeclaration
                {
                    name = name.Value,
                    type = str(NormalizeWhitespace(type)),
                    initializer = "",
                    initializerConstructorSyntax = false
                };
            }

            if (initializer == null)
                throw new Error(
                    ws.FirstSourceLine,
                    "Wait statement must be a declaration or standalone statement"
                );

            var waitParams = initializer
                .SkipWhile(Whitespace)
                .Consume(
                    "Statement contains a wait, but is not a valid wait statement or a supported compound statement.1",
                    t =>
                    {
                        if (t.Value == "wait")
                            return true;
                        if (t.Value == "waitNext")
                        {
                            ws.isWaitNext = true;
                            return true;
                        }
                        return false;
                    }
                )
                .SkipWhile(Whitespace)
                .First()
                .Assert("Expected (", t => t.Value == "(")
                .GetMatchingRangeIn(initializer);
            if (!range(waitParams.End, initializer.End).Consume(")").All(Whitespace))
            {
                throw new Error(
                    toks.First().SourceLine,
                    "Statement contains a wait, but is not a valid wait statement or a supported compound statement.2"
                );
            }

            ws.futureExpression = str(NormalizeWhitespace(waitParams));
            return ws;
        }

        WhileStatement ParseWhileStatement(TokenRange toks)
        {
            var expr = toks.Consume("while")
                .First(NonWhitespace)
                .Assert("Expected (", t => t.Value == "(")
                .GetMatchingRangeIn(toks);
            return new WhileStatement
            {
                expression = str(NormalizeWhitespace(expr)),
                body = ParseCompoundStatement(range(expr.End + 1, toks.End))
            };
        }

        Statement ParseForStatement(TokenRange toks)
        {
            var head = toks.Consume("for")
                .First(NonWhitespace)
                .Assert("Expected (", t => t.Value == "(")
                .GetMatchingRangeIn(toks);

            Token[] delim = head.Where(t =>
                    t.ParenDepth == head.First().ParenDepth
                    && t.BraceDepth == head.First().BraceDepth
                    && t.Value == ";"
                )
                .ToArray();
            if (delim.Length == 2)
            {
                var init = range(head.Begin, delim[0].Position);
                var cond = range(delim[0].Position + 1, delim[1].Position);
                var next = range(delim[1].Position + 1, head.End);
                var body = range(head.End + 1, toks.End);

                return new ForStatement
                {
                    initExpression = str(NormalizeWhitespace(init)),
                    condExpression = str(NormalizeWhitespace(cond)),
                    nextExpression = str(NormalizeWhitespace(next)),
                    body = ParseCompoundStatement(body)
                };
            }

            delim = head.Where(t =>
                    t.ParenDepth == head.First().ParenDepth
                    && t.BraceDepth == head.First().BraceDepth
                    && t.Value == ":"
                )
                .ToArray();
            if (delim.Length != 1)
            {
                throw new Error(
                    head.First().SourceLine,
                    "for statement must be 3-arg style or c++11 2-arg style"
                );
            }

            return new RangeForStatement
            {
                // The container over which to iterate
                rangeExpression = str(
                    NormalizeWhitespace(
                        range(delim[0].Position + 1, head.End).SkipWhile(Whitespace)
                    )
                ),
                // Type and name of the variable assigned in each iteration
                rangeDecl = str(
                    NormalizeWhitespace(
                        range(head.Begin, delim[0].Position - 1).SkipWhile(Whitespace)
                    )
                ),
                // The body of the for loop
                body = ParseCompoundStatement(range(head.End + 1, toks.End))
            };
        }

        Statement ParseIfStatement(TokenRange toks)
        {
            toks = toks.Consume("if");
            toks = toks.SkipWhile(Whitespace);
            bool constexpr = toks.First().Value == "constexpr";
            if (constexpr)
            {
                toks = toks.Consume("constexpr").SkipWhile(Whitespace);
            }

            var expr = toks.First(NonWhitespace)
                .Assert("Expected (", t => t.Value == "(")
                .GetMatchingRangeIn(toks);
            return new IfStatement
            {
                expression = str(NormalizeWhitespace(expr)),
                constexpr = constexpr,
                ifBody = ParseCompoundStatement(range(expr.End + 1, toks.End))
                // elseBody will be filled in later if necessary by ParseElseStatement
            };
        }

        void ParseElseStatement(TokenRange toks, Statement prevStatement)
        {
            var ifStatement = prevStatement as IfStatement;
            while (ifStatement != null && ifStatement.elseBody != null)
                ifStatement = ifStatement.elseBody as IfStatement;
            if (ifStatement == null)
                throw new Error(toks.First().SourceLine, "else without matching if");
            ifStatement.elseBody = ParseCompoundStatement(toks.Consume("else"));
        }

        Statement ParseTryStatement(TokenRange toks)
        {
            return new TryStatement
            {
                tryBody = ParseCompoundStatement(toks.Consume("try")),
                catches = new List<TryStatement.Catch>() // will be filled in later by ParseCatchStatement
            };
        }

        void ParseCatchStatement(TokenRange toks, Statement prevStatement)
        {
            var tryStatement = prevStatement as TryStatement;
            if (tryStatement == null)
                throw new Error(toks.First().SourceLine, "catch without matching try");
            var expr = toks.Consume("catch")
                .First(NonWhitespace)
                .Assert("Expected (", t => t.Value == "(")
                .GetMatchingRangeIn(toks);
            tryStatement.catches.Add(
                new TryStatement.Catch
                {
                    expression = str(NormalizeWhitespace(expr)),
                    body = ParseCompoundStatement(range(expr.End + 1, toks.End)),
                    FirstSourceLine = expr.First().SourceLine
                }
            );
        }

        static readonly HashSet<string> IllegalKeywords = new HashSet<string>
        {
            "goto",
            "do",
            "finally",
            "__if_exists",
            "__if_not_exists"
        };

        void ParseDeclaration(TokenRange toks, List<Declaration> declarations)
        {
            Declaration dec = new Declaration();

            Token delim = toks.First(t => t.Value == ";");
            var nameRange = range(toks.Begin, delim.Position)
                .RevSkipWhile(Whitespace)
                .RevTakeWhile(NonWhitespace);
            var typeRange = range(toks.Begin, nameRange.Begin);
            var commentRange = range(delim.Position + 1, toks.End);

            dec.name = str(nameRange).Trim();
            dec.type = str(typeRange).Trim();
            dec.comment = str(commentRange).Trim().TrimStart('/');

            declarations.Add(dec);
        }

        void ParseStatement(TokenRange toks, List<Statement> statements)
        {
            toks = toks.SkipWhile(Whitespace);

            Action<Statement> Add = stmt =>
            {
                stmt.FirstSourceLine = toks.First().SourceLine;
                statements.Add(stmt);
            };

            switch (toks.First().Value)
            {
                case "loop":
                    Add(ParseLoopStatement(toks));
                    break;
                case "while":
                    Add(ParseWhileStatement(toks));
                    break;
                case "for":
                    Add(ParseForStatement(toks));
                    break;
                case "break":
                    Add(new BreakStatement());
                    break;
                case "continue":
                    Add(new ContinueStatement());
                    break;
                case "return":
                    Add(ParseReturnStatement(toks));
                    break;
                case "{":
                    Add(ParseCompoundStatement(toks));
                    break;
                case "if":
                    Add(ParseIfStatement(toks));
                    break;
                case "else":
                    ParseElseStatement(toks, statements[statements.Count - 1]);
                    break;
                case "choose":
                    Add(ParseChooseStatement(toks));
                    break;
                case "when":
                    Add(ParseWhenStatement(toks));
                    break;
                case "try":
                    Add(ParseTryStatement(toks));
                    break;
                case "catch":
                    ParseCatchStatement(toks, statements[statements.Count - 1]);
                    break;
                case "throw":
                    Add(ParseThrowStatement(toks));
                    break;
                default:
                    if (IllegalKeywords.Contains(toks.First().Value))
                        throw new Error(
                            toks.First().SourceLine,
                            "Statement '{0}' not supported in actors.",
                            toks.First().Value
                        );
                    if (toks.Any(t => t.Value == "wait" || t.Value == "waitNext"))
                        Add(ParseWaitStatement(toks));
                    else if (toks.First().Value == "state")
                        Add(ParseStateDeclaration(toks));
                    else if (toks.First().Value == "switch" && toks.Any(t => t.Value == "return"))
                        throw new Error(
                            toks.First().SourceLine,
                            "Unsupported compound statement containing return."
                        );
                    else if (toks.First().Value.StartsWith("#"))
                        throw new Error(
                            toks.First().SourceLine,
                            "Found \"{0}\". Preprocessor directives are not supported within ACTORs",
                            toks.First().Value
                        );
                    else if (toks.RevSkipWhile(t => t.Value == ";").Any(NonWhitespace))
                        Add(
                            new PlainOldCodeStatement
                            {
                                code =
                                    str(NormalizeWhitespace(toks.RevSkipWhile(t => t.Value == ";")))
                                    + ";"
                            }
                        );
                    break;
            }
            ;
        }

        Statement ParseCompoundStatement(TokenRange toks)
        {
            var first = toks.First(NonWhitespace);
            if (first.Value == "{")
            {
                var inBraces = first.GetMatchingRangeIn(toks);
                if (!range(inBraces.End, toks.End).Consume("}").All(Whitespace))
                    throw new Error(
                        inBraces.Last().SourceLine,
                        "Unexpected tokens after compound statement"
                    );
                return ParseCodeBlock(inBraces);
            }
            else
            {
                List<Statement> statements = new List<Statement>();
                ParseStatement(toks.Skip(1), statements);
                return statements[0];
            }
        }

        List<Declaration> ParseDescrCodeBlock(TokenRange toks)
        {
            List<Declaration> declarations = new List<Declaration>();
            while (true)
            {
                Token delim = toks.FirstOrDefault(t => t.Value == ";");
                if (delim == null)
                    break;

                int pos = delim.Position + 1;
                var potentialComment = range(pos, toks.End)
                    .SkipWhile(t => t.Value == "\t" || t.Value == " ");
                if (!potentialComment.IsEmpty && potentialComment.First().Value.StartsWith("//"))
                {
                    pos = potentialComment.First().Position + 1;
                }

                ParseDeclaration(range(toks.Begin, pos), declarations);

                toks = range(pos, toks.End);
            }
            if (!toks.All(Whitespace))
                throw new Error(
                    toks.First(NonWhitespace).SourceLine,
                    "Trailing unterminated statement in code block"
                );
            return declarations;
        }

        CodeBlock ParseCodeBlock(TokenRange toks)
        {
            List<Statement> statements = new List<Statement>();
            while (true)
            {
                Token delim = toks.FirstOrDefault(t =>
                    t.ParenDepth == toks.First().ParenDepth
                    && t.BraceDepth == toks.First().BraceDepth
                    && (t.Value == ";" || t.Value == "}")
                );
                if (delim == null)
                    break;
                ParseStatement(range(toks.Begin, delim.Position + 1), statements);
                toks = range(delim.Position + 1, toks.End);
            }
            if (!toks.All(Whitespace))
                throw new Error(
                    toks.First(NonWhitespace).SourceLine,
                    "Trailing unterminated statement in code block"
                );
            return new CodeBlock { statements = statements.ToArray() };
        }

        TokenRange range(int beginPos, int endPos)
        {
            return new TokenRange(tokens, beginPos, endPos);
        }

        Descr ParseDescr(int pos, out int end)
        {
            var descr = new Descr();
            var toks = range(pos + 1, tokens.Length);
            var heading = toks.TakeWhile(t => t.Value != "{");
            var body = range(heading.End + 1, tokens.Length)
                .TakeWhile(t => t.BraceDepth > toks.First().BraceDepth || t.Value == ";"); //assumes no whitespace between the last "}" and the ";"

            ParseDescrHeading(descr, heading);
            descr.body = ParseDescrCodeBlock(body);

            end = body.End + 1;
            return descr;
        }

        Actor ParseActor(int pos, out int end)
        {
            var actor = new Actor();
            var head_token = tokens[pos];
            actor.SourceLine = head_token.SourceLine;

            var toks = range(pos + 1, tokens.Length);
            var heading = toks.TakeWhile(t => t.Value != "{");
            var toSemicolon = toks.TakeWhile(t => t.Value != ";");
            actor.isForwardDeclaration = toSemicolon.Length < heading.Length;
            if (actor.isForwardDeclaration)
            {
                heading = toSemicolon;
                if (head_token.Value == "ACTOR")
                {
                    ParseActorHeading(actor, heading);
                }
                else
                {
                    head_token.Assert("ACTOR expected!", t => false);
                }
                end = heading.End + 1;
            }
            else
            {
                var body = range(heading.End + 1, tokens.Length)
                    .TakeWhile(t => t.BraceDepth > toks.First().BraceDepth);

                if (head_token.Value == "ACTOR" || head_token.Value == "SWIFT_ACTOR")
                {
                    ParseActorHeading(actor, heading);
                }
                else if (head_token.Value == "TEST_CASE")
                {
                    ParseTestCaseHeading(actor, heading);
                    actor.isTestCase = true;
                }
                else
                    head_token.Assert("ACTOR or TEST_CASE expected!", t => false);

                actor.body = ParseCodeBlock(body);

                if (!actor.body.containsWait())
                    this.errorMessagePolicy.HandleActorWithoutWait(sourceFile, actor);

                end = body.End + 1;
            }
            return actor;
        }

        string str(IEnumerable<Token> tokens)
        {
            return string.Join("", tokens.Select(x => x.Value).ToArray());
        }

        string str(int begin, int end)
        {
            return str(range(begin, end));
        }

        void CountParens()
        {
            int BraceDepth = 0,
                ParenDepth = 0,
                LineCount = 1;
            Token lastParen = null,
                lastBrace = null;
            for (int i = 0; i < tokens.Length; i++)
            {
                switch (tokens[i].Value)
                {
                    case "}":
                        BraceDepth--;
                        break;
                    case ")":
                        ParenDepth--;
                        break;
                    case "\r\n":
                        LineCount++;
                        break;
                    case "\n":
                        LineCount++;
                        break;
                }
                if (BraceDepth < 0)
                    throw new Error(LineCount, "Mismatched braces");
                if (ParenDepth < 0)
                    throw new Error(LineCount, "Mismatched parenthesis");
                tokens[i].Position = i;
                tokens[i].SourceLine = LineCount;
                tokens[i].BraceDepth = BraceDepth;
                tokens[i].ParenDepth = ParenDepth;
                if (tokens[i].Value.StartsWith("/*"))
                    LineCount += tokens[i].Value.Count(c => c == '\n');
                switch (tokens[i].Value)
                {
                    case "{":
                        BraceDepth++;
                        if (BraceDepth == 1)
                            lastBrace = tokens[i];
                        break;
                    case "(":
                        ParenDepth++;
                        if (ParenDepth == 1)
                            lastParen = tokens[i];
                        break;
                }
            }
            if (BraceDepth != 0)
                throw new Error(lastBrace.SourceLine, "Unmatched brace");
            if (ParenDepth != 0)
                throw new Error(lastParen.SourceLine, "Unmatched parenthesis");
        }

        void showTokens()
        {
            foreach (var t in tokens)
            {
                if (t.Value == "\r\n")
                    Console.WriteLine();
                else if (t.Value.Length == 1)
                    Console.Write(t.Value);
                else
                    Console.Write("|{0}|", t.Value);
            }
        }

        readonly Regex identifierPattern = new Regex(
            @"\G[a-zA-Z_][a-zA-Z_0-9]*",
            RegexOptions.Singleline
        );

        readonly Regex[] tokenExpressions = (
            new string[]
            {
                @"\{",
                @"\}",
                @"\(",
                @"\)",
                @"\[",
                @"\]",
                @"//[^\n]*",
                @"/[*]([*][^/]|[^*])*[*]/",
                @"'(\\.|[^\'\n])*'", //< SOMEDAY: Not fully restrictive
                @"""(\\.|[^\""\n])*""",
                @"[a-zA-Z_][a-zA-Z_0-9]*",
                @"\r\n",
                @"\n",
                @"::",
                @":",
                @"#[a-z]*", // Recognize preprocessor directives so that we can reject them
                @".",
            }
        )
            .Select(x => new Regex(@"\G" + x, RegexOptions.Singleline))
            .ToArray();

        IEnumerable<string> Tokenize(string text)
        {
            int pos = 0;
            while (pos < text.Length)
            {
                bool ok = false;
                foreach (var re in tokenExpressions)
                {
                    var m = re.Match(text, pos);
                    if (m.Success)
                    {
                        yield return m.Value;
                        pos += m.Value.Length;
                        ok = true;
                        break;
                    }
                }
                if (!ok)
                    throw new Exception(String.Format("Can't tokenize! {0}", pos));
            }
        }
    }
}

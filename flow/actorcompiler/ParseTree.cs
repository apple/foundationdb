/*
 * ParseTree.cs
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
using System.Text.RegularExpressions;

namespace actorcompiler
{
    class VarDeclaration
    {
        public string type;
        public string name;
        public string initializer;
        public bool initializerConstructorSyntax;
    };

    abstract class Statement
    {
        public int FirstSourceLine;

        public virtual bool containsWait()
        {
            return false;
        }
    };

    class PlainOldCodeStatement : Statement
    {
        public string code;

        public override string ToString()
        {
            return code;
        }
    };

    class StateDeclarationStatement : Statement
    {
        public VarDeclaration decl;

        public override string ToString()
        {
            if (decl.initializerConstructorSyntax)
                return string.Format("State {0} {1}({2});", decl.type, decl.name, decl.initializer);
            else
                return string.Format(
                    "State {0} {1} = {2};",
                    decl.type,
                    decl.name,
                    decl.initializer
                );
        }
    };

    class WhileStatement : Statement
    {
        public string expression;
        public Statement body;

        public override bool containsWait()
        {
            return body.containsWait();
        }
    };

    class ForStatement : Statement
    {
        public string initExpression = "";
        public string condExpression = "";
        public string nextExpression = "";
        public Statement body;

        public override bool containsWait()
        {
            return body.containsWait();
        }
    };

    class RangeForStatement : Statement
    {
        public string rangeExpression;
        public string rangeDecl;
        public Statement body;

        public override bool containsWait()
        {
            return body.containsWait();
        }
    };

    class LoopStatement : Statement
    {
        public Statement body;

        public override string ToString()
        {
            return "Loop " + body.ToString();
        }

        public override bool containsWait()
        {
            return body.containsWait();
        }
    };

    class BreakStatement : Statement { };

    class ContinueStatement : Statement { };

    class IfStatement : Statement
    {
        public string expression;
        public bool constexpr;
        public Statement ifBody;
        public Statement elseBody; // might be null

        public override bool containsWait()
        {
            return ifBody.containsWait() || (elseBody != null && elseBody.containsWait());
        }
    };

    class ReturnStatement : Statement
    {
        public string expression;

        public override string ToString()
        {
            return "Return " + expression;
        }
    };

    class WaitStatement : Statement
    {
        public VarDeclaration result;
        public string futureExpression;
        public bool resultIsState;
        public bool isWaitNext;

        public override string ToString()
        {
            return string.Format(
                "Wait {0} {1} <- {2} ({3})",
                result.type,
                result.name,
                futureExpression,
                resultIsState ? "state" : "local"
            );
        }

        public override bool containsWait()
        {
            return true;
        }
    };

    class ChooseStatement : Statement
    {
        public Statement body;

        public override string ToString()
        {
            return "Choose " + body.ToString();
        }

        public override bool containsWait()
        {
            return body.containsWait();
        }
    };

    class WhenStatement : Statement
    {
        public WaitStatement wait;
        public Statement body;

        public override string ToString()
        {
            return string.Format("When ({0}) {1}", wait, body);
        }

        public override bool containsWait()
        {
            return true;
        }
    };

    class TryStatement : Statement
    {
        public struct Catch
        {
            public string expression;
            public Statement body;
            public int FirstSourceLine;
        };

        public Statement tryBody;
        public List<Catch> catches;

        public override bool containsWait()
        {
            if (tryBody.containsWait())
                return true;
            foreach (Catch c in catches)
                if (c.body.containsWait())
                    return true;
            return false;
        }
    };

    class ThrowStatement : Statement
    {
        public string expression;
    };

    class CodeBlock : Statement
    {
        public Statement[] statements;

        public override string ToString()
        {
            return string.Join(
                "\n",
                new string[] { "CodeBlock" }
                    .Concat(statements.Select(s => s.ToString()))
                    .Concat(new string[] { "EndCodeBlock" })
                    .ToArray()
            );
        }

        public override bool containsWait()
        {
            foreach (Statement s in statements)
                if (s.containsWait())
                    return true;
            return false;
        }
    };

    class Declaration
    {
        public string type;
        public string name;
        public string comment;
    };

    class Actor
    {
        public List<string> attributes = new List<string>();
        public string returnType;
        public string name;
        public string enclosingClass = null;
        public VarDeclaration[] parameters;
        public VarDeclaration[] templateFormals; //< null if not a template
        public CodeBlock body;
        public int SourceLine;
        public bool isStatic = false;
        private bool isUncancellable;
        public string testCaseParameters = null;
        public string nameSpace = null;
        public bool isForwardDeclaration = false;
        public bool isTestCase = false;

        public bool IsCancellable()
        {
            return returnType != null && !isUncancellable;
        }

        public void SetUncancellable()
        {
            isUncancellable = true;
        }
    };

    class Descr
    {
        public string name;
        public string superClassList;
        public List<Declaration> body;
    };
};

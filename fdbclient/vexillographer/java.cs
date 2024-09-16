/*
 * java.cs
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

﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace vexillographer
{
    class java : BindingWriter
    {
        private static string formatComment(int indentTabs, string commentText)
        {
            string tabs = "";
            for(int i=0; i<indentTabs; i++) tabs += "\t";
            return tabs + "/**" + 
                string.Join( "", 
                    commentText.Split('\n')
                        .Select( line=> "\n" + tabs + " * " + line.Trim() ) )
                + "\n" + tabs + " *" + "/";
        }

        private static string replaceTicks(string input)
        {
            int startIdx = input.IndexOf("``");
            int lastReplaceEnd = 0;
            string output = "";
            while (startIdx >= 0)
            {
                output += input.Substring(lastReplaceEnd, startIdx - lastReplaceEnd);
                int closingIdx = input.IndexOf("``", startIdx + 2);
                if (closingIdx < 0)
                    throw new InvalidDataException("No closing double tick");
                output += "{@code " + input.Substring(startIdx + 2, closingIdx - startIdx - 2) + "}";
                lastReplaceEnd = closingIdx + 2;
                startIdx = input.IndexOf("``", lastReplaceEnd);
            }
            output += input.Substring(lastReplaceEnd);
            return output;
        }

        private static string getEnum(Option o)
        {
            string mainComment = o.comment == "" ? "Currently undocumented." : o.comment;
            if (!mainComment.EndsWith("."))
                mainComment += ".";
            mainComment = replaceTicks(mainComment);
            return formatComment(1, mainComment) + 
                String.Format("\n{2}\t{0}({1})", o.name.ToUpper(), o.code, o.isDeprecated() ? "\t@Deprecated\n" : "");
        }

        class ScopeOptions
        {
            public bool isPublic;
            public bool isSettableOption;
            public String comment;

            public ScopeOptions(bool isSettable, String comment, bool isPublic = true) {
                this.isPublic = isPublic;
                this.isSettableOption = isSettable;
                this.comment = comment;
            }
        }

        private static Dictionary<Scope, ScopeOptions> scopeDocOptions = new Dictionary<Scope, ScopeOptions>()
            {
                { Scope.NetworkOption, new ScopeOptions(true, 
                    "A set of options that can be set globally for the {@link FDB FoundationDB API}.") },
                { Scope.DatabaseOption, new ScopeOptions(true, 
                    "A set of options that can be set on a {@link Database}.") },
                { Scope.TransactionOption, new ScopeOptions(true, 
                    "A set of options that can be set on a {@link Transaction}.") },
                { Scope.StreamingMode, new ScopeOptions(false, 
                    "Options that control the way the Java binding performs range reads. These options can be passed to {@link Transaction#getRange(byte[], byte[], int, boolean, StreamingMode) Transaction.getRange(...)}.") },
                { Scope.MutationType, new ScopeOptions(false, 
                    "A set of operations that can be performed atomically on a database. These are used as parameters to {@link Transaction#mutate(MutationType, byte[], byte[])}.") },
                { Scope.ConflictRangeType, new ScopeOptions(false, 
                    "Conflict range types used internally by the C API.", false) },
                { Scope.ErrorPredicate, new ScopeOptions(true, 
                    "Error code predicates for binding writers and non-standard layer implementers.") },
            };

        private static bool scopeIsSettableOption(Scope s) {
            return s != Scope.StreamingMode && s != Scope.MutationType && s != Scope.ConflictRangeType;
        }

        private static string toCamelCase(string optionName) {
            return string.Join("", 
                optionName
                    .Split('_')
                    .Select( s=>s.Substring(0,1).ToUpper() + s.Substring(1) )
                );
        }

        private static string toSetFuncName(string optionName) {
            return "set" + toCamelCase(optionName);
        }

        private static string toPredicateFuncName(string optionName) {
            return "is" + toCamelCase(optionName);
        }

        private static string getJavaTypeName(ParamType t)
        {
            switch (t)
            {
                case ParamType.Int: return "long";
                case ParamType.Bytes: return "byte[]";
                case ParamType.String: return "String";
                default: throw new InvalidDataException("Unsupported operation type");
            }
        }

        private static void writeOptionsClass(TextWriter outFile, Scope scope, IEnumerable<Option> options)
        {
            string className = scope.ToString() + "s";
            outFile.WriteLine( "package com.apple.foundationdb;" );
            outFile.WriteLine();

            outFile.WriteLine(formatComment(0, scopeDocOptions[scope].comment +
                (options.Count() == 0 ? "\n\nThere are currently no options available." : "")
                ));
            outFile.WriteLine("public class {0} extends OptionsSet {{", className);
            outFile.WriteLine("\tpublic {0}( OptionConsumer consumer ) {{ super(consumer); }}", className);
            foreach(var option in options) {
                if (!option.hidden) {
                    outFile.WriteLine();
                    if (option.comment != "") 
                    {
                        string comment = option.comment;
                        if (!comment.EndsWith("."))
                            comment += ".";
                        if (option.paramDesc != null)
                            comment += "\n\n@param value " + option.paramDesc;
                        outFile.WriteLine(formatComment(1, replaceTicks(comment)));
                    }
                    if (option.isDeprecated())
                        outFile.WriteLine("\t@Deprecated");
                    if (option.paramType == ParamType.None)
                        outFile.WriteLine("\tpublic void {0}() {{ setOption({1}); }}", toSetFuncName(option.name), option.code);
                    else
                        outFile.WriteLine("\tpublic void {0}({2} value) {{ setOption({1}, value); }}", toSetFuncName(option.name), option.code, getJavaTypeName(option.paramType));
                }
            }
            outFile.WriteLine("}");
        }

        private static void writePredicateClass(TextWriter outFile, Scope scope, IEnumerable<Option> options)
        {
            outFile.WriteLine(
@"package com.apple.foundationdb;

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
     *  are published <a href=""/foundationdb/api-error-codes.html"">elsewhere within
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
");

            foreach(var option in options) {
                if (!option.hidden) {
                    outFile.WriteLine();
                    if (option.comment != "") 
                    {
                        string comment = option.comment;
                        if (!comment.EndsWith("."))
                            comment += ".";
                        if (option.paramDesc != null)
                            comment += "\n\n@param value " + option.paramDesc;
                        comment += "\n\n@return {@code true} if this {@code FDBException} is {@code " + option.name + "}";
                        outFile.WriteLine(formatComment(1, replaceTicks(comment)));
                    }
                    if (option.isDeprecated())
                        outFile.WriteLine("\t@Deprecated");
                    outFile.WriteLine("\tpublic boolean {0}() {{ return FDB.evalErrorPredicate({1}, this.code); }}", toPredicateFuncName(option.name), option.code);
                }
            }
            outFile.WriteLine("}");
        }

        private static void writeEnumClass(TextWriter outFile, Scope scope, IEnumerable<Option> options)
        {
            string scopeName = scope.ToString();

            outFile.WriteLine( "package com.apple.foundationdb;" );
            outFile.WriteLine();
            outFile.WriteLine(formatComment(0, scopeDocOptions[scope].comment + 
                (options.Count() == 0 ? "\n\nThere are currently no options available." : "")));

            outFile.WriteLine("{0}enum {1} {{", scopeDocOptions[scope].isPublic ? "public " : "", scopeName);
            outFile.WriteLine(string.Join(",\n\n", options.Where(f => !f.hidden).Select(f => getEnum(f)).ToArray()) + ";");
            outFile.WriteLine(
@"
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
}}", scopeName);
        }

        public void writeFiles(string outputDirectory, IEnumerable<Option> options)
        {
            foreach (Scope s in Enum.GetValues(typeof(Scope)))
            {
                if (!Directory.Exists(outputDirectory))
                    throw new Exception(string.Format("Directory {0} does not exist", outputDirectory));

                string className = s.ToString();
                if (scopeDocOptions[s].isSettableOption) className += "s";
                string filePath = Path.Combine(outputDirectory, (s == Scope.ErrorPredicate ? "FDBException" : className) + ".java");
                using (var javaFile = System.IO.File.Open(filePath,
                                System.IO.FileMode.Create, System.IO.FileAccess.Write))
                {
                    TextWriter outFile = new StreamWriter(javaFile);
                    outFile.NewLine = "\r";
                    if (scopeDocOptions[s].isSettableOption) {
                        if(s == Scope.ErrorPredicate)
                            writePredicateClass(outFile, s, options.Where(o => o.scope == s).OrderBy(o=>o.comment==""));
                        else
                            writeOptionsClass(outFile, s, options.Where(o => o.scope == s).OrderBy(o=>o.comment==""));
                    }
                    else
                        writeEnumClass(outFile, s, options.Where(o => o.scope == s));
                    outFile.Flush();
                }
            }
        }
    }
}

/*
 * vexillographer.cs
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
using System.Linq;
using System.Xml.Linq;
using System.Text;
using System.IO;

namespace vexillographer
{
    public enum Scope
    {
        NetworkOption,
        DatabaseOption,
        TransactionOption,
        StreamingMode,
        MutationType,
        ConflictRangeType,
        ErrorPredicate
    }

    public enum ParamType
    {
        None,
        String,
        Int,
        Bytes
    }

    class Option
    {
        public Scope scope { get; set; }
        public string name { get; set; }
        public ParamType paramType { get; set; }
        public string paramDesc { get; set; }
        public int code { get; set; }
        public bool hidden { get; set; }
        public bool persistent { get; set; }
        public bool sensitive { get; set; }
        public int defaultFor { get; set; }
        private string _comment;
        public string comment {
            get {
                return _comment == null ? "" : _comment;
            }
            set {
                _comment = value;
            }
        }
        public bool isDeprecated() { return comment.StartsWith("Deprecated"); }

        public string getParameterComment()
        {
            return paramDesc == null ? "Option takes no parameter" : string.Format("({0}) {1}", paramType.ToString(), paramDesc);
        }
    }

    interface BindingWriter
    {
        void writeFiles(string outputFile, IEnumerable<Option> options);
    }

    static class vexillographer
    {
        static int Main(string[] args)
        {
            if (args.Length < 3)
            {
                usage();
                return 1;
            }

            IEnumerable<Option> options;
            int result = parseOptions(args[0], out options, args[1]);
            if (result != 0)
                return result;

            BindingWriter writer;
            try
            {
                Type t = Type.GetType("vexillographer." + args[1]);
                writer = (BindingWriter)Activator.CreateInstance(t);
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(string.Format("Could not load language binding for `{0}'", args[1]));
                Console.Error.WriteLine(e.StackTrace);
                usage();
                return 31;
            }

            writer.writeFiles(args[2], options);

            return 0;
        }

        private static void usage()
        {
            Console.WriteLine("{0} inputFile {{c,cpp,java,ruby,python}} <outputDirectory/outputFile>",
                Environment.GetCommandLineArgs()[0]);
        }

        private static int parseOptions(string path, out IEnumerable<Option> options, string binding)
        {

            var list = new List<Option>();
            try
            {
                var optionsDoc = XDocument.Load(path).Element("Options");
                foreach (var scopeDoc in optionsDoc.Elements("Scope"))
                {
                    var scopeName = scopeDoc.Attribute("name").Value;
                    Scope s = (Scope)Enum.Parse(typeof(Scope), scopeName);
                    foreach (var oDoc in scopeDoc.Elements("Option"))
                    {
                        var paramTypeStr = oDoc.AttributeOrNull("paramType");
                        ParamType p = paramTypeStr == null ? ParamType.None : (ParamType)Enum.Parse(typeof(ParamType), paramTypeStr);
                        bool hidden = oDoc.AttributeOrNull("hidden") == "true";
                        bool persistent = oDoc.AttributeOrNull("persistent") == "true";
                        bool sensitive = oDoc.AttributeOrNull("sensitive") == "true";
                        String defaultForString = oDoc.AttributeOrNull("defaultFor");
                        int defaultFor = defaultForString == null ? -1 : int.Parse(defaultForString);
                        string disableOn = oDoc.AttributeOrNull("disableOn");
                        bool disabled = false;
                        if(disableOn != null)
                        {
                            string[] disabledBindings = disableOn.Split(',');
                            disabled = disabledBindings.Contains(binding);
                        }

                        if (!disabled)
                        {
                            list.Add(new Option
                            {
                                scope = s,
                                name = oDoc.AttributeNonNull("name"),
                                code = int.Parse(oDoc.AttributeNonNull("code")),
                                paramType = p,
                                paramDesc = oDoc.AttributeOrNull("paramDescription"),
                                comment = oDoc.AttributeOrNull("description"),
                                hidden = hidden,
                                persistent = persistent,
                                sensitive = sensitive,
                                defaultFor = defaultFor
                            });
                        }
                    }
                }
                options = list;
                return 0;
            }
            catch (Exception)
            {
                options = null;
                return 1;
            }
        }

        public static string getDescription(this Scope s)
        {
            switch (s)
            {
                case Scope.NetworkOption:
                    return "NET_OPTION";
                case Scope.DatabaseOption:
                    return "DB_OPTION";
                case Scope.TransactionOption:
                    return "TR_OPTION";
                case Scope.StreamingMode:
                    return "STREAMING_MODE";
                case Scope.MutationType:
                    return "MUTATION_TYPE";
                case Scope.ConflictRangeType:
                    return "CONFLICT_RANGE_TYPE";
                case Scope.ErrorPredicate:
                    return "ERROR_PREDICATE";
            }
            throw new Exception("Enum type unknown");
        }

        public static string AttributeOrNull(this XElement e, string name)
        {
            var attr = e.Attribute(name);
            return attr == null ? null : attr.Value;
        }

        public static string AttributeNonNull(this XElement e, string name)
        {
            var attr = e.Attribute(name);
            if (attr == null)
                throw new Exception(string.Format("Option attribute {0} must exist and have a value", name));
            return attr.Value;
        }
    }
}

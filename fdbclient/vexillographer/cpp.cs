/*
 * cpp.cs
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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
    class cpp : BindingWriter
    {
        private static void writeCppEnum(TextWriter outFile, Scope scope, IEnumerable<Option> options)
        {
            outFile.WriteLine("struct FDB{0}s {{", scope.ToString());
            outFile.WriteLine("\tfriend class FDBOptionInfoMap<FDB{0}s>;",scope.ToString());
            outFile.WriteLine();
            outFile.WriteLine("\tenum Option : int {");
            outFile.WriteLine(string.Join(",\n\n", options.Select(f => c.getCLine(f, "\t\t", "")).ToArray()));
            outFile.WriteLine("\t};");
            outFile.WriteLine();
            outFile.WriteLine("\tstatic FDBOptionInfoMap<FDB{0}s> optionInfo;", scope.ToString());
            outFile.WriteLine();
            outFile.WriteLine("private:");
            outFile.WriteLine("\tstatic void init();");
            outFile.WriteLine("};");
            outFile.WriteLine("");
        }

        private static string getCInfoLine(Option o, string indent, string structName)
        {
            return String.Format("{0}ADD_OPTION_INFO({1}, {2}, \"{2}\", \"{3}\", \"{4}\", {5}, {6}, {7}, {8}, FDBOptionInfo::ParamType::{9})",
                indent, structName, o.name.ToUpper(), o.comment, o.getParameterComment(), (o.paramDesc != null).ToString().ToLower(), 
                o.hidden.ToString().ToLower(), o.persistent.ToString().ToLower(), o.defaultFor, o.paramType);
        }

        private static void writeCppInfo(TextWriter outFile, Scope scope, IEnumerable<Option> options)
        {
            outFile.WriteLine("FDBOptionInfoMap<FDB{0}s> FDB{0}s::optionInfo;", scope.ToString());
            outFile.WriteLine();
            outFile.WriteLine("void FDB{0}s::init() {{", scope.ToString());
            outFile.WriteLine(string.Join("\n", options.Select(f => getCInfoLine(f, "\t", "FDB{0}s")).ToArray()), scope.ToString());
            outFile.WriteLine("}");
            outFile.WriteLine();
        }

        public void writeFiles(string fileName, IEnumerable<Option> options)
        {
            using (var header = System.IO.File.Open(fileName + ".h",
                System.IO.FileMode.Create, System.IO.FileAccess.Write))
            {
                TextWriter outFile = new StreamWriter(header);
                outFile.NewLine = "\n";
                outFile.WriteLine("#ifndef FDBCLIENT_FDBOPTIONS_G_H");
                outFile.WriteLine("#define FDBCLIENT_FDBOPTIONS_G_H");
                outFile.WriteLine("#pragma once");
                outFile.WriteLine();
                outFile.WriteLine("#include \"fdbclient/FDBOptions.h\"");
                outFile.WriteLine();
                foreach (Scope s in Enum.GetValues(typeof(Scope)))
                {
                    writeCppEnum(outFile, s, options.Where(o => o.scope == s));
                }
                outFile.WriteLine("");
                outFile.WriteLine("#endif");
                outFile.Flush();
            }

            using (var cFile = System.IO.File.Open(fileName + ".cpp",
                System.IO.FileMode.Create, System.IO.FileAccess.Write))
            {
                TextWriter outFile = new StreamWriter(cFile);
                outFile.NewLine = "\n";
                outFile.WriteLine("#include \"fdbclient/FDBOptions.g.h\"");
                outFile.WriteLine();
                foreach (Scope s in Enum.GetValues(typeof(Scope))) 
                {
					writeCppInfo(outFile, s, options.Where(o => o.scope == s));    
                }
                outFile.Flush();
            }
        }


        
    }
}

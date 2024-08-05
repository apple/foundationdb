/*
 * c.cs
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

/*
 * c.cs
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
using System.IO;
using System.Linq;
using System.Text;

namespace vexillographer
{
    class c : BindingWriter
    {
        public static string getCLine(Option o, string indent, string prefix)
        {
            string parameterComment = "";
            if (o.scope.ToString().EndsWith("Option"))
                parameterComment = String.Format("{0}/* {1} {2}*/\n", indent, "Parameter: " + o.getParameterComment(), o.hidden ? "This is a hidden parameter and should not be used directly by applications." : "");
            return String.Format("{0}/* {2} */\n{5}{0}{1}{3}={4}", indent, prefix, o.comment, o.name.ToUpper(), o.code, parameterComment);
        }

        private static void writeCEnum(TextWriter outFile, Scope scope, IEnumerable<Option> options)
        {
            outFile.WriteLine("typedef enum {");
            string prefix = "FDB_" + scope.getDescription() + "_";
            if (options.Count() == 0)
                options = new Option[] { new Option{ scope = scope,
                    comment = "This option is only a placeholder for C compatibility and should not be used",
                    code = -1, name = "DUMMY_DO_NOT_USE", paramDesc = null } };
            outFile.WriteLine(string.Join(",\n\n", options.Select(f => getCLine(f, "    ", prefix)).ToArray()));
            outFile.WriteLine("}} FDB{0};", scope.ToString());
            outFile.WriteLine();
        }

        public void writeFiles(string fileName, IEnumerable<Option> options)
        {
            using (var cFile = System.IO.File.Open(fileName,
                System.IO.FileMode.Create, System.IO.FileAccess.Write))
            {
                TextWriter outFile = new StreamWriter(cFile);
                outFile.NewLine = "\n";
				outFile.WriteLine(@"#ifndef FDB_C_OPTIONS_G_H
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
");
                foreach (Scope s in Enum.GetValues(typeof(Scope)))
                {
                    writeCEnum(outFile, s, options.Where(o => o.scope == s));
                }
                outFile.WriteLine("#endif");
                outFile.Flush();
            }
        }


    }
}

/*
 * Program.cs
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
using System.IO;

namespace actorcompiler
{
    class Program
    {
        private static void OverwriteByMove(string target, string temporaryFile)
        {
            if (File.Exists(target))
            {
                File.SetAttributes(target, FileAttributes.Normal);
                File.Delete(target);
            }
            File.Move(temporaryFile, target);
            File.SetAttributes(target, FileAttributes.ReadOnly);
        }

        public static int Main(string[] args)
        {
            bool generateProbes = false;
            if (args.Length < 2)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine(
                    "  actorcompiler <input> <output> [--disable-diagnostics] [--generate-probes]"
                );
                return 100;
            }
            Console.WriteLine("actorcompiler {0}", string.Join(" ", args));
            string input = args[0],
                output = args[1],
                outputtmp = args[1] + ".tmp",
                outputUid = args[1] + ".uid";
            ErrorMessagePolicy errorMessagePolicy = new ErrorMessagePolicy();
            foreach (var arg in args)
            {
                if (arg.StartsWith("--"))
                {
                    if (arg.Equals("--disable-diagnostics"))
                    {
                        errorMessagePolicy.DisableDiagnostics = true;
                    }
                    else if (arg.Equals("--generate-probes"))
                    {
                        generateProbes = true;
                    }
                }
            }
            try
            {
                var inputData = File.ReadAllText(input);
                var parser = new ActorParser(
                    inputData,
                    input.Replace('\\', '/'),
                    errorMessagePolicy,
                    generateProbes
                );

                using (var outputStream = new StreamWriter(outputtmp))
                {
                    parser.Write(outputStream, output.Replace('\\', '/'));
                }
                OverwriteByMove(output, outputtmp);

                using (var outputStream = new StreamWriter(outputtmp))
                {
                    // FIXME The only reason this ugly format is used is that System.Text.Json is not supported
                    // in the build environment of Mono/.NET framework.
                    foreach (var item in parser.uidObjects)
                    {
                        outputStream.WriteLine(
                            "{0}|{1}|{2}|{3}|{4}|{5}",
                            item.guid1,
                            item.guid2,
                            item.fileName,
                            item.lineNumber,
                            item.type,
                            item.name
                        );
                    }
                }
                OverwriteByMove(outputUid, outputtmp);

                return 0;
            }
            catch (actorcompiler.Error e)
            {
                Console.Error.WriteLine(
                    "{0}({1}): error FAC1000: {2}",
                    input,
                    e.SourceLine,
                    e.Message
                );
                if (File.Exists(outputtmp))
                    File.Delete(outputtmp);
                if (File.Exists(output))
                {
                    File.SetAttributes(output, FileAttributes.Normal);
                    File.Delete(output);
                }
                return 1;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(
                    "{0}({1}): error FAC2000: Internal {2}",
                    input,
                    1,
                    e.ToString()
                );
                if (File.Exists(outputtmp))
                    File.Delete(outputtmp);
                if (File.Exists(output))
                {
                    File.SetAttributes(output, FileAttributes.Normal);
                    File.Delete(output);
                }
                return 3;
            }
        }
    }
}

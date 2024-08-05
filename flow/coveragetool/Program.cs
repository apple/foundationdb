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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Xml.Linq;
using System.Text.RegularExpressions;

namespace coveragetool
{
    class CoverageCase
    {
        public string File;
        public int Line;
        public string Comment;
        public string Condition;
    };

    class ParseException : Exception {
    }

    class Program
    {
        public static int Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Usage:");
                Console.WriteLine("  coveragetool [coveragefile] [inputpath]*");
                return 100;
            }

            bool quiet = true;
            if (Environment.GetEnvironmentVariable("VERBOSE") != null) {
                quiet = false;
            }

            if (!quiet) {
                Console.WriteLine("coveragetool {0}", string.Join(" ", args));
            }

            string output = args[0];
            string[] inputPaths = args.Skip(1).Where(p=>!p.Contains(".g.") && !p.Contains(".amalgamation.")).ToArray();

            var outputFile = new FileInfo( output );

            /*var allFiles = inputPaths.SelectMany( path =>
                new DirectoryInfo( Path.GetDirectoryName(path) )
                    .EnumerateFiles( Path.GetFileName(path), SearchOption.AllDirectories )
                ).ToArray();*/

            var exists = inputPaths.ToLookup(n=>n);

            CoverageCase[] cases = new CoverageCase[0];
            var outputTime = DateTime.MinValue;
            if (outputFile.Exists)
            {
                string[] oldArgs;
                ParseOutput(output, out cases, out oldArgs);
                if (oldArgs.Length == inputPaths.Length && !oldArgs.Zip(inputPaths,(a,b)=>a!=b).Any(b=>b))
                    outputTime = outputFile.LastWriteTimeUtc;
            }

            var changedFiles = inputPaths
                .Where( fi=>new FileInfo(fi).LastWriteTimeUtc > outputTime )
                .ToLookup(n=>n);

            try {
                cases = cases
                .Where(c => exists.Contains(c.File) && !changedFiles.Contains(c.File))
                .Concat( changedFiles.SelectMany( f => ParseSource( f.Key ) ) )
                .ToArray();
            } catch (ParseException) {
                return 1;
            }

            if (!quiet) {
                Console.WriteLine("  {0}/{1} files scanned", changedFiles.Count, inputPaths.Length);
                Console.WriteLine("  {0} coverage cases found", cases.Length);
            }

            WriteOutput(output, cases, inputPaths);

            return 0;
        }

        private static string ValueOrDefault(XAttribute attr, string def)
        {
            if (attr == null) return def;
            else return attr.Value;
        }

        public static void ParseOutput(string filename, out CoverageCase[] cases, out string[] args)
        {
            var doc = XDocument.Load(filename).Element("CoverageTool");
            cases =
                doc.Element("CoverageCases")
                    .Elements("Case")
                    .Select(c =>
                        new CoverageCase { File = c.Attribute("File").Value, Line = int.Parse(c.Attribute("Line").Value), Comment=c.Attribute("Comment").Value, Condition=ValueOrDefault(c.Attribute("Condition"),"") }
                        )
                    .ToArray();
            args =
                doc.Element("Inputs")
                    .Elements("Input")
                    .Select(i => i.Value)
                    .ToArray();
        }
        public static void WriteOutput(string filename, CoverageCase[] cases, string[] args)
        {
            var doc = new XDocument(
                new XElement("CoverageTool",
                    new XElement("CoverageCases",
                        cases.Select(c =>
                            new XElement("Case",
                                new XAttribute("File", c.File),
                                new XAttribute("Line", c.Line.ToString()),
                                new XAttribute("Comment", c.Comment),
                                new XAttribute("Condition", c.Condition)
                                )
                            )
                        ),
                    new XElement("Inputs",
                        args.Select(a => new XElement("Input", a)))
                ));
            doc.Save(filename);
        }
        public static CoverageCase[] ParseSource(string filename)
        {
            var regex = new Regex( @"^([^/]|/[^/])*\s+(TEST|INJECT_FAULT|SHOULD_INJECT_FAULT)[ \t]*\(([^)]*)\)" );

            var lines = File.ReadAllLines(filename);
            var res = Enumerable.Range(0, lines.Length)
                .Where( i=>regex.IsMatch(lines[i]) && !lines[i].StartsWith("#define") )
                .Select( i=>new CoverageCase {
                    File = filename,
                    Line = i+1,
                    Comment = FindComment(lines[i]),
                    Condition = regex.Match(lines[i]).Groups[3].Value
                    } )
                .ToArray();
            var comments = new Dictionary<string, CoverageCase>();
            bool failed = false;
            foreach(var coverageCase in res) {
                if (String.IsNullOrEmpty(coverageCase.Comment) || coverageCase.Comment.Trim() == "") {
                    failed = true;
                    Console.Error.WriteLine(String.Format("Error at {0}:{1}: Empty or missing comment", coverageCase.File, coverageCase.Line));
                }
                else if (comments.ContainsKey(coverageCase.Comment)) {
                    failed = true;
                    var prev = comments[coverageCase.Comment];
                    Console.Error.WriteLine(String.Format("Error at {0}:{1}: {2} is not a unique comment", coverageCase.File, coverageCase.Line, coverageCase.Comment));
                    Console.Error.WriteLine(String.Format("\tPreviously seen in {0} at {1}", prev.File, prev.Line));
                } else {
                    comments.Add(coverageCase.Comment, coverageCase);
                }
            }
            if (failed) {
                throw new ParseException();
            }
            return res;
        }
        public static string FindComment(string line)
        {
            int comment = line.IndexOf("//");
            if (comment == -1)
                return "";
            else
                return line.Substring(comment + 2).Trim();
        }
    }
}

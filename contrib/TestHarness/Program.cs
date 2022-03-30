/*
 * Program.cs
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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
using System.Threading;
using System.Xml.Linq;
using System.IO;
using System.Diagnostics;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Xml;
using System.Runtime.Serialization.Json;

namespace SummarizeTest
{
    static class Program
    {
        static Random random;
        const int killSeconds = 60 * 30;
        const int maxWarnings = 10;
        const int maxStderrBytes = 1000;
        const double unseedRatio = 0.05;
        const double buggifyOnRatio = 0.8;
        static string BINARY = "fdbserver" + (IsRunningOnMono() ? "" : ".exe");
        static string PLUGIN = "FDBLibTLS." + (IsRunningOnMono() ? "so" : "dll");
        static string OS_NAME = IsRunningOnMono() ? "linux" : "win";

        static int Main(string[] args)
        {
            bool traceToStdout = false;
            try
            {
                string joshuaSeed = System.Environment.GetEnvironmentVariable("JOSHUA_SEED");
                byte[] seed = new byte[4];
                new System.Security.Cryptography.RNGCryptoServiceProvider().GetBytes(seed);
                random = new Random(joshuaSeed != null ? Convert.ToInt32(Int64.Parse(joshuaSeed) % 2147483648) : new BinaryReader(new MemoryStream(seed)).ReadInt32());

                if (args.Length < 1)
                    return UsageMessage();

                if (args[0] == "summarize")
                {
                    if (args.Length < 3)
                        return UsageMessage();
                    string valgrindFileName = args.Length >= 4 ? args[3] : null;
                    string externalError = args.Length >= 5 ? args[4] : "";
                    traceToStdout = args.Length == 6 && args[5] == "true";

                    int unseed;
                    bool retryableError;
                    //SOMEDAY: This only works if a run generated just one trace file. We should change the summarize command to take multiple trace files
                    return Summarize(new string[]{ args[1] }, args[2], null, null, null, null, null, null, valgrindFileName, -1, out unseed, out retryableError, true,
                                     traceToStdout: traceToStdout, externalError: externalError);
                }
                else if (args[0] == "remote")
                {
                    if (args.Length < 6)
                        return UsageMessage();

                    return Remote(args[1], args[2], double.Parse(args[3]), int.Parse(args[4]), args[5], args.Length == 7 ? args[6] : "default");
                }
                else if (args[0] == "run")
                {
                    try
                    {
                        if (args.Length < 6)
                            return UsageMessage();

                        string runDir = null;
                        if(args[1] != "temp") runDir = args[1];

                        bool useValgind = args.Length > 6 && args[6].ToLower() == "true";
                        int maxTries = (args.Length > 7) ? int.Parse(args[7]) : 3;
                        return Run(args[2], args[3], args[4], args[5], null, runDir, null, useValgind, maxTries);
                    }
                    catch(Exception e)
                    {
                        var xout = new XElement("TestHarnessError",
                            new XAttribute("Severity", (int)Magnesium.Severity.SevError),
                            new XAttribute("ErrorMessage", e.Message));

                        AppendXmlMessageToSummary(args[5], xout);
                        throw;
                    }
                }
                else if (args[0] == "replay")
                {
                    if (args.Length != 6)
                        return UsageMessage();

                    string runDir = null;
                    if (args[1] != "temp") runDir = args[1];

                    return Replay(args[2], args[3], args[4], args[5], runDir);
                }
                else if (args[0] == "auto")
                {
                    if (args.Length < 4)
                        return UsageMessage();

                    string runDir = null;
                    if (args[1] != "temp") runDir = args[1];

                    string cacheDir = Path.Combine(runDir, "test_harness_cache");
                    bool useValgrind = args.Length > 4 && args[4].ToLower() == "true";
                    int maxTries = (args.Length > 5) ? int.Parse(args[5]) : 3;

                    return Auto(args[2], Path.Combine(runDir, "runs"), args[3], cacheDir, useValgrind, maxTries);
                }
                else if (args[0] == "extract-errors")
                {
                    if (args.Length != 3)
                        return UsageMessage();

                    return ExtractErrors(args[1], args[2]);
                }
                else if (args[0] == "joshua-run")
                {
                    traceToStdout = true;

                    try
                    {
                        string oldBinaryFolder = (args.Length > 1)  ? args[1] : Path.Combine("/opt", "joshua", "global_data", "oldBinaries");
                        bool useValgrind = args.Length > 2 && args[2].ToLower() == "true";
                        int maxTries = (args.Length > 3) ? int.Parse(args[3]) : 3;
                        bool buggifyEnabled = (args.Length > 4) ? bool.Parse(args[4]) : true;
                        bool faultInjectionEnabled = (args.Length > 5) ? bool.Parse(args[5]) : true;
                        return Run(Path.Combine("bin", BINARY), "", "tests", "summary.xml", "error.xml", "tmp", oldBinaryFolder, useValgrind, maxTries, true, Path.Combine("/app", "deploy", "runtime", ".tls_5_1", PLUGIN), buggifyEnabled, faultInjectionEnabled);
                    }
                    catch(Exception e)
                    {
                        var xout = new XElement("TestHarnessError",
                            new XAttribute("Severity", (int)Magnesium.Severity.SevError),
                            new XAttribute("ErrorMessage", e.ToString()));

                        AppendXmlMessageToSummary("summary.xml", xout, true);
                        throw;
                    }
                }
                else if (args[0] == "version")
                {
                    return VersionInfo.Show();
                }

                return UsageMessage();
            }
            catch (Exception e)
            {
                if (!traceToStdout)
                {
                    Console.WriteLine("Error:");
                    Console.WriteLine(e.ToString());
                }

                return 100;
            }
        }

        static T Choice<T>(this Random random, IList<T> items)
        {
            return items[ random.Next(items.Count) ];
        }

        static bool IsRunningOnMono()
        {
            return Type.GetType("Mono.Runtime") != null;
        }

        static int Replay(string fdbserverName, string tlsPluginFile, string inputSummaryFileName, string outputSummaryFileName, string runDir)
        {
            using (var summaryFileIn = System.IO.File.Open(inputSummaryFileName,
                            System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite | System.IO.FileShare.Delete))
            {
                foreach (var test in Magnesium.XmlParser.Parse(summaryFileIn, inputSummaryFileName).OfType<Magnesium.TestPlan>())
                {
                    if (test is Magnesium.Test) continue;  // Only process the test plans
                    int unseed;
                    bool retryableError;
                    RunTest(fdbserverName, tlsPluginFile, outputSummaryFileName, null, test.randomSeed, test.Buggify, test.TestFile, runDir, test.TestUID, -1, out unseed, out retryableError, true, false);
                }
            }
            return 0;
        }

        // Ver format - x.x.x (eg, 5.0.1, 4.2.11)
        // Returns true is ver1 is greater than or equal to ver2
        static bool versionGreaterThanOrEqual(string ver1, string ver2)
        {
            string[] tokens1 = ver1.Split('.');
            string[] tokens2 = ver2.Split('.');
            if (tokens1.Length != tokens2.Length || tokens2.Length != 3)
            {
                throw new ArgumentException("Invalid Version Format Version1: " + ver1 + " Version2: " + ver2);
            }
            int[] version1 = Array.ConvertAll(tokens1, int.Parse);
            int[] version2 = Array.ConvertAll(tokens2, int.Parse);
            return ((System.Collections.IStructuralComparable)version1).CompareTo(version2, System.Collections.Generic.Comparer<int>.Default) >= 0;
        }

        static bool versionLessThan(string ver1, string ver2) {
            return !versionGreaterThanOrEqual(ver1, ver2);
        }

        static string getFdbserverVersion(string fdbserverName) {
           using (var process = new System.Diagnostics.Process())
           {
              process.StartInfo.UseShellExecute = false;
              process.StartInfo.RedirectStandardOutput = true;
              process.StartInfo.FileName = fdbserverName;
              process.StartInfo.Arguments = "--version";
              process.StartInfo.RedirectStandardError = true;

              process.Start();
              var output = process.StandardOutput.ReadToEnd();
              // If the process finished successfully, we call the parameterless WaitForExit to ensure that output buffers get flushed
              process.WaitForExit();

              var match = Regex.Match(output, @"v(\d+\.\d+\.\d+)");
              if (match.Groups.Count < 1) return "";
              return match.Groups[1].Value;
            }
        }

        static int Run(string fdbserverName, string tlsPluginFile, string testFolder, string summaryFileName, string errorFileName, string runDir, string oldBinaryFolder, bool useValgrind, int maxTries, bool traceToStdout = false, string tlsPluginFile_5_1 = "", bool buggifyEnabled = true, bool faultInjectionEnabled = true)
        {
            int seed = random.Next(1000000000);
            bool buggify = buggifyEnabled ? (random.NextDouble() < buggifyOnRatio) : false;
            string testFile = null;
            string testDir = "";
            string oldServerName = "";
            bool noSim = false;

            if (Directory.Exists(testFolder))
            {
                int poolSize = 0;
                if( Directory.Exists(Path.Combine(testFolder, "rare")) ) poolSize += 1;
                if( Directory.Exists(Path.Combine(testFolder, "slow")) ) poolSize += 5;
                if( Directory.Exists(Path.Combine(testFolder, "fast")) ) poolSize += 14;
                if( Directory.Exists(Path.Combine(testFolder, "restarting")) ) poolSize += 1;
                if( Directory.Exists(Path.Combine(testFolder, "noSim")) ) poolSize += 1;

                if( poolSize == 0 ) {
                    Console.WriteLine("Passed folder ({0}) did not have a fast, slow, rare, restarting, or noSim sub-folder", testFolder);
                    return 1;
                }
                int selection = random.Next(poolSize);
                int selectionWindow = 0;

                if( Directory.Exists(Path.Combine(testFolder, "rare")) ) selectionWindow += 1;
                if (selection < selectionWindow)
                    testDir = Path.Combine(testFolder, "rare");
                else
                {
                    if (Directory.Exists(Path.Combine(testFolder, "restarting"))) selectionWindow += 1;
                    if (selection < selectionWindow)
                        testDir = Path.Combine(testFolder, "restarting");
                    else
                    {
                        if (Directory.Exists(Path.Combine(testFolder, "noSim"))) selectionWindow += 1;
                        if (selection < selectionWindow)
                        {
                            testDir = Path.Combine(testFolder, "noSim");
                            noSim = true;
                        }
                        else
                        {
                            if (Directory.Exists(Path.Combine(testFolder, "slow"))) selectionWindow += 5;
                            if (selection < selectionWindow)
                                testDir = Path.Combine(testFolder, "slow");
                            else
                                testDir = Path.Combine(testFolder, "fast");
                        }
                    }
                }
                string[] files = Directory.GetFiles(testDir, "*", SearchOption.AllDirectories);
                string[] uniqueFiles;
                if (testDir.EndsWith("restarting"))
                {
                    ISet<string> uniqueFileSet = new HashSet<String>();
                    foreach(string file in files) {
                        uniqueFileSet.Add(file.Substring(0, file.LastIndexOf("-"))); // all restarting tests end with -1.txt or -2.txt
                    }
                    uniqueFiles = uniqueFileSet.ToArray();
                    testFile = random.Choice(uniqueFiles);
                    // The on-disk format changed in 4.0.0, and 5.x can't load files from 3.x.
                    string oldBinaryVersionLowerBound = "4.0.0";
                    string lastFolderName = Path.GetFileName(Path.GetDirectoryName(testFile));
                    if (lastFolderName.Contains("from_") || lastFolderName.Contains("to_")) // Only perform upgrade/downgrade tests from certain versions
                    {
                        oldBinaryVersionLowerBound = lastFolderName.Split('_').ElementAt(1); // Assuming "from_*.*.*" appears first in the folder name
                    }
                    string oldBinaryVersionUpperBound = getFdbserverVersion(fdbserverName);
                    if (lastFolderName.Contains("until_")) // Specify upper bound for old binary; "until_*.*.*" is assumed at the end if present
                    {
                        string givenUpperBound = lastFolderName.Split('_').Last();
                        if (versionLessThan(givenUpperBound, oldBinaryVersionUpperBound)) {
                            oldBinaryVersionUpperBound = givenUpperBound;
                        }
                    }
                    if (versionGreaterThanOrEqual("4.0.0", oldBinaryVersionUpperBound)) {
                        // If the binary under test is from 3.x, then allow upgrade tests from 3.x binaries.
                        oldBinaryVersionLowerBound = "0.0.0";
                    }
                    string[] currentBinary = { fdbserverName };
                    IEnumerable<string> oldBinaries = Array.FindAll(
                                                         Directory.GetFiles(oldBinaryFolder),
                                                         x => versionGreaterThanOrEqual(Path.GetFileName(x).Split('-').Last(), oldBinaryVersionLowerBound)
                                                           && versionLessThan(Path.GetFileName(x).Split('-').Last(), oldBinaryVersionUpperBound));
                    if (!lastFolderName.Contains("until_")) {
                        // only add current binary to the list of old binaries if "until_" is not specified in the folder name
                        // <version> in until_<version> should be less or equal to the current binary version
                        // otherwise, using "until_" makes no sense
                        // thus, by definition, if "until_" appears, we do not want to run with the current binary version
                        oldBinaries = oldBinaries.Concat(currentBinary);
                    }
                    List<string> oldBinariesList = oldBinaries.ToList<string>();
                    if (oldBinariesList.Count == 0) {
                        // In theory, restarting tests are named to have at least one old binary version to run
                        // But if none of the provided old binaries fall in the range, we just skip the test
                        Console.WriteLine("No available old binary version from {0} to {1}", oldBinaryVersionLowerBound, oldBinaryVersionUpperBound);
                        return 0;
                    } else {
                        oldServerName = random.Choice(oldBinariesList);
                    }
                }
                else
                {
                    uniqueFiles = Directory.GetFiles(testDir);
                    testFile = random.Choice(uniqueFiles);
                }
            }
            else if (File.Exists(testFolder))
                testFile = testFolder;
            else
            {
                Console.WriteLine("Passed path ({0}) was not a folder or file", testFolder);
                return 1;
            }

            int result = 0;
            bool unseedCheck = !noSim && random.NextDouble() < unseedRatio;
            for (int i = 0; i < maxTries; ++i)
            {
                bool logOnRetryableError = i == maxTries - 1;
                bool retryableError = false;

                if (testDir.EndsWith("restarting"))
                {
                    bool isDowngrade = Path.GetFileName(Path.GetDirectoryName(testFile)).Contains("to_");
                    string firstServerName = isDowngrade ? fdbserverName : oldServerName;
                    string secondServerName = isDowngrade ? oldServerName : fdbserverName;
                    int expectedUnseed = -1;
                    int unseed;
                    string uid = Guid.NewGuid().ToString();
                    bool useNewPlugin = (oldServerName == fdbserverName) || versionGreaterThanOrEqual(oldServerName.Split('-').Last(), "5.2.0");
                    bool useToml = File.Exists(testFile + "-1.toml");
                    string testFile1 = useToml ? testFile + "-1.toml" : testFile + "-1.txt";
                    result = RunTest(firstServerName, useNewPlugin ? tlsPluginFile : tlsPluginFile_5_1, summaryFileName, errorFileName, seed, buggify, testFile1, runDir, uid, expectedUnseed, out unseed, out retryableError, logOnRetryableError, useValgrind, false, true, oldServerName, traceToStdout, noSim, faultInjectionEnabled);
                    if (result == 0)
                    {
                        string testFile2 = useToml ? testFile + "-2.toml" : testFile + "-2.txt";
                        result = RunTest(secondServerName, tlsPluginFile, summaryFileName, errorFileName, seed+1, buggify, testFile2, runDir, uid, expectedUnseed, out unseed, out retryableError, logOnRetryableError, useValgrind, true, false, oldServerName, traceToStdout, noSim, faultInjectionEnabled);
                    }
                }
                else
                {
                    int expectedUnseed = -1;
                    if (!useValgrind && unseedCheck)
                    {
                        result = RunTest(fdbserverName, tlsPluginFile, null, null, seed, buggify, testFile, runDir, Guid.NewGuid().ToString(), -1, out expectedUnseed, out retryableError, logOnRetryableError, false, false, false, "", traceToStdout, noSim, faultInjectionEnabled);
                    }

                    if (!retryableError)
                    {
                        int unseed;
                        result = RunTest(fdbserverName, tlsPluginFile, summaryFileName, errorFileName, seed, buggify, testFile, runDir, Guid.NewGuid().ToString(), expectedUnseed, out unseed, out retryableError, logOnRetryableError, useValgrind, false, false, "", traceToStdout, noSim, faultInjectionEnabled);
                    }
                }

                if (!retryableError)
                {
                    return result;
                }
            }

            return result;
        }

        private static int RunTest(string fdbserverName, string tlsPluginFile, string summaryFileName, string errorFileName, int seed,
            bool buggify, string testFile, string runDir, string uid, int expectedUnseed, out int unseed, out bool retryableError, bool logOnRetryableError, bool useValgrind, bool restarting = false,
            bool willRestart = false, string oldBinaryName = "", bool traceToStdout = false, bool noSim = false, bool faultInjectionEnabled = true)
        {
            unseed = -1;

            retryableError = false;
            string tempPath = Path.Combine(runDir != null ? Path.GetFullPath(runDir) : Path.GetTempPath(), uid);
            string oldDir = Directory.GetCurrentDirectory();

            try
            {
                fdbserverName = Path.GetFullPath(fdbserverName);
                tlsPluginFile = (tlsPluginFile.Length > 0) ? Path.GetFullPath(tlsPluginFile) : "";
                testFile = Path.GetFullPath(testFile);
                var ok = 0;

                if (summaryFileName != null)
                    summaryFileName = Path.GetFullPath(summaryFileName);

                Directory.CreateDirectory(tempPath);
                Directory.SetCurrentDirectory(tempPath);

                if (!restarting) LogTestPlan(summaryFileName, testFile, seed, buggify, expectedUnseed != -1, uid, faultInjectionEnabled, oldBinaryName);

                string valgrindOutputFile = null;
                using (var process = new System.Diagnostics.Process())
                {
                    ErrorOutputListener errorListener = new ErrorOutputListener();
                    process.StartInfo.UseShellExecute = false;
                    string tlsPluginArg = "";
                    if (tlsPluginFile.Length > 0) {
                        process.StartInfo.EnvironmentVariables["FDB_TLS_PLUGIN"] = tlsPluginFile;
                        // Use the old-style option with underscores because old binaries do not support hyphens
                        tlsPluginArg = "--tls_plugin=" + tlsPluginFile;
                    }
                    process.StartInfo.RedirectStandardOutput = true;
                    string role = (noSim) ? "test" : "simulation";
                    var args = "";
                    string faultInjectionArg = string.IsNullOrEmpty(oldBinaryName) ? string.Format("-fi {0}", faultInjectionEnabled ? "on" : "off") : "";
                    if (willRestart && oldBinaryName.EndsWith("alpha6"))
                    {
                        args = string.Format("-Rs 1000000000 -r {0} {1} -s {2} -f \"{3}\" -b {4} {5} {6} --crash",
                            role, IsRunningOnMono() ? "" : "-q", seed, testFile, buggify ? "on" : "off", faultInjectionArg, tlsPluginArg);
                    }
                    else
                    {
                        args = string.Format("-Rs 1GB -r {0} {1} -s {2} -f \"{3}\" -b {4} {5} {6} --crash",
                            role, IsRunningOnMono() ? "" : "-q", seed, testFile, buggify ? "on" : "off", faultInjectionArg, tlsPluginArg);
                    }
                    if (restarting) args = args + " --restarting";
                    if (useValgrind && !willRestart)
                    {
                        valgrindOutputFile = string.Format("valgrind-{0}.xml", seed);
                        process.StartInfo.FileName = "valgrind";
                        // Add extra debug directory, if environment variables is defined
                        string valgrindDbgDir = System.Environment.GetEnvironmentVariable("FDB_VALGRIND_DBGPATH");
                        process.StartInfo.Arguments = (string.IsNullOrEmpty(valgrindDbgDir)) ? "" : string.Format("--extra-debuginfo-path={0} ", valgrindDbgDir);

                        process.StartInfo.Arguments +=
                            string.Format("--xml=yes --xml-file={0} -q {1} {2}", valgrindOutputFile, fdbserverName, args);
                    }
                    else
                    {
                        process.StartInfo.FileName = fdbserverName;
                        process.StartInfo.Arguments = args;
                    }
                    process.StartInfo.RedirectStandardError = true;
                    process.ErrorDataReceived += new DataReceivedEventHandler(errorListener.handleData);

                    if (!traceToStdout)
                    {
                        Console.WriteLine("Executing {0} {1} in {2} (buggify: {3}, valgrind: {4})",
                        process.StartInfo.FileName, process.StartInfo.Arguments, tempPath,
                        buggify ? "on" : "off",
                        useValgrind ? "on" : "off");
                    }

                    process.Start();

                    // SOMEDAY: Do we want to actually do anything with standard output or error?
                    // Using standarderror requires async read, see http://msdn.microsoft.com/en-us/library/system.diagnostics.process.standardoutput.aspx
                    //process.StandardOutput.ReadToEnd();
                    OutputCopier copier = new OutputCopier(process.StandardOutput);
                    Thread consoleThread = new Thread(new ThreadStart(copier.copyOutput));
                    consoleThread.Start();

                    MemoryChecker memChecker = new MemoryChecker(process);
                    Thread memCheckThread = new Thread(new ThreadStart(memChecker.monitorMemory));
                    memCheckThread.Start();

                    process.BeginErrorReadLine();

                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();

                    var ms = 1000 * killSeconds;
                    if (useValgrind)
                        ms *= 20;
                    bool killed = !process.WaitForExit(ms);  // Wait "killSeconds" seconds here

                    stopwatch.Stop();

                    if (!killed)
                        process.WaitForExit(); // If the process finished successfully, we call the parameterless WaitForExit to ensure that output buffers get flushed
                    else
                    {
                        // It seems that WaitForExit sometimes returns false well before the timeout has elapsed
                        // We won't report that as an error, but just in case the process didn't actually finish we attempt to
                        // kill it here
                        if (killed && ms - stopwatch.ElapsedMilliseconds > 10000)
                            killed = false;

                        if (!traceToStdout)
                        {
                            Console.WriteLine("Warning: Killing process after {0} seconds...", killSeconds);
                        }

                        try
                        {
                            process.Kill();
                        }
                        catch (Exception e)
                        {
                            if (!traceToStdout)
                            {
                                Console.WriteLine("Warning: Process.Kill returned {0}", e);
                            }
                        }

                        int waitSeconds = 60 * 2;
                        process.WaitForExit(1000 * waitSeconds);
                        if (!process.HasExited)
                        {
                            if(!traceToStdout)
                            {
                                Console.WriteLine("Warning: Unable to kill process after {0} seconds...", waitSeconds);
                            }

                            var xout = new XElement("UnableToKillProcess",
                                new XAttribute("Severity", (int)Magnesium.Severity.SevWarnAlways));

                            AppendXmlMessageToSummary(summaryFileName, xout, traceToStdout, testFile, seed, buggify, expectedUnseed != -1, oldBinaryName, faultInjectionEnabled);
                            return 104;
                        }
                    }
                    if (!traceToStdout)
                    {
                        Console.WriteLine("Exit code is {0}", process.ExitCode);
                    }
                    memCheckThread.Join();
                    consoleThread.Join();

                    var traceFiles = Directory.GetFiles(tempPath, "trace*.*").Where(s => s.EndsWith(".xml") || s.EndsWith(".json")).ToArray();
                    // if no traces caused by the process failed then the result will include its stderr
                    if (process.ExitCode == 0 && traceFiles.Length == 0)
                    {
                        if (!traceToStdout)
                        {
                            Console.WriteLine("Warning: No trace file was generated, summary will not be affected.");
                        }

                        var xout = new XElement((useValgrind ? "NoTraceFileGeneratedLibPos" : "NoTraceFileGenerated"),
                            new XAttribute("Severity", (int)Magnesium.Severity.SevWarnAlways),
                            new XAttribute("Plugin", tlsPluginFile),
                            new XAttribute("MachineName", System.Environment.MachineName));

                        AppendXmlMessageToSummary(summaryFileName, xout, traceToStdout, testFile, seed, buggify, expectedUnseed != -1, oldBinaryName, faultInjectionEnabled);
                        ok = useValgrind ? 0 : 103;
                    }
                    else
                    {
                        var result = Summarize(traceFiles, summaryFileName, errorFileName, killed, errorListener.Errors,
                            process.ExitCode, memChecker.MaxMem, uid,
                            valgrindOutputFile == null ? null : Path.Combine(tempPath, valgrindOutputFile),
                            expectedUnseed, out unseed, out retryableError, logOnRetryableError, willRestart, restarting, oldBinaryName, traceToStdout);

                        if (result != 0)
                        {
                            ok = result;
                        }
                    }

                    if (willRestart) foreach (var f in traceFiles) File.Delete(f);

                    process.Close();
                }
                if (!traceToStdout)
                {
                    Console.WriteLine("Done (unseed {0})", unseed);
                }

                return ok;
            }
            catch (Exception e)
            {
                if (!traceToStdout)
                {
                    Console.WriteLine("Error in Run:");
                    Console.WriteLine(e);
                }

                var xout = new XElement("TestHarnessRunError",
                    new XAttribute("Severity", (int)Magnesium.Severity.SevError),
                    new XAttribute("ErrorMessage", e.Message));

                AppendXmlMessageToSummary(summaryFileName, xout, traceToStdout, testFile, seed, buggify, expectedUnseed != -1, oldBinaryName, faultInjectionEnabled);
                return 101;
            }
            finally
            {
                Directory.SetCurrentDirectory(oldDir);
                if (!willRestart) Directory.Delete(tempPath, true);
            }
        }

        class OutputCopier
        {
            private StreamReader reader;

            public OutputCopier(StreamReader streamReader)
            {
                this.reader = streamReader;
            }

            public void copyOutput()
            {
                //Console.WriteLine("  Beginning console copy...");
                while (!reader.EndOfStream)
                {
                    reader.ReadLine();
                    //Console.WriteLine("  : " + s);
                }
            }
        }

        private class ErrorOutputListener
        {
            public List<string> Errors { get; set; }
            int maxErrorLength;
            int maxErrors;
            bool errorsExceeded;

            public ErrorOutputListener(int maxErrorLength = 1000, int maxErrors = 10)
            {
                Errors = new List<string>();
                this.maxErrorLength = maxErrorLength;
                this.maxErrors = maxErrors;
                this.errorsExceeded = false;
            }

            public bool hasError = false;
            public void handleData(object sendingProcess, DataReceivedEventArgs errLine)
            {
                if(!String.IsNullOrEmpty(errLine.Data))
                {
                    if (errLine.Data.EndsWith("WARNING: ASan doesn't fully support makecontext/swapcontext functions and may produce false positives in some cases!")) {
                        // When running ASAN we expect to see this message. Boost coroutine should be using the correct asan annotations so that it shouldn't produce any false positives.
                        return;
                    }
                    if (errLine.Data.EndsWith("Warning: unimplemented fcntl command: 1036")) {
                        // Valgrind produces this warning when F_SET_RW_HINT is used
                        return;
                    }

                    hasError = true;
                    if(Errors.Count < maxErrors) {
                        if(errLine.Data.Length > maxErrorLength) {
                            Errors.Add(errLine.Data.Substring(0, maxErrorLength) + "...");
                        }
                        else {
                            Errors.Add(errLine.Data);
                        }
                    }
                    else if(!errorsExceeded) {
                        Errors.Add("TestHarness error limit exceeded");
                        errorsExceeded = true;
                    }
                }
            }
        }

        class MemoryChecker
        {
            //private System.Diagnostics.Process process;
            private long maxMem;
            private Process process;

            public long MaxMem
            {
                get { return maxMem; }
                set { maxMem = value; }
            }

            public MemoryChecker(System.Diagnostics.Process process)
            {
                this.process = process;
                this.maxMem = 0;
            }

            public void monitorMemory()
            {
                while (true)
                {
                    try
                    {
                        process.Refresh();
                        if (process.HasExited)
                            return;
                        long mem = process.PrivateMemorySize64;
                        MaxMem = Math.Max(MaxMem, mem);
                        //Console.WriteLine(string.Format("Process used {0} bytes", MaxMem));
                        Thread.Sleep(1000);
                    }
                    catch
                    {
                        return;
                    }
                }
            }
        }

        static void LogTestPlan(string summaryFileName, string testFileName, int randomSeed, bool buggify, bool testDeterminism, string uid, bool faultInjectionEnabled, string oldBinary="")
        {
            var xout = new XElement("TestPlan",
                new XAttribute("TestUID", uid),
                new XAttribute("RandomSeed", randomSeed),
                new XAttribute("TestFile", testFileName),
                new XAttribute("BuggifyEnabled", buggify ? "1" : "0"),
                new XAttribute("FaultInjectionEnabled", faultInjectionEnabled ? "1" : "0"),
                new XAttribute("DeterminismCheck", testDeterminism ? "1" : "0"),
                new XAttribute("OldBinary", Path.GetFileName(oldBinary)));
            AppendToSummary(summaryFileName, xout);
        }

        // Parses the valgrind XML file and returns a list of "what" tags for each error.
        //  All errors for which the "kind" tag starts with "Leak" are ignored
        static string[] ParseValgrindOutput(string valgrindOutputFileName, bool traceToStdout)
        {
            if (!traceToStdout)
            {
                Console.WriteLine("Reading vXML file: " + valgrindOutputFileName);
            }

            ISet<string> whats = new HashSet<string>();
            XElement xdoc = XDocument.Load(valgrindOutputFileName).Element("valgrindoutput");
            foreach(var elem in xdoc.Elements()) {
                if (elem.Name != "error")
                    continue;
                string kind = elem.Element("kind").Value;
                if(kind.StartsWith("Leak"))
                    continue;
                whats.Add(elem.Element("what").Value);
            }
            return whats.ToArray();
        }

        delegate IEnumerable<Magnesium.Event> parseDelegate(System.IO.Stream stream, string file,
            bool keepOriginalElement = false, double startTime = -1, double endTime = Double.MaxValue,
            double samplingFactor = 1.0, Action<string> nonFatalErrorMessage = null);

        static int Summarize(string[] traceFiles, string summaryFileName,
            string errorFileName, bool? killed, List<string> outputErrors, int? exitCode, long? peakMemory,
            string uid, string valgrindOutputFileName, int expectedUnseed, out int unseed, out bool retryableError, bool logOnRetryableError,
            bool willRestart = false, bool restarted = false, string oldBinaryName = "", bool traceToStdout = false, string externalError = "")
        {
            unseed = -1;
            retryableError = false;
            List<string> errorList = new List<string>();
            var xout = new XElement("Test");
            if (uid != null)
                xout.Add(new XAttribute("TestUID", uid));
            bool ok = false;
            string testFile = "Unknown";
            int testsPassed = 0, testCount = -1, warnings = 0, errors = 0;
            bool testBeginFound = false, testEndFound = false, error = false;
            string firstRetryableError = "";
            int stderrSeverity = (int)Magnesium.Severity.SevError;

            Dictionary<KeyValuePair<string, Magnesium.Severity>, Magnesium.Severity> severityMap = new Dictionary<KeyValuePair<string, Magnesium.Severity>, Magnesium.Severity>();
            Dictionary<Tuple<string, string>, bool> codeCoverage = new Dictionary<Tuple<string, string>, bool>();

            foreach (var traceFileName in traceFiles)
            {
                if(!traceToStdout) {
                    Console.WriteLine("Summarizing {0}", traceFileName);
                }
                using (var traceFile = System.IO.File.Open(traceFileName,
                                System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite | System.IO.FileShare.Delete))
                {
                    try
                    {
                        // Use Action to set this because IEnumerables with yield can't have an out variable
                        string nonFatalParseError = null;
                        parseDelegate parse;
                        if (traceFileName.EndsWith(".json"))
                            parse = Magnesium.JsonParser.Parse;
                        else
                            parse = Magnesium.XmlParser.Parse;
                        foreach (var ev in parse(traceFile, traceFileName, nonFatalErrorMessage: (x) => { nonFatalParseError = x; }))
                        {
                            Magnesium.Severity newSeverity;
                            if (severityMap.TryGetValue(new KeyValuePair<string, Magnesium.Severity>(ev.Type, ev.Severity), out newSeverity))
                                ev.Severity = newSeverity;
                            if (ev.Severity >= Magnesium.Severity.SevWarnAlways && ev.DDetails.ContainsKey("ErrorIsInjectedFault"))
                                ev.Severity = Magnesium.Severity.SevWarn;

                            if (ev.Type == "ProgramStart" && !testBeginFound
                                // Just in case the first ProgramStart seen is a Simulated one, ignore it
                                && (!ev.DDetails.ContainsKey("Simulated") || ev.Details.Simulated != "1"))
                            {
                                xout.Add(
                                    new XAttribute("RandomSeed", ev.Details.RandomSeed),
                                    new XAttribute("SourceVersion", ev.Details.SourceVersion),
                                    new XAttribute("Time", ev.Details.ActualTime),
                                    new XAttribute("BuggifyEnabled", ev.Details.BuggifyEnabled),
                                    new XAttribute("DeterminismCheck", expectedUnseed != -1 ? "1" : "0"),
                                    new XAttribute("OldBinary", Path.GetFileName(oldBinaryName)));
                                testBeginFound = true;
                                if (ev.DDetails.ContainsKey("FaultInjectionEnabled"))
                                    xout.Add(new XAttribute("FaultInjectionEnabled", ev.Details.FaultInjectionEnabled));
                            }
                            if (ev.Type == "Simulation" || ev.Type == "NonSimulationTest")
                            {
                                xout.Add(
                                    new XAttribute("TestFile", ev.Details.TestFile));
                                testFile = ev.Details.TestFile.Substring(ev.Details.TestFile.IndexOf("tests"));
                            }
                            if (ev.Type == "ElapsedTime" && !testEndFound)
                            {
                                testEndFound = true;
                                unseed = int.Parse(ev.Details.RandomUnseed);
                                if (expectedUnseed != -1 && expectedUnseed != unseed)
                                {
                                    Magnesium.Severity severity;
                                    if (!severityMap.TryGetValue(new KeyValuePair<string, Magnesium.Severity>("UnseedMismatch", Magnesium.Severity.SevError), out severity))
                                        severity = Magnesium.Severity.SevError;

                                    if (severity >= Magnesium.Severity.SevWarnAlways)
                                    {
                                        xout.Add(new XElement("UnseedMismatch",
                                            new XAttribute("Unseed", unseed),
                                            new XAttribute("ExpectedUnseed", expectedUnseed),
                                            new XAttribute("Severity", (int)severity)));
                                        if ( severity == Magnesium.Severity.SevError ) {
                                            error = true;
                                            errorList.Add("UnseedMismatch");
                                        }
                                    }
                                }
                                xout.Add(
                                    new XAttribute("SimElapsedTime", ev.Details.SimTime),
                                    new XAttribute("RealElapsedTime", ev.Details.RealTime),
                                    new XAttribute("RandomUnseed", ev.Details.RandomUnseed));
                            }
                            if (ev.Severity == Magnesium.Severity.SevWarnAlways)
                            {
                                if (warnings < maxWarnings)
                                {
                                    xout.Add(new XElement(ev.Type,
                                        new XAttribute("Severity", (int)ev.Severity),
                                        ev.DDetails
                                        //.Where(kv => true)
                                            .Select(kv => new XAttribute(kv.Key, kv.Value))));
                                }
                                warnings++;
                            }
                            if (ev.Severity >= Magnesium.Severity.SevError)
                            {
                                string errorString = ev.FormatTestError(true);
                                if (errorString.Contains("platform_error"))
                                {
                                    if (!retryableError)
                                    {
                                        firstRetryableError = errorString;
                                    }
                                    retryableError = true;
                                }
                                if (errors < maxWarnings)
                                {
                                    xout.Add(new XElement(ev.Type,
                                        new XAttribute("Severity", (int)ev.Severity),
                                        ev.DDetails
                                        //.Where(kv => true)
                                            .Select(kv => new XAttribute(kv.Key, kv.Value))));
                                    errorList.Add(errorString);
                                }
                                errors++;
                                error = true;
                            }
                            if (ev.Type == "CodeCoverage" && !willRestart)
                            {
                                bool covered = true;
                                if(ev.DDetails.ContainsKey("Covered"))
                                {
                                    covered = int.Parse(ev.Details.Covered) != 0;
                                }

                                var key = new Tuple<string, string>(ev.Details.File, ev.Details.Line);
                                if (covered || !codeCoverage.ContainsKey(key))
                                {
                                    codeCoverage[key] = covered;
                                }
                            }
                            if (ev.Type == "FaultInjected" || (ev.Type == "BuggifySection" && ev.Details.Activated == "1"))
                            {
                                xout.Add(new XElement(ev.Type, new XAttribute("File", ev.Details.File), new XAttribute("Line", ev.Details.Line)));
                            }
                            if (ev.Type == "TestsExpectedToPass")
                                testCount = int.Parse(ev.Details.Count);
                            if (ev.Type == "TestResults" && ev.Details.Passed == "1")
                                testsPassed++;
                            if (ev.Type == "RemapEventSeverity")
                                severityMap[new KeyValuePair<string, Magnesium.Severity>(ev.Details.TargetEvent, (Magnesium.Severity)int.Parse(ev.Details.OriginalSeverity))] = (Magnesium.Severity)int.Parse(ev.Details.NewSeverity);
                            if (ev.Type == "StderrSeverity")
                                stderrSeverity = int.Parse(ev.Details.NewSeverity);
                        }
                        if (nonFatalParseError != null) {
                            xout.Add(new XElement("NonFatalParseError",
                                new XAttribute("Severity", (int)Magnesium.Severity.SevWarnAlways),
                                new XAttribute("ErrorMessage", nonFatalParseError)));
                        }

                    }
                    catch (Exception e)
                    {
                        if (!traceToStdout)
                        {
                            Console.WriteLine("Error summarizing {0}: {1}", traceFileName, e);
                        }

                        error = true;
                        xout.Add(new XElement("SummarizationError",
                           new XAttribute("Severity", (int)Magnesium.Severity.SevError),
                           new XAttribute("ErrorMessage", e.Message)));
                        errorList.Add("SummarizationError " + e.Message);
                        break;
                    }
                }
            }

            if (externalError.Length > 0) {
                xout.Add(new XElement(externalError, new XAttribute("Severity", (int)Magnesium.Severity.SevError)));
            }

            foreach(var kv in codeCoverage)
            {
                var element = new XElement("CodeCoverage", new XAttribute("File", kv.Key.Item1), new XAttribute("Line", kv.Key.Item2));
                if(!kv.Value)
                {
                    element.Add(new XAttribute("Covered", "0"));
                }

                xout.Add(element);
            }

            if (warnings > maxWarnings)
            {
                //error = true;
                xout.Add(new XElement("WarningLimitExceeded",
                    new XAttribute("Severity", (int)Magnesium.Severity.SevWarnAlways),
                    new XAttribute("WarningCount", warnings)));
            }
            if (errors > maxWarnings)
            {
                error = true;
                xout.Add(new XElement("ErrorLimitExceeded",
                    new XAttribute("Severity", (int)Magnesium.Severity.SevError),
                    new XAttribute("ErrorCount", errors)));
                errorList.Add("ErrorLimitExceeded");
            }
            if (killed == true)
            {
                if (!retryableError)
                {
                    firstRetryableError = "ExternalTimeout";
                }

                retryableError = true;
                error = true;
                xout.Add(new XElement("ExternalTimeout", new XAttribute("Severity", (int)Magnesium.Severity.SevError)));
            }
            if (outputErrors != null)
            {
                int stderrBytes = 0;
                foreach (string err in outputErrors)
                {
                    if (stderrSeverity == (int)Magnesium.Severity.SevError)
                    {
                        error = true;
                    }

                    int remainingBytes = maxStderrBytes - stderrBytes;
                    if (remainingBytes > 0)
                    {
                        string outErr = (err.Length > remainingBytes) ? err.Substring(remainingBytes) + "..." : err;

                        xout.Add(new XElement("StdErrOutput",
                            new XAttribute("Severity", stderrSeverity),
                            new XAttribute("Output", outErr)));
                    }

                    stderrBytes += err.Length;
                }

                if (stderrBytes > maxStderrBytes)
                {
                    xout.Add(new XElement("StdErrOutputTruncated",
                        new XAttribute("Severity", stderrSeverity),
                        new XAttribute("BytesRemaining", stderrBytes - maxStderrBytes)));
                }
            }
            if (exitCode.HasValue && exitCode != 0)
            {
                error = true;
                xout.Add(new XElement("ExitCode", new XAttribute("Code", exitCode.Value), new XAttribute("Severity", (int)Magnesium.Severity.SevError)));
                errorList.Add(string.Format("ExitCode 0x{0:x}", exitCode.Value));
            }
            if (!testEndFound && !willRestart)
            {
                // We didn't terminate the test, but it didn't reach the end?
                error = true;
                xout.Add(new XElement("TestUnexpectedlyNotFinished"), new XAttribute("Severity", (int)Magnesium.Severity.SevError));
                errorList.Add("TestUnexpectedlyNotFinished");
            }
            ok = testsPassed == testCount && testsPassed > 0 && !error;
            xout.Add(
                new XAttribute("Passed", testsPassed),
                new XAttribute("Failed", testCount - testsPassed));
            if (peakMemory.HasValue)
                xout.Add(new XAttribute("PeakMemory", peakMemory.Value));

            if (valgrindOutputFileName != null && valgrindOutputFileName.Length > 0)
            {
                try
                {
                    // If there are any errors reported "ok" will be set to false
                    var whats = ParseValgrindOutput(valgrindOutputFileName, traceToStdout);
                    foreach (var what in whats)
                    {
                        xout.Add(new XElement("ValgrindError",
                                new XAttribute("Severity", (int)Magnesium.Severity.SevError),
                                new XAttribute("What", what)));
                        ok = false;
                        error = true;
                    }
                }
                catch (Exception e)
                {
                    if (!traceToStdout)
                    {
                        Console.WriteLine(e);
                    }

                    error = true;
                    xout.Add(new XElement("ValgrindParseError",
                        new XAttribute("Severity", (int)Magnesium.Severity.SevError),
                        new XAttribute("ErrorMessage", e.Message)));
                    errorList.Add("Failed to parse valgrind output: " + e.Message);
                }
            }

            if (retryableError && !logOnRetryableError)
            {
                xout = new XElement("Test", xout.Attributes());
                xout.Add(new XElement("RetryingError",
                            new XAttribute("Severity", (int)Magnesium.Severity.SevWarnAlways),
                            new XAttribute("What", firstRetryableError)));
            }
            else
            {
                xout.Add(new XAttribute("OK", ok || willRestart));
            }

            AppendToSummary(summaryFileName, xout, traceToStdout);

            if ((!retryableError || logOnRetryableError) && errorFileName != null && (errorList.Count > 0 || !ok) && !willRestart)
            {
                var errorText = string.Join("\n\t", errorList
                    .Concat((!ok && errorList.Count == 0) ? new string[] { "Failed with no explanation" } : new string[] { })
                    .Distinct()
                    .ToArray());
                AppendToFile(errorFileName, string.Format("Test {0} failed with:\n\t{1}\n", testFile, errorText));
            }
            if (!error) {
                return 0;
            }
            else {
                return 102;
            }
        }

        static int ExtractErrors(string summaryFileName, string errorSummaryFileName)
        {
            Console.WriteLine("Extracting from {0}", summaryFileName);
            List<XElement> xout = new List<XElement>();
            var coverage = new Dictionary<Tuple<string,int>,Tuple<int,int>>();
            using (var traceFile = System.IO.File.Open(summaryFileName,
                            System.IO.FileMode.Open, System.IO.FileAccess.Read, System.IO.FileShare.ReadWrite | System.IO.FileShare.Delete))
            {
                try
                {
                    var events = Magnesium.XmlParser.Parse(traceFile, summaryFileName, true);
                    events = Magnesium.TraceLogUtil.IdentifyFailedTestPlans(events);
                    foreach (var ev in events)
                    {
                        Magnesium.Test t = ev as Magnesium.Test;
                        if (t != null)
                        {
                            foreach (var tev in t.events)
                            {
                                if (tev.Type == "CodeCoverage" || tev.Type == "FaultInjected")
                                {
                                    var keyTuple = Tuple.Create(tev.Details.File, int.Parse(tev.Details.Line));
                                    if (coverage.ContainsKey(keyTuple))
                                    {
                                        var old = coverage[keyTuple];
                                        coverage[keyTuple] = Tuple.Create(old.Item1 + 1, old.Item2 + (t.ok ? 0 : 1));
                                    }
                                    else
                                    {
                                        coverage[keyTuple] = Tuple.Create(1, (t.ok ? 0 : 1));
                                    }
                                }
                            }
                            if (!t.ok)
                            {
                                if (t.original != null)
                                {
                                    foreach (var c in t.original.Elements("CodeCoverage"))
                                        c.Remove();
                                    foreach (var f in t.original.Elements("FaultInjected"))
                                        f.Remove();

                                    xout.Add(t.original);
                                }
                                else
                                {
                                    xout.Add(new XElement("Test",
                                        new XAttribute("Type", t.Type),
                                        new XAttribute("Time", t.Time),
                                        new XAttribute("Machine", t.Machine),
                                        new XAttribute("TestUID", t.TestUID),
                                        new XAttribute("TestFile", t.TestFile),
                                        new XAttribute("randomSeed", t.randomSeed),
                                        new XAttribute("Buggify", t.Buggify),
                                        new XAttribute("DeterminismCheck", t.DeterminismCheck),
                                        new XAttribute("OldBinary", t.OldBinary),
                                        new XElement("TestNotSummarized",
                                            new XAttribute("Severity", (int)Magnesium.Severity.SevWarnAlways)
                                            )
                                        )
                                    );
                                }
                            }
                        }
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error summarizing {0}: {1}", summaryFileName, e);
                    xout.Add(new XElement("SummarizationError",
                       new XAttribute("Severity", (int)Magnesium.Severity.SevError),
                       new XAttribute("ErrorMessage", e.Message)));
                    //failedTests.Add("SummarizationError " + e.Message);
                }
            }

            foreach (var e in coverage)
            {
                xout.Add(new XElement("Event",
                    new XAttribute("Type", "CoverageSummary"),
                    new XAttribute("Time", 0),
                    new XAttribute("Machine", ""),
                    new XAttribute("File", e.Key.Item1),
                    new XAttribute("Line", e.Key.Item2),
                    new XAttribute("Covered", e.Value.Item1),
                    new XAttribute("Failed", e.Value.Item2)));
            }

            AppendToErrorSummary(errorSummaryFileName, xout);
            return 0;
        }

        private static void AppendToErrorSummary(string summaryFileName, List<XElement> elements)
        {
            if (summaryFileName == null)
                return;
            takeLock(summaryFileName);
            try {
                foreach (XElement e in elements)
                    AppendToSummary(summaryFileName, e, false, false);
            }
            finally
            {
                releaseLock(summaryFileName);
            }
        }

        private static void AppendToSummary(string summaryFileName, XElement xout, bool traceToStdout = false, bool shouldLock = true)
        {
            bool useXml = true;
            if (summaryFileName != null && summaryFileName.EndsWith(".json")) {
                useXml = false;
            }

            if (traceToStdout)
            {
                if (useXml) {
                    using (var wr = System.Xml.XmlWriter.Create(Console.OpenStandardOutput(), new System.Xml.XmlWriterSettings() { OmitXmlDeclaration = true, Encoding = new System.Text.UTF8Encoding(false) }))
                        xout.WriteTo(wr);
                } else {
                    using (var wr = System.Runtime.Serialization.Json.JsonReaderWriterFactory.CreateJsonWriter(Console.OpenStandardOutput()))
                        xout.WriteTo(wr);
                }
                Console.WriteLine();
                return;
            }

            if (summaryFileName == null)
                return;
            if (shouldLock)
                takeLock(summaryFileName);
            try
            {
                using (var f = System.IO.File.Open(summaryFileName, System.IO.FileMode.Append, System.IO.FileAccess.Write))
                {
                    if (f.Length == 0)
                    {
                        byte[] bytes = Encoding.UTF8.GetBytes("<Trace>");
                        f.Write(bytes, 0, bytes.Length);
                    }
                    if (useXml) {
                        using (var wr = System.Xml.XmlWriter.Create(f, new System.Xml.XmlWriterSettings() { OmitXmlDeclaration = true }))
                            xout.Save(wr);
                    } else {
                        using (var wr = System.Runtime.Serialization.Json.JsonReaderWriterFactory.CreateJsonWriter(f))
                            xout.WriteTo(wr);
                    }
                    var endl = Encoding.UTF8.GetBytes(Environment.NewLine);
                    f.Write(endl, 0, endl.Length);
                }
            }
            finally
            {
                if (shouldLock)
                    releaseLock(summaryFileName);
            }
        }

        private static void AppendXmlMessageToSummary(string summaryFileName, XElement xout, bool traceToStdout = false, string testFile = null,
            int? seed = null, bool? buggify = null, bool? determinismCheck = null, string oldBinaryName = null, bool? faultInjectionEnabled = null)
        {
            var test = new XElement("Test", xout);
            if(testFile != null)
                test.Add(new XAttribute("TestFile", testFile));
            if(seed != null)
                test.Add(new XAttribute("RandomSeed", seed));
            if(buggify != null)
                test.Add(new XAttribute("BuggifyEnabled", buggify.Value ? "1" : "0"));
            if(faultInjectionEnabled != null)
                test.Add(new XAttribute("FaultInjectionEnabled", faultInjectionEnabled.Value ? "1" : "0"));
            if(determinismCheck != null)
                test.Add(new XAttribute("DeterminismCheck", determinismCheck.Value ? "1" : "0"));
            if(oldBinaryName != null)
                test.Add(new XAttribute("OldBinary", Path.GetFileName(oldBinaryName)));

            test.Add(xout);
            AppendToSummary(summaryFileName, test, traceToStdout);
        }

        private static void AppendToFile(string fileName, string content)
        {
            if (fileName == null)
                return;
            takeLock(fileName);
            try
            {
                using (var f = System.IO.File.Open(fileName, System.IO.FileMode.Append, System.IO.FileAccess.Write))
                {
                    var endl = Encoding.UTF8.GetBytes(content);
                    f.Write(endl, 0, endl.Length);
                }
            }
            finally
            {
                releaseLock(fileName);
            }
        }

        static int Remote(string queue, string fdbRoot, double addHours, int testCount, string testTypes, string userScope)
        {
            queue = Path.GetFullPath(queue);
            fdbRoot = Path.GetFullPath(fdbRoot);
            var output = Path.Combine(queue, "archive");
            var now = DateTime.Now;
            string date = string.Format("{0}-{1:00}-{2:00}-{3:00}-{4:00}",
                now.Year, now.Month, now.Day, now.Hour, now.Minute);

            if (!Directory.Exists(queue))
                Directory.CreateDirectory(queue);

            int maxCount = 0;
            foreach (var f in Directory.GetFiles(queue, String.Format("{0}-*.xml", date)))
            {
                var count = Int32.Parse(f.Split('-')[5]);
                maxCount = Math.Max(maxCount, count);
            }
            maxCount++;

            var suffix = String.Format("{0}-{1}-{2}", Environment.UserName, userScope, OS_NAME);
            foreach (var f in Directory.GetFiles(queue, "*" + suffix + ".xml"))
                File.Delete(f);

            string label = String.Format("{0}-{1}-{2}", date, maxCount, suffix);

            var testStaging = Path.Combine(output, label);
            Directory.CreateDirectory(testStaging);
            File.Create(Path.Combine(testStaging, "errors.txt"));

            var release = Path.Combine(fdbRoot, "bin");
            if(!IsRunningOnMono())
                release = Path.Combine(release, "Release");

            File.Copy(Path.Combine(release, BINARY), Path.Combine(testStaging, BINARY));
            File.Copy(Path.Combine(fdbRoot, "tls-plugins", PLUGIN), Path.Combine(testStaging, PLUGIN));

            if (IsRunningOnMono())
                File.Copy(Path.Combine(release, BINARY + ".debug"), Path.Combine(testStaging, BINARY + ".debug"));

            //using (var f = System.IO.File.Open(
            //    Path.Combine(testStaging, "summary.xml"),
            //    System.IO.FileMode.Create, System.IO.FileAccess.ReadWrite, System.IO.FileShare.Delete))
            //{
            //    byte[] bytes = Encoding.UTF8.GetBytes("<Trace>");
            //    f.Write(bytes, 0, bytes.Length);
            //}

            var coverageFiles = Directory.GetFiles(release, "coverage*.xml");
            foreach (var coverage in coverageFiles)
                File.Copy(coverage, Path.Combine(testStaging, Path.GetFileName(coverage)));

            Directory.CreateDirectory(Path.Combine(testStaging, "tests"));

            if (testTypes == "fast" || testTypes == "all")
                CopyAll(new DirectoryInfo(Path.Combine(fdbRoot, "tests", "fast")), new DirectoryInfo(Path.Combine(testStaging, "tests", "fast")));
            if (testTypes == "restarting" || testTypes == "all")
            {
                CopyAll(new DirectoryInfo(Path.Combine(fdbRoot, "tests", "restarting")), new DirectoryInfo(Path.Combine(testStaging, "tests", "restarting")));
                //CopyAll(new DirectoryInfo(Path.Combine(fdbRoot, "tests", "oldBinaries")), new DirectoryInfo(Path.Combine(testStaging, "tests", "oldBinaries")));
            }
            if (testTypes == "all")
            {
                CopyAll(new DirectoryInfo(Path.Combine(fdbRoot, "tests", "slow")), new DirectoryInfo(Path.Combine(testStaging, "tests", "slow")));
                CopyAll(new DirectoryInfo(Path.Combine(fdbRoot, "tests", "rare")), new DirectoryInfo(Path.Combine(testStaging, "tests", "rare")));
            }


            if (testTypes != "fast" && testTypes != "all" && testTypes != "restarting")
            {
                FileInfo file = new FileInfo(testTypes);
                Directory.CreateDirectory(Path.Combine(testStaging, "tests", file.Directory.Name));
                file.CopyTo(Path.Combine(testStaging, "tests", file.Directory.Name, file.Name), true);
            }

            var e =
                new XElement("TestDefinition",
                    new XElement("Duration",
                        new XAttribute("Hours", addHours)),
                    new XElement("TestCount", testCount));
            new XDocument(e).Save(Path.Combine(queue, label + ".xml"));
            File.Copy(Path.Combine(queue, label + ".xml"), Path.Combine(testStaging, label + ".xml"));

            var summaryFile = Path.Combine(testStaging, "summary.xml");
            Console.WriteLine(label);

            if (!IsRunningOnMono())
            {
                using (var mProcess = new System.Diagnostics.Process())
                {
                    mProcess.StartInfo.UseShellExecute = false;
                    mProcess.StartInfo.RedirectStandardOutput = true;
                    mProcess.StartInfo.FileName = "Magnesium.exe";
                    mProcess.StartInfo.Arguments = "Summary " + summaryFile;
                    mProcess.Start();
                }
            }

            return 0;
        }

        public static void CopyAll(DirectoryInfo source, DirectoryInfo target, Func<FileInfo, bool> predicate = null)
        {
            // Check if the target directory exists, if not, create it.
            if (!Directory.Exists(target.FullName))
                Directory.CreateDirectory(target.FullName);

            // Copy each file into it's new directory.
            foreach (FileInfo fi in source.GetFiles())
                if (predicate == null || predicate(fi))
                    fi.CopyTo(Path.Combine(target.ToString(), fi.Name), true);

            // Copy each subdirectory using recursion.
            foreach (DirectoryInfo diSourceSubDir in source.GetDirectories())
                CopyAll(diSourceSubDir, target.CreateSubdirectory(diSourceSubDir.Name), predicate);
        }

        static int Auto(string queueDirectory, string runDir, string shareDir, string cacheDir, bool useValgrind, int maxTries)
        {
            try
            {
                queueDirectory = Path.GetFullPath(queueDirectory);
                while (true)
                {
                    Test test = getTest(queueDirectory, runDir, shareDir, cacheDir);
                    Console.WriteLine("Running test {0}", test.label);

                    // run test
                    test.run(useValgrind, maxTries);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
                return 100;
            }
        }

        class Test
        {
            public DateTime? testEnd;
            public string queueDirectory;
            public string label;
            public string runDir;
            public string inputDir;
            public string outputDir;
            public string oldBinaryDir;
            public int testCount;

            public Test(string queueDirectory, string label, string runDir, string shareDir, string cacheDir)
            {
                this.queueDirectory = queueDirectory;
                this.label = label;
                this.runDir = runDir;
                var specFile = Path.Combine(queueDirectory, label + ".xml");
                var testDef = XDocument.Load(specFile).Element("TestDefinition");
                var testDuration = double.Parse(testDef.Element("Duration").Attribute("Hours").Value);
                var testBegin = File.GetCreationTime(specFile);
                testEnd = testDuration < 0 ? (DateTime?)null : testBegin.AddHours(testDuration);

                testCount = int.Parse(testDef.Element("TestCount").Value);
                outputDir = Path.Combine(queueDirectory, "archive", label);
                Directory.CreateDirectory(outputDir);

                string oldBinarySourceDir = Path.Combine(shareDir, "oldBinaries");

                if (cacheDir != null)
                {
                    inputDir = Path.Combine(cacheDir, "archive", label);
                    Directory.CreateDirectory(Path.Combine(cacheDir, "archive"));

                    if (!Directory.Exists(inputDir))
                    {
                        takeLock(inputDir);
                        if (!Directory.Exists(inputDir))
                        {
                            string tmpDir = Path.Combine(cacheDir, "archive.part", label + "." + Path.GetRandomFileName() + ".part");
                            Directory.CreateDirectory(tmpDir);
                            CopyAll(new DirectoryInfo(outputDir), new DirectoryInfo(tmpDir), (FileInfo file) =>
                                    file.Name != "fdbserver.debug" &&
                                    !file.Name.StartsWith("summary-") &&
                                    file.Name != "errors.txt"
                                );
                            Directory.Move(tmpDir, inputDir);
                        }
                        releaseLock(inputDir);
                    }

                    oldBinaryDir = Path.Combine(cacheDir, "oldBinaries");

                    Directory.CreateDirectory(oldBinaryDir);
                    foreach (FileInfo fi in new DirectoryInfo(oldBinarySourceDir).GetFiles())
                    {
                        var targetName = Path.Combine(oldBinaryDir, fi.Name);
                        if (!File.Exists(targetName) || fi.LastWriteTimeUtc != File.GetLastWriteTimeUtc(targetName))
                        {
                            fi.CopyTo(targetName, true);
                            File.SetLastWriteTimeUtc(targetName, fi.LastWriteTimeUtc);
                        }
                    }

                    foreach (FileInfo fi in new DirectoryInfo(oldBinaryDir).GetFiles())
                    {
                        var targetName = Path.Combine(oldBinarySourceDir, fi.Name);
                        if (!File.Exists(targetName))
                            fi.Delete();
                    }
                }
                else
                {
                    inputDir = outputDir;
                    oldBinaryDir = oldBinarySourceDir;
                }
                //Console.WriteLine("TestEnd {0}, now {1}, duration {2}, done {3}", testEnd, DateTime.Now, testDuration, done());
            }
            public bool done()
            {
                return testEnd.HasValue && System.DateTime.Now > testEnd.Value;
            }
            public void run(bool useValgrind, int maxTries)
            {
                Run(Path.Combine(inputDir, BINARY),
                    Path.Combine(inputDir, PLUGIN),
                    Path.Combine(inputDir, "tests"),
                    Path.Combine(outputDir, "summary-" + Environment.MachineName + ".xml"),
                    Path.Combine(outputDir, "errors.txt"),
                    runDir,
                    oldBinaryDir,
                    useValgrind,
                    maxTries);
            }

            public void finalize()
            {
                try
                {
                    Console.WriteLine("Deleting: {0}", Path.Combine(queueDirectory, label + ".xml"));
                    File.Delete(Path.Combine(queueDirectory, label + ".xml"));
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error deleting queue folder: {0}", e.Message);
                }
            }
        }

        static Test getTest(string parent, string runDir, string shareDir, string cacheDir)
        {
            while (true)
            {
                var testFiles = Directory.GetFiles(parent, String.Format("*{0}.xml", OS_NAME));
                //   (if no tests, wait, try again)
                if (testFiles.Length != 0)
                {
                    /*try
                    {*/
                        var testFile = testFiles[random.Next(testFiles.Length)];
                        var test = new Test(parent, Path.GetFileNameWithoutExtension(testFile), runDir, shareDir, cacheDir);
                        if ((test.testCount < 0 || UpdateTestTotals(Path.Combine(test.outputDir, "testCount"), test.testCount)) && !test.done())
                            return test;
                        else
                            test.finalize();
                    /*}
                    catch (Exception)
                    {
                        // retry opening a test
                    }*/
                }
                System.Threading.Thread.Sleep(1000);
            }
        }

        static void takeLock(string targetFile)
        {
            // Console.WriteLine("Attempting to take lock on {0}", targetFile);
            string lockFile = targetFile + ".lock";
            while (true)
            {
                try
                {
                    using (var f = System.IO.File.Open(lockFile, System.IO.FileMode.CreateNew))
                    {
                        return;
                    }
                }
                catch (System.IO.IOException e)
                {
                    Console.WriteLine("Waiting for file lock: {0}", e.Message);
                    System.Threading.Thread.Sleep(250);
                }
            }
        }

        static void releaseLock(string targetFile)
        {
            File.Delete(targetFile + ".lock");
        }

        private static bool UpdateTestTotals(string countFileName, int desiredTestCount)
        {
            takeLock(countFileName);
            try
            {
                using (var f = System.IO.File.Open(countFileName, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite))
                {
                    int currentCount = 0;
                    byte[] b;

                    if (f.Length != 0)
                    {
                        b = new byte[f.Length];
                        f.Read(b, 0, b.Length);
                        currentCount = int.Parse(Encoding.UTF8.GetString(b));
                    }

                    if (currentCount >= desiredTestCount)
                        return false;

                    f.SetLength(0);
                    b = Encoding.UTF8.GetBytes(String.Format("{0}", currentCount + 1));
                    f.Write(b, 0, b.Length);

                    return true;
                }
            }
            finally
            {
                releaseLock(countFileName);
            }
        }

        private static int UsageMessage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  TestHarness run [temp/runDir] [fdbserver[.exe]] [TLSplugin] [testfolder] [summary.xml] <useValgrind> <maxTries>");
            Console.WriteLine("  TestHarness summarize [trace.xml] [summary.xml] <valgrind-XML-file> <external-error> <traceToStdOut>");
            Console.WriteLine("  TestHarness replay [temp/runDir] [fdbserver[.exe]] [TLSplugin] [summary-in.xml] [summary-out.xml]");
            Console.WriteLine("  TestHarness auto [temp/runDir] [directory] [shareDir] <useValgrind> <maxTries>");
            Console.WriteLine("  TestHarness remote [queue folder] [root foundation folder] [duration in hours] [amount of tests] [all/fast/<test_path>] [scope]");
            Console.WriteLine("  TestHarness extract-errors [summary-file] [error-summary-file]");
            Console.WriteLine("  TestHarness joshua-run <useValgrind> <maxTries>");
            VersionInfo.Show();
            return 1;
        }
    }
}

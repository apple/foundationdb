/*
 * SimpleSwiftTestSuite.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

// ==== ---------------------------------------------------------------------------------------------------------------

protocol SimpleSwiftTestSuite {
    init()

    typealias TestsCases = [TestCase]

    @TestCasesBuilder
    var tests: [TestCase] { get }
}

extension SimpleSwiftTestSuite {
    public static var key: String {
        "\(Self.self)".split(separator: ".").last.map(String.init) ?? ""
    }
}


@resultBuilder
struct TestCasesBuilder {
    static func buildBlock(_ tests: TestCase...) -> [TestCase] {
        return tests
    }
}

struct TestCase {
    var _testSuiteName: String = ""
    let name: String

    let file: String
    let line: UInt

    var block: () async throws -> ()

    init(_ name: String, block: @escaping @Sendable () async throws -> (),
         file: String = #fileID, line: UInt = #line) {
        self.name = name
        self.block = block
        self.file = file
        self.line = line
    }

    /// Run a specific unit-test.
    public func run() async throws {
        try await self.block()
    }
}

final class SimpleSwiftTestRunner {

    struct TestFilter {
        private let matcher: String?

        init(parse arguments: ArraySlice<String>) {
            let optName = "--test-filter"

            if let filterIdx = arguments.firstIndex(of: optName) {
                let filterValueIdx = arguments.index(after: filterIdx)
                if filterValueIdx >= arguments.endIndex {
                    fatalError("No value for `\(optName)` given! Arguments: \(arguments)")
                }
                self.matcher = arguments[filterValueIdx]
            } else {
                self.matcher = nil
            }
        }

        public func matches(suiteName: String, testName: String?) -> Bool {
            guard let matcher = self.matcher else {
                return true // always match if no matcher
            }

            if suiteName.contains(matcher) {
                return true
            } else if let testName, testName.contains(matcher) {
                return true
            } else {
                return false
            }
        }
    }

    func run() async throws {
        let filter = TestFilter(parse: CommandLine.arguments.dropFirst())

        for suite in SimpleSwiftTestSuites {
            let tests = self.allTestsForSuite("\(suite)")

            for (testName, testCase) in tests {
                guard filter.matches(suiteName: "\(suite)", testName: testName) else {
                    print("[swift] [skip] \(suite).'\(testName)' @ \(testCase.file):\(testCase.line) -------------------------------------------".yellow)
                    continue
                }

                print("[swift] [test] \(suite).'\(testName)' @ \(testCase.file):\(testCase.line) -------------------------------------------".yellow)

                do {
                    try await testCase.run()
                    print("[swift] [pass] Finished: \(suite).'\(testName)' ------------------------------------------------------------".green)
                } catch {
                    print("[swift] [fail] Failed \(suite).'\(testName)' ------------------------------------------------------------".red)
                }
            }
        }
    }

    func allTestsForSuite(_ testSuite: String) -> [(String, TestCase)] {
        guard let suiteType = SimpleSwiftTestSuites.first(where: { testSuite == "\($0)" }) else {
            return []
        }

        let suiteInstance = suiteType.init()

        var tests: [(String, TestCase)] = []
        for test in suiteInstance.tests {
            tests.append((test.name, test))
        }
        return tests
    }

    func findTestCase(suite: String, testName: String) -> TestCase? {
        allTestsForSuite(suite)
                .first(where: { $0.0 == testName })?
                .1
    }
}
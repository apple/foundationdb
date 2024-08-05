/*
 * swift_test_streams.swift
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

enum RainbowColor: String {
    case black = "\u{001B}[0;30m"
    case red = "\u{001B}[0;31m"
    case green = "\u{001B}[0;32m"
    case yellow = "\u{001B}[0;33m"
    case blue = "\u{001B}[0;34m"
    case magenta = "\u{001B}[0;35m"
    case cyan = "\u{001B}[0;36m"
    case white = "\u{001B}[0;37m"
    case `default` = "\u{001B}[0;0m"

    func name() -> String {
        switch self {
        case .black: return "Black"
        case .red: return "Red"
        case .green: return "Green"
        case .yellow: return "Yellow"
        case .blue: return "Blue"
        case .magenta: return "Magenta"
        case .cyan: return "Cyan"
        case .white: return "White"
        case .default: return "Default"
        }
    }
}

extension String {
    var black: String {
        self.colored(as: .black)
    }

    var red: String {
        self.colored(as: .red)
    }

    var green: String {
        self.colored(as: .green)
    }

    var yellow: String {
        self.colored(as: .yellow)
    }

    var blue: String {
        self.colored(as: .blue)
    }

    var magenta: String {
        self.colored(as: .magenta)
    }

    var cyan: String {
        self.colored(as: .cyan)
    }

    var white: String {
        self.colored(as: .white)
    }

    var `default`: String {
        self.colored(as: .default)
    }

    func colored(as color: RainbowColor) -> String {
        "\(color.rawValue)\(self)\(RainbowColor.default.rawValue)"
    }
}

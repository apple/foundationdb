/*
 * clock_support.swift
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

import Flow

public struct FlowClock {
    /// A flow point in time used for `FlowClock`.
    public struct Instant: Codable, Sendable {
        internal var _value: Swift.Duration

        internal init(_value: Swift.Duration) {
            self._value = _value
        }
    }

    public init() {
    }

}

extension Clock where Self == FlowClock {
    /// A clock that measures time that always increments but does not stop
    /// incrementing while the system is asleep.
    ///
    ///       try await Task.sleep(until: .now + .seconds(3), clock: .flow)
    ///
    public static var flow: FlowClock {
        return FlowClock()
    }
}


extension FlowClock: Clock {
    /// The current flow instant.
    public var now: FlowClock.Instant {
        FlowClock.now
    }

    /// The minimum non-zero resolution between any two calls to `now`.
    public var minimumResolution: Swift.Duration {
        let seconds = Int64(0)
        var nanoseconds = Int64(0)
        nanoseconds += Duration.milliseconds(100).nanoseconds

        return .seconds(seconds) + .nanoseconds(nanoseconds)
    }

    /// The current flow instant.
    public static var now: FlowClock.Instant {
        var seconds = Int64(0)
        var nanoseconds = Int64(0)

        let nowDouble: Double = flow_gNetwork_now()
        seconds += Duration.seconds(nowDouble).seconds
        nanoseconds = Duration.seconds(nowDouble).nanoseconds

        return FlowClock.Instant(_value: .seconds(seconds) + .nanoseconds(nanoseconds))
    }

    /// Suspend task execution until a given deadline within a tolerance.
    /// If no tolerance is specified then the system may adjust the deadline
    /// to coalesce CPU wake-ups to more efficiently process the wake-ups in
    /// a more power efficient manner.
    ///
    /// If the task is canceled before the time ends, this function throws
    /// `CancellationError`.
    ///
    /// This function doesn't block the underlying thread.
    public func sleep(
            until deadline: Instant,
            tolerance: Swift.Duration? = nil
    ) async throws {
        let (seconds, attoseconds) = deadline._value.components
        _ = seconds // silence warning
        let nanoseconds = attoseconds / 1_000_000_000
        _ = nanoseconds // silence warning

        let duration = FlowClock.now.duration(to: deadline)
        let secondsDouble = Double(duration.seconds)
        let nanosDouble = 0 // TODO: fix this handling of nanos from the deadline
        _ = nanosDouble // silence warning

        try await flow_gNetwork_delay(/*secondsDouble=*/secondsDouble, /*priority=*/TaskPriority.DefaultDelay).value()
    }

    public static func sleep(
        for duration: Duration
    ) async throws {
        try await Task.sleep(until: now + duration, clock: .flow)
    }
}

extension FlowClock.Instant: InstantProtocol {
    public static var now: FlowClock.Instant {
        FlowClock.now
    }

    public func advanced(by duration: Swift.Duration) -> FlowClock.Instant {
        return FlowClock.Instant(_value: _value + duration)
    }

    public func duration(to other: FlowClock.Instant) -> Swift.Duration {
        other._value - _value
    }

    public func hash(into hasher: inout Hasher) {
        hasher.combine(_value)
    }

    public static func ==(
            _ lhs: FlowClock.Instant, _ rhs: FlowClock.Instant
    ) -> Bool {
        return lhs._value == rhs._value
    }

    public static func <(
            _ lhs: FlowClock.Instant, _ rhs: FlowClock.Instant
    ) -> Bool {
        return lhs._value < rhs._value
    }

    @_alwaysEmitIntoClient
    @inlinable
    public static func +(
            _ lhs: FlowClock.Instant, _ rhs: Swift.Duration
    ) -> FlowClock.Instant {
        lhs.advanced(by: rhs)
    }

    @_alwaysEmitIntoClient
    @inlinable
    public static func +=(
            _ lhs: inout FlowClock.Instant, _ rhs: Swift.Duration
    ) {
        lhs = lhs.advanced(by: rhs)
    }

    @_alwaysEmitIntoClient
    @inlinable
    public static func -(
            _ lhs: FlowClock.Instant, _ rhs: Swift.Duration
    ) -> FlowClock.Instant {
        lhs.advanced(by: .zero - rhs)
    }

    @_alwaysEmitIntoClient
    @inlinable
    public static func -=(
            _ lhs: inout FlowClock.Instant, _ rhs: Swift.Duration
    ) {
        lhs = lhs.advanced(by: .zero - rhs)
    }

    @_alwaysEmitIntoClient
    @inlinable
    public static func -(
            _ lhs: FlowClock.Instant, _ rhs: FlowClock.Instant
    ) -> Swift.Duration {
        rhs.duration(to: lhs)
    }
}

// ==== ----------------------------------------------------------------------------------------------------------------

// MARK: Duration conversion support

extension Swift.Duration {
    public typealias Value = Int64

    public var nanoseconds: Value {
        let (seconds, attoseconds) = self.components
        let sNanos = seconds * Value(1_000_000_000)
        let asNanos = attoseconds / Value(1_000_000_000)
        let (totalNanos, overflow) = sNanos.addingReportingOverflow(asNanos)
        return overflow ? .max : totalNanos
    }

    /// The microseconds representation of the `TimeAmount`.
    public var microseconds: Value {
        self.nanoseconds / TimeUnit.microseconds.rawValue
    }

    /// The milliseconds representation of the `TimeAmount`.
    public var milliseconds: Value {
        self.nanoseconds / TimeUnit.milliseconds.rawValue
    }

    /// The seconds representation of the `TimeAmount`.
    public var seconds: Value {
        self.nanoseconds / TimeUnit.seconds.rawValue
    }

    public var isEffectivelyInfinite: Bool {
        self.nanoseconds == .max
    }

    /// Represents number of nanoseconds within given time unit
    public enum TimeUnit: Value {
        case days = 86_400_000_000_000
        case hours = 3_600_000_000_000
        case minutes = 60_000_000_000
        case seconds = 1_000_000_000
        case milliseconds = 1_000_000
        case microseconds = 1000
        case nanoseconds = 1

        public var abbreviated: String {
            switch self {
            case .nanoseconds: return "ns"
            case .microseconds: return "μs"
            case .milliseconds: return "ms"
            case .seconds: return "s"
            case .minutes: return "m"
            case .hours: return "h"
            case .days: return "d"
            }
        }

        public func duration(_ duration: Int) -> Duration {
            switch self {
            case .nanoseconds: return .nanoseconds(Value(duration))
            case .microseconds: return .microseconds(Value(duration))
            case .milliseconds: return .milliseconds(Value(duration))
            case .seconds: return .seconds(Value(duration))
            case .minutes: return .seconds(Value(duration) * 60)
            case .hours: return .seconds(Value(duration) * 60 * 60)
            case .days: return .seconds(Value(duration) * 24 * 60 * 60)
            }
        }
    }
}

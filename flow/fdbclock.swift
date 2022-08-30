
struct FDBClock: _Concurrency.Clock {
    // associatedtype Duration where Self.Duration == Self.Instant.Duration
    struct Duration

    associatedtype Instant : InstantProtocol

    var now: Self.Instant { get }

    var minimumResolution: Self.Duration { get }

    func sleep(until deadline: Self.Instant, tolerance: Self.Instant.Duration?) async throws
}
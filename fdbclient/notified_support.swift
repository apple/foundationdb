import Flow
import flow_swift
import FDBClient

// ==== ---------------------------------------------------------------------------------------------------------------

extension NotifiedVersion {

    /// async version of `whenAtLeast`
    public func atLeast(_ limit: VersionMetricHandle.ValueType) async throws {
        var f: FutureVoid = self.whenAtLeast(limit)
        _ = try await f.waitValue
    }
}

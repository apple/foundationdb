import Flow

import Flow

// ==== ---------------------------------------------------------------------------------------------------------------

extension Flow.TaskPriority {

    /// Values representing TaskPriority in the range that is possible to use as Task priority in Swift Concurrency.
    ///
    /// The values are set back to ``Flow/TaskPriority`` when they are enqueued.
    ///
    /// WARNING: When changing the values here, make sure to change the switch in `swift_priority_to_flow`
    ///
    /// Note: Priority in swift must be in the 0-255 range, so we should map the priorities into this range.
    /// The actual values don't really matter at this point, because they'll be mapped back into the exact values as
    /// has them declared in `flow.h` before enqueueing jobs.
    private enum Repr: UInt8, CaseIterable {
        /// WARNING: When changing the values here, make sure to change the switch in `swift_priority_to_net2`
        case Max =                               255
        case RunLoop =                           200
        case ASIOReactor =                       173
        case RunCycleFunction =                  73
        case FlushTrace =                        72
        case WriteSocket =                       71
        case PollEIO =                           70
        case DiskIOComplete =                    69
        case LoadBalancedEndpoint =              68
        case ReadSocket =                        67
        case AcceptSocket =                      66
        case Handshake =                         65
        case CoordinationReply =                 64
        case Coordination =                      63
        case FailureMonitor =                    62
        case ResolutionMetrics =                 61
        case Worker =                            60
        case ClusterControllerWorker =           59
        case ClusterControllerRecruit =          58
        case ClusterControllerRegister =         57
        case ClusterController =                 56
        case MasterTLogRejoin =                  55
        case ProxyStorageRejoin =                54
        case TLogQueuingMetrics =                53
        case TLogPop =                           52
        case TLogPeekReply =                     51
        case TLogPeek =                          50
        case TLogCommitReply =                   49
        case TLogCommit =                        48
        case ReportLiveCommittedVersion =        47
        case ProxyGetRawCommittedVersion =       46
        case ProxyMasterVersionReply =           45
        case ProxyCommitYield2 =                 44
        case ProxyTLogCommitReply =              43
        case ProxyCommitYield1 =                 42
        case ProxyResolverReply =                41
        case ProxyCommit =                       40
        case ProxyCommitBatcher =                39
        case TLogConfirmRunningReply =           38
        case TLogConfirmRunning =                37
        case ProxyGRVTimer =                     36
        case GetConsistentReadVersion =          35
        case GetLiveCommittedVersionReply =      34
        case GetLiveCommittedVersion =           33
        case GetTLogPrevCommitVersion =          32
        case UpdateRecoveryTransactionVersion =  31
        case DefaultPromiseEndpoint =            30
        case DefaultOnMainThread =               29
        case DefaultDelay =                      28
        case DefaultYield =                      27
        case DiskRead =                          26
        case DefaultEndpoint =                   25 // FIXME: avoid overlaps with Swift's predefined priorities
        case UnknownEndpoint =                   24
        case MoveKeys =                          23
        case DataDistributionLaunch =            22
        case Ratekeeper =                        21
        case DataDistribution =                  20
        case DataDistributionLow =               19
        case DataDistributionVeryLow =           18
        case BlobManager =                       17
        case DiskWrite =                         16
        case UpdateStorage =                     15
        case CompactCache =                      14
        case TLogSpilledPeekReply =              13
        case BlobWorkerReadChangeFeed =          12
        case BlobWorkerUpdateFDB =               11
        case BlobWorkerUpdateStorage =           10
        case FetchKeys =                         9
        case RestoreApplierWriteDB =             8
        case RestoreApplierReceiveMutations =    7
        case RestoreLoaderFinishVersionBatch =   6
        case RestoreLoaderSendMutations =        5
        case RestoreLoaderLoadFiles =            4
        case LowPriorityRead =                   3
        case Low =                               2
        case Min =                               1
        case Zero =                              0
    }

    // NOTE: The values returned MUST fit in UInt8 (i.e. be 0..<256)
    var asSwift: _Concurrency.TaskPriority {
        switch self {
        case .Max:                               return .init(rawValue: Repr.Max.rawValue)
        case .RunLoop:                           return .init(rawValue: Repr.RunLoop.rawValue)
        case .ASIOReactor:                       return .init(rawValue: Repr.ASIOReactor.rawValue)
        case .RunCycleFunction:                  return .init(rawValue: Repr.RunCycleFunction.rawValue)
        case .FlushTrace:                        return .init(rawValue: Repr.FlushTrace.rawValue)
        case .WriteSocket:                       return .init(rawValue: Repr.WriteSocket.rawValue)
        case .PollEIO:                           return .init(rawValue: Repr.PollEIO.rawValue)
        case .DiskIOComplete:                    return .init(rawValue: Repr.DiskIOComplete.rawValue)
        case .LoadBalancedEndpoint:              return .init(rawValue: Repr.LoadBalancedEndpoint.rawValue)
        case .ReadSocket:                        return .init(rawValue: Repr.ReadSocket.rawValue)
        case .AcceptSocket:                      return .init(rawValue: Repr.AcceptSocket.rawValue)
        case .Handshake:                         return .init(rawValue: Repr.Handshake.rawValue)
        case .CoordinationReply:                 return .init(rawValue: Repr.CoordinationReply.rawValue)
        case .Coordination:                      return .init(rawValue: Repr.Coordination.rawValue)
        case .FailureMonitor:                    return .init(rawValue: Repr.FailureMonitor.rawValue)
        case .ResolutionMetrics:                 return .init(rawValue: Repr.ResolutionMetrics.rawValue)
        case .Worker:                            return .init(rawValue: Repr.Worker.rawValue)
        case .ClusterControllerWorker:           return .init(rawValue: Repr.ClusterControllerWorker.rawValue)
        case .ClusterControllerRecruit:          return .init(rawValue: Repr.ClusterControllerRecruit.rawValue)
        case .ClusterControllerRegister:         return .init(rawValue: Repr.ClusterControllerRegister.rawValue)
        case .ClusterController:                 return .init(rawValue: Repr.ClusterController.rawValue)
        case .MasterTLogRejoin:                  return .init(rawValue: Repr.MasterTLogRejoin.rawValue)
        case .ProxyStorageRejoin:                return .init(rawValue: Repr.ProxyStorageRejoin.rawValue)
        case .TLogQueuingMetrics:                return .init(rawValue: Repr.TLogQueuingMetrics.rawValue)
        case .TLogPop:                           return .init(rawValue: Repr.TLogPop.rawValue)
        case .TLogPeekReply:                     return .init(rawValue: Repr.TLogPeekReply.rawValue)
        case .TLogPeek:                          return .init(rawValue: Repr.TLogPeek.rawValue)
        case .TLogCommitReply:                   return .init(rawValue: Repr.TLogCommitReply.rawValue)
        case .TLogCommit:                        return .init(rawValue: Repr.TLogCommit.rawValue)
        case .ReportLiveCommittedVersion:        return .init(rawValue: Repr.ReportLiveCommittedVersion.rawValue)
        case .ProxyGetRawCommittedVersion:       return .init(rawValue: Repr.ProxyGetRawCommittedVersion.rawValue)
        case .ProxyMasterVersionReply:           return .init(rawValue: Repr.ProxyMasterVersionReply.rawValue)
        case .ProxyCommitYield2:                 return .init(rawValue: Repr.ProxyCommitYield2.rawValue)
        case .ProxyTLogCommitReply:              return .init(rawValue: Repr.ProxyTLogCommitReply.rawValue)
        case .ProxyCommitYield1:                 return .init(rawValue: Repr.ProxyCommitYield1.rawValue)
        case .ProxyResolverReply:                return .init(rawValue: Repr.ProxyResolverReply.rawValue)
        case .ProxyCommit:                       return .init(rawValue: Repr.ProxyCommit.rawValue)
        case .ProxyCommitBatcher:                return .init(rawValue: Repr.ProxyCommitBatcher.rawValue)
        case .TLogConfirmRunningReply:           return .init(rawValue: Repr.TLogConfirmRunningReply.rawValue)
        case .TLogConfirmRunning:                return .init(rawValue: Repr.TLogConfirmRunning.rawValue)
        case .ProxyGRVTimer:                     return .init(rawValue: Repr.ProxyGRVTimer.rawValue)
        case .GetConsistentReadVersion:          return .init(rawValue: Repr.GetConsistentReadVersion.rawValue)
        case .GetLiveCommittedVersionReply:      return .init(rawValue: Repr.GetLiveCommittedVersionReply.rawValue)
        case .GetLiveCommittedVersion:           return .init(rawValue: Repr.GetLiveCommittedVersion.rawValue)
        case .GetTLogPrevCommitVersion:          return .init(rawValue: Repr.GetTLogPrevCommitVersion.rawValue)
        case .UpdateRecoveryTransactionVersion:  return .init(rawValue: Repr.UpdateRecoveryTransactionVersion.rawValue)
        case .DefaultPromiseEndpoint:            return .init(rawValue: Repr.DefaultPromiseEndpoint.rawValue)
        case .DefaultOnMainThread:               return .init(rawValue: Repr.DefaultOnMainThread.rawValue)
        case .DefaultDelay:                      return .init(rawValue: Repr.DefaultDelay.rawValue)
        case .DefaultYield:                      return .init(rawValue: Repr.DefaultYield.rawValue)
        case .DiskRead:                          return .init(rawValue: Repr.DiskRead.rawValue)
        case .DefaultEndpoint:                   return .init(rawValue: Repr.DefaultEndpoint.rawValue)
        case .UnknownEndpoint:                   return .init(rawValue: Repr.UnknownEndpoint.rawValue)
        case .MoveKeys:                          return .init(rawValue: Repr.MoveKeys.rawValue)
        case .DataDistributionLaunch:            return .init(rawValue: Repr.DataDistributionLaunch.rawValue)
        case .Ratekeeper:                        return .init(rawValue: Repr.Ratekeeper.rawValue)
        case .DataDistribution:                  return .init(rawValue: Repr.DataDistribution.rawValue)
        case .DataDistributionLow:               return .init(rawValue: Repr.DataDistributionLow.rawValue)
        case .DataDistributionVeryLow:           return .init(rawValue: Repr.DataDistributionVeryLow.rawValue)
        case .BlobManager:                       return .init(rawValue: Repr.BlobManager.rawValue)
        case .DiskWrite:                         return .init(rawValue: Repr.DiskWrite.rawValue)
        case .UpdateStorage:                     return .init(rawValue: Repr.UpdateStorage.rawValue)
        case .CompactCache:                      return .init(rawValue: Repr.CompactCache.rawValue)
        case .TLogSpilledPeekReply:              return .init(rawValue: Repr.TLogSpilledPeekReply.rawValue)
        case .BlobWorkerReadChangeFeed:          return .init(rawValue: Repr.BlobWorkerReadChangeFeed.rawValue)
        case .BlobWorkerUpdateFDB:               return .init(rawValue: Repr.BlobWorkerUpdateFDB.rawValue)
        case .BlobWorkerUpdateStorage:           return .init(rawValue: Repr.BlobWorkerUpdateStorage.rawValue)
        case .FetchKeys:                         return .init(rawValue: Repr.FetchKeys.rawValue)
        case .RestoreApplierWriteDB:             return .init(rawValue: Repr.RestoreApplierWriteDB.rawValue)
        case .RestoreApplierReceiveMutations:    return .init(rawValue: Repr.RestoreApplierReceiveMutations.rawValue)
        case .RestoreLoaderFinishVersionBatch:   return .init(rawValue: Repr.RestoreLoaderFinishVersionBatch.rawValue)
        case .RestoreLoaderSendMutations:        return .init(rawValue: Repr.RestoreLoaderSendMutations.rawValue)
        case .RestoreLoaderLoadFiles:            return .init(rawValue: Repr.RestoreLoaderLoadFiles.rawValue)
        case .LowPriorityRead:                   return .init(rawValue: Repr.LowPriorityRead.rawValue)
        case .Low:                               return .init(rawValue: Repr.Low.rawValue)
        case .Min:                               return .init(rawValue: Repr.Min.rawValue)
        case .Zero:                              return .init(rawValue: Repr.Zero.rawValue)
        @unknown default: fatalError("Unknown Flow.TaskPriority: \(self)")
        }
    }
}

extension _Concurrency.TaskPriority {
    public static var Max: Self { Flow.TaskPriority.Max.asSwift }
    public static var RunLoop: Self { Flow.TaskPriority.RunLoop.asSwift }
    public static var ASIOReactor: Self { Flow.TaskPriority.ASIOReactor.asSwift }
    public static var RunCycleFunction: Self { Flow.TaskPriority.RunCycleFunction.asSwift }
    public static var FlushTrace: Self { Flow.TaskPriority.FlushTrace.asSwift }
    public static var WriteSocket: Self { Flow.TaskPriority.WriteSocket.asSwift }
    public static var PollEIO: Self { Flow.TaskPriority.PollEIO.asSwift }
    public static var DiskIOComplete: Self { Flow.TaskPriority.DiskIOComplete.asSwift }
    public static var LoadBalancedEndpoint: Self { Flow.TaskPriority.LoadBalancedEndpoint.asSwift }
    public static var ReadSocket: Self { Flow.TaskPriority.ReadSocket.asSwift }
    public static var AcceptSocket: Self { Flow.TaskPriority.AcceptSocket.asSwift }
    public static var Handshake: Self { Flow.TaskPriority.Handshake.asSwift }
    public static var CoordinationReply: Self { Flow.TaskPriority.CoordinationReply.asSwift }
    public static var Coordination: Self { Flow.TaskPriority.Coordination.asSwift }
    public static var FailureMonitor: Self { Flow.TaskPriority.FailureMonitor.asSwift }
    public static var ResolutionMetrics: Self { Flow.TaskPriority.ResolutionMetrics.asSwift }
    public static var Worker: Self { Flow.TaskPriority.Worker.asSwift }
    public static var ClusterControllerWorker: Self { Flow.TaskPriority.ClusterControllerWorker.asSwift }
    public static var ClusterControllerRecruit: Self { Flow.TaskPriority.ClusterControllerRecruit.asSwift }
    public static var ClusterControllerRegister: Self { Flow.TaskPriority.ClusterControllerRegister.asSwift }
    public static var ClusterController: Self { Flow.TaskPriority.ClusterController.asSwift }
    public static var MasterTLogRejoin: Self { Flow.TaskPriority.MasterTLogRejoin.asSwift }
    public static var ProxyStorageRejoin: Self { Flow.TaskPriority.ProxyStorageRejoin.asSwift }
    public static var TLogQueuingMetrics: Self { Flow.TaskPriority.TLogQueuingMetrics.asSwift }
    public static var TLogPop: Self { Flow.TaskPriority.TLogPop.asSwift }
    public static var TLogPeekReply: Self { Flow.TaskPriority.TLogPeekReply.asSwift }
    public static var TLogPeek: Self { Flow.TaskPriority.TLogPeek.asSwift }
    public static var TLogCommitReply: Self { Flow.TaskPriority.TLogCommitReply.asSwift }
    public static var TLogCommit: Self { Flow.TaskPriority.TLogCommit.asSwift }
    public static var ReportLiveCommittedVersion: Self { Flow.TaskPriority.ReportLiveCommittedVersion.asSwift }
    public static var ProxyGetRawCommittedVersion: Self { Flow.TaskPriority.ProxyGetRawCommittedVersion.asSwift }
    public static var ProxyMasterVersionReply: Self { Flow.TaskPriority.ProxyMasterVersionReply.asSwift }
    public static var ProxyCommitYield2: Self { Flow.TaskPriority.ProxyCommitYield2.asSwift }
    public static var ProxyTLogCommitReply: Self { Flow.TaskPriority.ProxyTLogCommitReply.asSwift }
    public static var ProxyCommitYield1: Self { Flow.TaskPriority.ProxyCommitYield1.asSwift }
    public static var ProxyResolverReply: Self { Flow.TaskPriority.ProxyResolverReply.asSwift }
    public static var ProxyCommit: Self { Flow.TaskPriority.ProxyCommit.asSwift }
    public static var ProxyCommitBatcher: Self { Flow.TaskPriority.ProxyCommitBatcher.asSwift }
    public static var TLogConfirmRunningReply: Self { Flow.TaskPriority.TLogConfirmRunningReply.asSwift }
    public static var TLogConfirmRunning: Self { Flow.TaskPriority.TLogConfirmRunning.asSwift }
    public static var ProxyGRVTimer: Self { Flow.TaskPriority.ProxyGRVTimer.asSwift }
    public static var GetConsistentReadVersion: Self { Flow.TaskPriority.GetConsistentReadVersion.asSwift }
    public static var GetLiveCommittedVersionReply: Self { Flow.TaskPriority.GetLiveCommittedVersionReply.asSwift }
    public static var GetLiveCommittedVersion: Self { Flow.TaskPriority.GetLiveCommittedVersion.asSwift }
    public static var GetTLogPrevCommitVersion: Self { Flow.TaskPriority.GetTLogPrevCommitVersion.asSwift }
    public static var UpdateRecoveryTransactionVersion: Self { Flow.TaskPriority.UpdateRecoveryTransactionVersion.asSwift }
    public static var DefaultPromiseEndpoint: Self { Flow.TaskPriority.DefaultPromiseEndpoint.asSwift }
    public static var DefaultOnMainThread: Self { Flow.TaskPriority.DefaultOnMainThread.asSwift }
    public static var DefaultDelay: Self { Flow.TaskPriority.DefaultDelay.asSwift }
    public static var DefaultYield: Self { Flow.TaskPriority.DefaultYield.asSwift }
    public static var DiskRead: Self { Flow.TaskPriority.DiskRead.asSwift }
    public static var DefaultEndpoint: Self { Flow.TaskPriority.DefaultEndpoint.asSwift }
    public static var UnknownEndpoint: Self { Flow.TaskPriority.UnknownEndpoint.asSwift }
    public static var MoveKeys: Self { Flow.TaskPriority.MoveKeys.asSwift }
    public static var DataDistributionLaunch: Self { Flow.TaskPriority.DataDistributionLaunch.asSwift }
    public static var Ratekeeper: Self { Flow.TaskPriority.Ratekeeper.asSwift }
    public static var DataDistribution: Self { Flow.TaskPriority.DataDistribution.asSwift }
    public static var DataDistributionLow: Self { Flow.TaskPriority.DataDistributionLow.asSwift }
    public static var DataDistributionVeryLow: Self { Flow.TaskPriority.DataDistributionVeryLow.asSwift }
    public static var BlobManager: Self { Flow.TaskPriority.BlobManager.asSwift }
    public static var DiskWrite: Self { Flow.TaskPriority.DiskWrite.asSwift }
    public static var UpdateStorage: Self { Flow.TaskPriority.UpdateStorage.asSwift }
    public static var CompactCache: Self { Flow.TaskPriority.CompactCache.asSwift }
    public static var TLogSpilledPeekReply: Self { Flow.TaskPriority.TLogSpilledPeekReply.asSwift }
    public static var BlobWorkerReadChangeFeed: Self { Flow.TaskPriority.BlobWorkerReadChangeFeed.asSwift }
    public static var BlobWorkerUpdateFDB: Self { Flow.TaskPriority.BlobWorkerUpdateFDB.asSwift }
    public static var BlobWorkerUpdateStorage: Self { Flow.TaskPriority.BlobWorkerUpdateStorage.asSwift }
    public static var FetchKeys: Self { Flow.TaskPriority.FetchKeys.asSwift }
    public static var RestoreApplierWriteDB: Self { Flow.TaskPriority.RestoreApplierWriteDB.asSwift }
    public static var RestoreApplierReceiveMutations: Self { Flow.TaskPriority.RestoreApplierReceiveMutations.asSwift }
    public static var RestoreLoaderFinishVersionBatch: Self { Flow.TaskPriority.RestoreLoaderFinishVersionBatch.asSwift }
    public static var RestoreLoaderSendMutations: Self { Flow.TaskPriority.RestoreLoaderSendMutations.asSwift }
    public static var RestoreLoaderLoadFiles: Self { Flow.TaskPriority.RestoreLoaderLoadFiles.asSwift }
    public static var LowPriorityRead: Self { Flow.TaskPriority.LowPriorityRead.asSwift }
    public static var Low: Self { Flow.TaskPriority.Low.asSwift }
    public static var Min : Self { Flow.TaskPriority.Min.asSwift }
    public static var Zero: Self { Flow.TaskPriority.Zero.asSwift }

    var rawValueNet2: Int32 {
        switch self {
        case .Max: return Flow.TaskPriority.Max.rawValue
        case .RunLoop: return Flow.TaskPriority.RunLoop.rawValue
        case .ASIOReactor: return Flow.TaskPriority.ASIOReactor.rawValue
        case .RunCycleFunction: return Flow.TaskPriority.RunCycleFunction.rawValue
        case .FlushTrace: return Flow.TaskPriority.FlushTrace.rawValue
        case .WriteSocket: return Flow.TaskPriority.WriteSocket.rawValue
        case .PollEIO: return Flow.TaskPriority.PollEIO.rawValue
        case .DiskIOComplete: return Flow.TaskPriority.DiskIOComplete.rawValue
        case .LoadBalancedEndpoint: return Flow.TaskPriority.LoadBalancedEndpoint.rawValue
        case .ReadSocket: return Flow.TaskPriority.ReadSocket.rawValue
        case .AcceptSocket: return Flow.TaskPriority.AcceptSocket.rawValue
        case .Handshake: return Flow.TaskPriority.Handshake.rawValue
        case .CoordinationReply: return Flow.TaskPriority.CoordinationReply.rawValue
        case .Coordination: return Flow.TaskPriority.Coordination.rawValue
        case .FailureMonitor: return Flow.TaskPriority.FailureMonitor.rawValue
        case .ResolutionMetrics: return Flow.TaskPriority.ResolutionMetrics.rawValue
        case .Worker: return Flow.TaskPriority.Worker.rawValue
        case .ClusterControllerWorker: return Flow.TaskPriority.ClusterControllerWorker.rawValue
        case .ClusterControllerRecruit: return Flow.TaskPriority.ClusterControllerRecruit.rawValue
        case .ClusterControllerRegister: return Flow.TaskPriority.ClusterControllerRegister.rawValue
        case .ClusterController: return Flow.TaskPriority.ClusterController.rawValue
        case .MasterTLogRejoin: return Flow.TaskPriority.MasterTLogRejoin.rawValue
        case .ProxyStorageRejoin: return Flow.TaskPriority.ProxyStorageRejoin.rawValue
        case .TLogQueuingMetrics: return Flow.TaskPriority.TLogQueuingMetrics.rawValue
        case .TLogPop: return Flow.TaskPriority.TLogPop.rawValue
        case .TLogPeekReply: return Flow.TaskPriority.TLogPeekReply.rawValue
        case .TLogPeek: return Flow.TaskPriority.TLogPeek.rawValue
        case .TLogCommitReply: return Flow.TaskPriority.TLogCommitReply.rawValue
        case .TLogCommit: return Flow.TaskPriority.TLogCommit.rawValue
        case .ReportLiveCommittedVersion: return Flow.TaskPriority.ReportLiveCommittedVersion.rawValue
        case .ProxyGetRawCommittedVersion: return Flow.TaskPriority.ProxyGetRawCommittedVersion.rawValue
        case .ProxyMasterVersionReply: return Flow.TaskPriority.ProxyMasterVersionReply.rawValue
        case .ProxyCommitYield2: return Flow.TaskPriority.ProxyCommitYield2.rawValue
        case .ProxyTLogCommitReply: return Flow.TaskPriority.ProxyTLogCommitReply.rawValue
        case .ProxyCommitYield1: return Flow.TaskPriority.ProxyCommitYield1.rawValue
        case .ProxyResolverReply: return Flow.TaskPriority.ProxyResolverReply.rawValue
        case .ProxyCommit: return Flow.TaskPriority.ProxyCommit.rawValue
        case .ProxyCommitBatcher: return Flow.TaskPriority.ProxyCommitBatcher.rawValue
        case .TLogConfirmRunningReply: return Flow.TaskPriority.TLogConfirmRunningReply.rawValue
        case .TLogConfirmRunning: return Flow.TaskPriority.TLogConfirmRunning.rawValue
        case .ProxyGRVTimer: return Flow.TaskPriority.ProxyGRVTimer.rawValue
        case .GetConsistentReadVersion: return Flow.TaskPriority.GetConsistentReadVersion.rawValue
        case .GetLiveCommittedVersionReply: return Flow.TaskPriority.GetLiveCommittedVersionReply.rawValue
        case .GetLiveCommittedVersion: return Flow.TaskPriority.GetLiveCommittedVersion.rawValue
        case .GetTLogPrevCommitVersion: return Flow.TaskPriority.GetTLogPrevCommitVersion.rawValue
        case .UpdateRecoveryTransactionVersion: return Flow.TaskPriority.UpdateRecoveryTransactionVersion.rawValue
        case .DefaultPromiseEndpoint: return Flow.TaskPriority.DefaultPromiseEndpoint.rawValue
        case .DefaultOnMainThread: return Flow.TaskPriority.DefaultOnMainThread.rawValue
        case .DefaultDelay: return Flow.TaskPriority.DefaultDelay.rawValue
        case .DefaultYield: return Flow.TaskPriority.DefaultYield.rawValue
        case .DiskRead: return Flow.TaskPriority.DiskRead.rawValue
        case .DefaultEndpoint: return Flow.TaskPriority.DefaultEndpoint.rawValue
        case .UnknownEndpoint: return Flow.TaskPriority.UnknownEndpoint.rawValue
        case .MoveKeys: return Flow.TaskPriority.MoveKeys.rawValue
        case .DataDistributionLaunch: return Flow.TaskPriority.DataDistributionLaunch.rawValue
        case .Ratekeeper: return Flow.TaskPriority.Ratekeeper.rawValue
        case .DataDistribution: return Flow.TaskPriority.DataDistribution.rawValue
        case .DataDistributionLow: return Flow.TaskPriority.DataDistributionLow.rawValue
        case .DataDistributionVeryLow: return Flow.TaskPriority.DataDistributionVeryLow.rawValue
        case .BlobManager: return Flow.TaskPriority.BlobManager.rawValue
        case .DiskWrite: return Flow.TaskPriority.DiskWrite.rawValue
        case .UpdateStorage: return Flow.TaskPriority.UpdateStorage.rawValue
        case .CompactCache: return Flow.TaskPriority.CompactCache.rawValue
        case .TLogSpilledPeekReply: return Flow.TaskPriority.TLogSpilledPeekReply.rawValue
        case .BlobWorkerReadChangeFeed: return Flow.TaskPriority.BlobWorkerReadChangeFeed.rawValue
        case .BlobWorkerUpdateFDB: return Flow.TaskPriority.BlobWorkerUpdateFDB.rawValue
        case .BlobWorkerUpdateStorage: return Flow.TaskPriority.BlobWorkerUpdateStorage.rawValue
        case .FetchKeys: return Flow.TaskPriority.FetchKeys.rawValue
        case .RestoreApplierWriteDB: return Flow.TaskPriority.RestoreApplierWriteDB.rawValue
        case .RestoreApplierReceiveMutations: return Flow.TaskPriority.RestoreApplierReceiveMutations.rawValue
        case .RestoreLoaderFinishVersionBatch: return Flow.TaskPriority.RestoreLoaderFinishVersionBatch.rawValue
        case .RestoreLoaderSendMutations: return Flow.TaskPriority.RestoreLoaderSendMutations.rawValue
        case .RestoreLoaderLoadFiles: return Flow.TaskPriority.RestoreLoaderLoadFiles.rawValue
        case .LowPriorityRead: return Flow.TaskPriority.LowPriorityRead.rawValue
        case .Low: return Flow.TaskPriority.Low.rawValue
        case .Min: return Flow.TaskPriority.Min.rawValue
        case .Zero: return Flow.TaskPriority.Zero.rawValue
        default: fatalError("Unknown TaskPriority value: \(self.rawValue)")
        }
    }

    var name: String {
        switch self {
        case .Max: return "Max"
        case .RunLoop: return "RunLoop"
        case .ASIOReactor: return "ASIOReactor"
        case .RunCycleFunction: return "RunCycleFunction"
        case .FlushTrace: return "FlushTrace"
        case .WriteSocket: return "WriteSocket"
        case .PollEIO: return "PollEIO"
        case .DiskIOComplete: return "DiskIOComplete"
        case .LoadBalancedEndpoint: return "LoadBalancedEndpoint"
        case .ReadSocket: return "ReadSocket"
        case .AcceptSocket: return "AcceptSocket"
        case .Handshake: return "Handshake"
        case .CoordinationReply: return "CoordinationReply"
        case .Coordination: return "Coordination"
        case .FailureMonitor: return "FailureMonitor"
        case .ResolutionMetrics: return "ResolutionMetrics"
        case .Worker: return "Worker"
        case .ClusterControllerWorker: return "ClusterControllerWorker"
        case .ClusterControllerRecruit: return "ClusterControllerRecruit"
        case .ClusterControllerRegister: return "ClusterControllerRegister"
        case .ClusterController: return "ClusterController"
        case .MasterTLogRejoin: return "MasterTLogRejoin"
        case .ProxyStorageRejoin: return "ProxyStorageRejoin"
        case .TLogQueuingMetrics: return "TLogQueuingMetrics"
        case .TLogPop: return "TLogPop"
        case .TLogPeekReply: return "TLogPeekReply"
        case .TLogPeek: return "TLogPeek"
        case .TLogCommitReply: return "TLogCommitReply"
        case .TLogCommit: return "TLogCommit"
        case .ReportLiveCommittedVersion: return "ReportLiveCommittedVersion"
        case .ProxyGetRawCommittedVersion: return "ProxyGetRawCommittedVersion"
        case .ProxyMasterVersionReply: return "ProxyMasterVersionReply"
        case .ProxyCommitYield2: return "ProxyCommitYield2"
        case .ProxyTLogCommitReply: return "ProxyTLogCommitReply"
        case .ProxyCommitYield1: return "ProxyCommitYield1"
        case .ProxyResolverReply: return "ProxyResolverReply"
        case .ProxyCommit: return "ProxyCommit"
        case .ProxyCommitBatcher: return "ProxyCommitBatcher"
        case .TLogConfirmRunningReply: return "TLogConfirmRunningReply"
        case .TLogConfirmRunning: return "TLogConfirmRunning"
        case .ProxyGRVTimer: return "ProxyGRVTimer"
        case .GetConsistentReadVersion: return "GetConsistentReadVersion"
        case .GetLiveCommittedVersionReply: return "GetLiveCommittedVersionReply"
        case .GetLiveCommittedVersion: return "GetLiveCommittedVersion"
        case .GetTLogPrevCommitVersion: return "GetTLogPrevCommitVersion"
        case .UpdateRecoveryTransactionVersion: return "UpdateRecoveryTransactionVersion"
        case .DefaultPromiseEndpoint: return "DefaultPromiseEndpoint"
        case .DefaultOnMainThread: return "DefaultOnMainThread"
        case .DefaultDelay: return "DefaultDelay"
        case .DefaultYield: return "DefaultYield"
        case .DiskRead: return "DiskRead"
        case .DefaultEndpoint: return "DefaultEndpoint"
        case .UnknownEndpoint: return "UnknownEndpoint"
        case .MoveKeys: return "MoveKeys"
        case .DataDistributionLaunch: return "DataDistributionLaunch"
        case .Ratekeeper: return "Ratekeeper"
        case .DataDistribution: return "DataDistribution"
        case .DataDistributionLow: return "DataDistributionLow"
        case .DataDistributionVeryLow: return "DataDistributionVeryLow"
        case .BlobManager: return "BlobManager"
        case .DiskWrite: return "DiskWrite"
        case .UpdateStorage: return "UpdateStorage"
        case .CompactCache: return "CompactCache"
        case .TLogSpilledPeekReply: return "TLogSpilledPeekReply"
        case .BlobWorkerReadChangeFeed: return "BlobWorkerReadChangeFeed"
        case .BlobWorkerUpdateFDB: return "BlobWorkerUpdateFDB"
        case .BlobWorkerUpdateStorage: return "BlobWorkerUpdateStorage"
        case .FetchKeys: return "FetchKeys"
        case .RestoreApplierWriteDB: return "RestoreApplierWriteDB"
        case .RestoreApplierReceiveMutations: return "RestoreApplierReceiveMutations"
        case .RestoreLoaderFinishVersionBatch: return "RestoreLoaderFinishVersionBatch"
        case .RestoreLoaderSendMutations: return "RestoreLoaderSendMutations"
        case .RestoreLoaderLoadFiles: return "RestoreLoaderLoadFiles"
        case .LowPriorityRead: return "LowPriorityRead"
        case .Low: return "Low"
        case .Min: return "Min"
        case .Zero: return "Zero"
        default: return "<unknown:\(self.rawValue)>"
        }
    }
}

extension _Concurrency.TaskPriority: CustomStringConvertible {
    public var description: String {
        "_Concurrency.TaskPriority(\(self.name), rawValue: \(rawValue), rawValueNet2: \(self.rawValueNet2))"
    }
}
/*
 * ProcessClassRecruitment.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/core/ProcessClassRecruitment.h"

namespace recruitment {

Fitness machineClassFitness(ProcessClass const& processClass, ClusterRole role) {
	auto type = processClass.classType();
	switch (role) {
	case Storage:
		switch (type) {
		case ProcessClass::StorageClass:
			return BestFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::TransactionClass:
		case ProcessClass::LogClass:
			return WorstFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
		default:
			return NeverAssign;
		}
	case TLog:
		switch (type) {
		case ProcessClass::LogClass:
			return BestFit;
		case ProcessClass::TransactionClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::StorageClass:
			return WorstFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return NeverAssign;
		}
	case CommitProxy: // Resolver, Master, CommitProxy, and GrvProxy need to be the same besides best fit
		switch (type) {
		case ProcessClass::CommitProxyClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::TransactionClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case GrvProxy: // Resolver, Master, CommitProxy, and GrvProxy need to be the same besides best fit
		switch (type) {
		case ProcessClass::GrvProxyClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::TransactionClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case Master: // Resolver, Master, CommitProxy, and GrvProxy need to be the same besides best fit
		switch (type) {
		case ProcessClass::MasterClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::TransactionClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case Resolver: // Resolver, Master, CommitProxy, and GrvProxy need to be the same besides best fit
		switch (type) {
		case ProcessClass::ResolutionClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::TransactionClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case LogRouter:
		switch (type) {
		case ProcessClass::LogRouterClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::TransactionClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case Backup:
		switch (type) {
		case ProcessClass::BackupClass:
			return BestFit;
		case ProcessClass::StatelessClass:
		case ProcessClass::LogRouterClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::TransactionClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::MasterClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case ClusterController:
		switch (type) {
		case ProcessClass::ClusterControllerClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::MasterClass:
		case ProcessClass::ResolutionClass:
		case ProcessClass::TransactionClass:
		case ProcessClass::CommitProxyClass:
		case ProcessClass::GrvProxyClass:
		case ProcessClass::LogRouterClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case DataDistributor:
		switch (type) {
		case ProcessClass::DataDistributorClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::MasterClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case Ratekeeper:
		switch (type) {
		case ProcessClass::RatekeeperClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::MasterClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case ConsistencyScan:
		switch (type) {
		case ProcessClass::ConsistencyScanClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::MasterClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case BlobManager:
		switch (type) {
		case ProcessClass::BlobManagerClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::MasterClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	case BlobWorker:
		switch (type) {
		case ProcessClass::BlobWorkerClass:
			return BestFit;
		default:
			return NeverAssign;
		}
	case BlobMigrator:
		switch (type) {
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::MasterClass:
		case ProcessClass::BlobWorkerClass:
			return OkayFit;
		default:
			return NeverAssign;
		}
	case EncryptKeyProxy:
		switch (type) {
		case ProcessClass::EncryptKeyProxyClass:
			return BestFit;
		case ProcessClass::StatelessClass:
			return GoodFit;
		case ProcessClass::UnsetClass:
			return UnsetFit;
		case ProcessClass::MasterClass:
			return OkayFit;
		case ProcessClass::CoordinatorClass:
		case ProcessClass::TesterClass:
		case ProcessClass::BlobWorkerClass:
			return NeverAssign;
		default:
			return WorstFit;
		}
	default:
		return NeverAssign;
	}
}

} // namespace recruitment

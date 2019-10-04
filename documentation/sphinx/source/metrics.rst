.. _metrics:

######################################################
Metrics
######################################################

.. _metric-introduction:

Introduction
============

FoundationDB's has different metrics.

* StorageMetrics: Storage metrics are for transient storage, storage server. The sample
  info should constitute sample, size, sampledSize.

* EmptyQueries: Metrics tracking empty reads.

* Transaction Metrics: Transaction metrics are for the database transaction metrics.

* Health Metrics: Health metrics constitutes storage stats. Few of these are worst
  storage queue size, worst transaction log queue size, transacations per second limit,
  worst storage non durable versions. Per process health metrics are CPU usage, disk
  usage, storage queue size, non durable versions, transactions log queue size.

* Machine Metrics: Covering machine failure scenarios.


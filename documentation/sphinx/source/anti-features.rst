#############
Anti-Features
#############

What is an anti-feature?
========================

FoundationDB's design clearly distinguishes its set of :doc:`core features <features>` from those that are better supported elsewhere, often as :doc:`layers <layer-concept>`. FoundationDB has deliberately limited its database core API to the minimal set of features needed to expose a scalable, fault-tolerant database with ACID transactions and high performance. As a result, there is also a set of *anti-features:* features that FoundationDB does not intend to provide in its database core. (For a detailed list of technical limitations, whether relating to our design or just to the current version, see :doc:`known-limitations`).

Data models
===========

With the rise of NoSQL databases, a wide variety of data models have become popular. Geospatial models are used for geographical data, JSON and XML for hierarchical documents, and column-family models for sparse, tabular data. Of course, the traditional relational model is also still widely used. Each of these data models is useful and appropriate for different use cases. An application that is even moderately complex may have distinct data sets that best fit different models.

FoundationDB's core exposes a single data model: an ordered key-value store. When combined with multikey, ACID transactions, this model is powerful enough to be used directly by applications, as illustrated in our :doc:`tutorial examples <tutorials>`. However, our ordered key-value store can also serve as a foundation for other data models, including those above. Because of the simplicity of the key-value model, it is easy to map higher-level models onto it. (See :doc:`data-modeling` for examples of these mappings).

Transactions are the essential capability that allows a developer to build a data model reliably and efficiently in a layer. If a higher-level model requires multiple key-value pairs per data item, a layer can update them all in a single transaction, ensuring their consistency.

Query languages
===============

A variety of query languages have come into use alongside NoSQL databases. For example, new query languages specific to JSON databases have become popular. XQuery is used with XML. Languages with SQL-like syntax are often defined for column-family databases. Of course, SQL itself is still heavily used with relational databases.

The FoundationDB core exposes a robust and powerful API but includes no separate query language. FoundationDB empowers developers to employ a broad range of data models and use the query languages best suited to their applications, implemented as layers.

Analytic frameworks
===================

Analytic processing of large data sets has become prominent in conjunction with NoSQL databases. Some popular approaches offer generic frameworks for batch processing (e.g, MapReduce); some offer frameworks for real-time, stream processing; some focus on more specific analytics, whether using traditional aggregation functions or statistical techniques such as machine learning. The most effective approach to analytics depends on the details of the application's data model.

Analytic frameworks are outside the scope of the FoundationDB core. However, because the core can use range reads to efficiently scan large swaths of data, analytics can be implemented within a layer, possibly as part of a query language.

Disconnected operation
======================

The rise of mobile computing has led to the model of *disconnected operation* in which an application on a mobile device remains available even when it's not connected to a central server. Examples of this sort of mobile application include note taking, to-do lists, and document editing.

While a central server running FoundationDB could be used as a database for a mobile application to connect and sync to from time to time, FoundationDB's core does not itself directly provide disconnected operation. Because it would sacrifice ACID properties, we believe that in those applications where disconnected operation is needed, the database is the wrong tier to implement it.

Long-running read/write transactions
====================================

FoundationDB aims to provide low latencies across a range of metrics. Transaction latencies, in particular, are typically under 15 milliseconds. Some applications require very large operations that require several seconds or more, several orders of magnitude larger than our usual transaction latency. Large operations of this kind are best approached in FoundationDB by decomposition into a set of smaller transactions.

FoundationDB does not support *long-running read/write transactions*, currently defined as those 
:ref:`lasting over five seconds <long-transactions>`. The system employs multiversion concurrency control and maintains conflict information for a five second period. A transaction that is kept open longer will not be able to commit.

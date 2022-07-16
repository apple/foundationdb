.. FoundationDB documentation master file
   
######################
FoundationDB |version|
######################

Overview
========

FoundationDB is a distributed database designed to handle large volumes of structured data across clusters of commodity servers. It organizes data as an ordered key-value store and employs :doc:`ACID transactions <transaction-manifesto>` for all operations. It is especially well-suited for read/write workloads but also has excellent :doc:`performance <performance>` for write-intensive workloads. Users interact with the database using a :doc:`API language binding <api-reference>`. You can :doc:`begin local development <local-dev>` today.

Documentation
=============

FoundationDB is a robust choice for a broad range of use cases:

**Developers can store all types of data.** FoundationDB is multi-model, meaning you can store many types of data in a single database. All data is safely stored, distributed, and replicated in FoundationDB.

**Administrators easily scale and handle hardware failures.** FoundationDB is easy to install, grow, and manage. It has a distributed architecture that gracefully scales out and handles faults while acting like a single ACID database.

**FoundationDB has industry-leading performance.** FoundationDB provides amazing performance on commodity hardware, allowing you to support very heavy loads at low cost.

**FoundationDB supports flexible application architectures.** Your application can talk directly to FoundationDB, to a layer, or both. Layers provide new capability on top of FoundationDB but are stateless.

The latest changes are detailed in :ref:`release-notes`. The documentation has the following sections:

* :doc:`why-foundationdb` describes the technical alternatives involved in NoSQL database design and explains the advantages of transaction processing at scale.

* :doc:`technical-overview` explains the engineering design of FoundationDB, with detailed information on its features, architecture, and performance.

* :doc:`client-design` contains documentation on getting started, data modeling, and design principles for building applications with FoundationDB.

* :doc:`design-recipes` give specific examples of how to build new data models, indexes, and more on top of the key-value store API.

* :doc:`api-reference` give a detailed description of the API for each language.

* :doc:`tutorials` provide simple examples of client design using FoundationDB.

* :doc:`administration` contains documentation on administering FoundationDB.

* :doc:`monitored-metrics` contains documentation on monitoring and alerting for FoundationDB.

* :doc:`redwood` contains documentation on Redwood Storage Engine.

* :doc:`visibility` contains documentation related to Visibility into FoundationDB.

.. toctree::
   :maxdepth: 1
   :titlesonly:
   :hidden:

   local-dev
   why-foundationdb
   technical-overview
   client-design
   design-recipes
   api-reference
   tutorials
   administration
   monitored-metrics
   redwood
   visibility
   earlier-release-notes

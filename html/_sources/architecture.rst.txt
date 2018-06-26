############
Architecture
############

FoundationDB makes your architecture flexible and easy to operate. Your applications can send their data directly to the FoundationDB or to a :doc:`layer<layer-concept>`, a user-written module that can provide a new data model, compatibility with existing systems, or even serve as an entire framework. In both cases, all data is stored in a single place via an ordered, transactional key-value API.

The following diagram details the logical architecture.

.. image:: /images/Architecture.png

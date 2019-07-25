###########
Consistency
###########

Two meanings of consistency
===========================

The concept of *consistency* comes up a great deal in the context of distributed databases. However, the term is used in one sense in the context of the ACID properties and in another in the context of the CAP theorem. The two meanings are often confused.

* The "C" in ACID refers to the property that data remains within an application's `integrity constraints <http://en.wikipedia.org/wiki/Integrity_constraints>`_. (For example, the constraint that some data and its index are consistent with respect to each other.)

* The "C" in CAP relates to a `consistency model <http://en.wikipedia.org/wiki/Consistency_model>`_, which describes the conditions under which write operations from one client become visible to other clients. (One example is the eventual consistency model, in which writes are expected to be consistent across replicas after a sufficiently long period.)

Integrity constraints relate to the application domain, whereas consistency models relate to the internal operation of the database itself. Both are important in that failure to support them can lead to corrupted data.

Integrity constraints
=====================

Applications typically have integrity constraints defined by their domains. These constraints can take the form of type constraints on certain data values ("domain integrity"), a relation between two or more data values ("referential integrity"), or business rules drawn from the application domain.

In relational database management systems, integrity constraints are usually specified using SQL when the relational schema is designed. In this approach, the specification and enforcement of integrity constraints is tightly bound to the relational model. At the other extreme, most NoSQL databases simply do not support integrity constraints, shifting the burden of maintaining data integrity entirely onto the application developer.

FoundationDB takes a third approach. Because integrity constraints are defined by the application domain, the FoundationDB core does not directly enforce them. However, FoundationDB's transactions, with their guarantees of atomicity and isolation, give the application developer the power to straightforwardly maintain integrity constraints as the domain requires. In simple terms, so long as each transaction individually maintains the desired constraints, FoundationDB guarantees that multiple clients executing transactions simultaneously will also maintain those constraints. This approach allows a broad range of data models, including document-oriented or relational models to be built as :doc:`layers <layer-concept>` that maintain their own integrity constraints.

Consistency model
=================

Consistency models serve to define the guarantees the database provides about when concurrent writes become visible to readers. Consistency models fall along a spectrum, depending on the strength of the guarantees. In general, stronger consistency models make reasoning about the database easier and speed development. For example, *causal* consistency guarantees that readers see all previously committed writes. *Eventual* consistency guarantees only that readers see writes after "sufficient time". Eventual consistency is the model used in many of the first-generation NoSQL systems.

FoundationDB provides strict serializability, the strongest possible consistency model, to provide the greatest possible ease of development.

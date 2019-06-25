#####################
Transaction Manifesto
#####################

What is a transaction?
======================

A transaction is a set of database reads and writes that is handled as a unit with a few crucial properties. First, all reads in the transaction see the same snapshot of the database (e.g. do not see changes to the data resulting from other transactions executing concurrently). Second, the writes within a transaction either all succeed or all fail. (Failure might be caused, for example, by a connection loss.) Lastly, after a transaction succeeds (commits), the writes are permanently stored. These properties of transactions (called isolation, atomicity, and durability) are the fundamental "ACID" guarantees. We at FoundationDB think that support for ACID transactions is much more than a nice extra feature; transactions are critical for building robust systems in an efficient, simple way.

Everyone needs transactions
===========================

Every application that needs to support simultaneous clients (concurrency) should be built using transactions with ACID properties. Transactions are the simplest and strongest programming model available to handle concurrency and will play an increasingly prominent role as NoSQL technology matures.

A common misconception is that transactions are useful only for e-commerce or banking, which deal with financial transactions. However, the power of transactions comes from their engineering impact on application-building, not from the details of any particular application. Because of the ACID properties, transactions can be composed to create new abstractions, support more efficient data structures, and enforce integrity constraints. As a result, building applications with transactions is easier, more reliable, and more extensible than any of the alternatives.

It may seem that the design tradeoffs for your system compel you to give up the advantages of transactions to gain speed, scalability, and fault tolerance. Systems built this way are usually fragile, difficult to manage, and often nearly impossible to adapt to changing business needs. The costs of foregoing transactions are rarely worth the benefits, especially if an alternative can provide transactional integrity at scale.

Transactions make concurrency simple
====================================

Concurrency arises whenever multiple clients, users, or parts of an application read and write the same data at the same time. Transactions make managing concurrency simple for developers. The main property of transactions that achieves this simplicity is *isolation*, the "I" in ACID. When a system guarantees that transactions are fully isolated (known as "serializability"), developers can treat each transaction as if it were executed sequentially, even though it may actually be executed concurrently. The burden of reasoning about potential interactions between operations from separate transactions goes away.

Slightly better than nothing: local transactions
================================================

Some systems offer ACID transactions for a limited set of predefined operations, typically restricted by the structure of the data model. For example, a document database may allow a single document to be updated in a transaction.

However, the real power of transactions comes when they can be *defined by the application developer* over any set of data elements. An application developer working with a key-value store should be able to define transactions that read and write any number of key-value pairs. When developers can freely define transactions without arbitrary restrictions, they can use transactions as fundamental building blocks for an application.

Transactions enable abstraction
===============================

Application-defined transactions are *composable*. Isolation, in combination with *atomicity* (the "A" in ACID), ensures that the execution of one transaction can't affect the visible behavior of another. This guarantee makes transactions composable with one another, allowing them to be assembled to build new *abstractions*. Abstractions can be encapsulated in *layers*. For example, a common abstraction is to maintain an index along with the primary data for to enable quickly finding data items matching some constraint. In any key-value store, this functionality can easily be implemented by storing a second copy of the data with the desired index field as the key. However, concurrent updates to the data potentially complicate this simple design. Without transactions it is difficult to ensure that, as the data changes, both the data and the index are updated consistently. With transactions, an indexing layer can update both the data and the index in a single transaction, guaranteeing their consistency and allowing a strong abstraction.

Transactions allow layers to build abstractions simply and efficiently, providing a highly extensible capability to support multiple data models. Data models optimized for hierarchical documents, column-oriented data, or relational data can all be implemented in layers on top of an ordered key-value store. In most of these cases, a single data object in the higher-level model will map onto multiple key-value pairs. Transactions make it straightforward to reliably implement these mappings by wrapping multiple key-value updates in atomic units.

Transactions enable efficient data representations
==================================================

The benefits of transactions extend beyond a more flexible choice of high-level data model. They can also enable a more efficient data representation within a given model. For example, consider the design pattern of *embedding* data commonly used in document-oriented databases. (Someone from the relational database world might know this as denormalizing.) In the embedding pattern, you nest data within the hierarchical structure of a single document (often duplicating it) rather than storing a reference to another document that can be shared.

Embedding is often motivated by a lack of support for global transactions. Since most document-oriented databases only allow atomic operations on a single document, the developer must "squeeze" the data into a single document to enable atomic updates. The resulting document with embedded data will be larger, more complex, and usually less efficient to access and update.

With transactions, there is no need to employ embedding to achieve atomic updates. Data elements can be modeled to optimize access efficiency, using multiple documents when appropriate, and still be updated in a single transaction to guarantee consistency. Moreover, the data can be shared by multiple clients and concurrently updated. Again, transactions provide the concurrency control needed to safely manage shared state.

Transactions enable flexibility
===============================

Most applications supporting significant business functions experience changing requirements. (And, of course, they usually need to do more, not less!) You may be tempted to think of transactional integrity as a nice feature but one that that's not essential for your application. To be sure, many applications can be built to avoid the need for global transactions by carefully designing the data schema for a specific, simple access pattern. However, when those applications evolve, having the flexibility to modify your data model and the power of global transactions can make the difference between an easy change and re-architecting.

Imagine you operate a web application in which users both generate posts and receive posts from other users. All posts are maintained in a back-end database. Management has asked you to build a dashboard for doing some basic analytics. Although your data store is read-write, the dashboard is initially intended to perform read-only queries. The data is just for analysis, so if results are sometimes a bit out-of-date, nobody will notice in the UI. Overall, you see no need for transactions. You put together some great data visualizations that allow your company to analyze and view posts in new ways.

On the up side, the dashboard proves very popular. On the down side, you start hearing demands for new features: users want the dashboard to support editing and moderation of the posts found using the analytics. Also, the ad-serving division wants access to the dashboard data via an API to drive their billing system. You now have a situation where the simple read-only model of the dashboard has gone out the window, and your new API needs to be able to provide strong guarantees about the results it is delivering.

This sort of evolution of requirements is a natural and frequently occurring pattern. Transactions make the difference between being able to easily add these features and throwing out large parts of your code and starting over.

Transactions are not as expensive as you think
==============================================

You may be reluctant to employ transactions due to a perception that they impose serious technical tradeoffs, particularly for the kinds of high performance applications targeted by NoSQL databases. However, when you examine the tradeoffs more closely, the cost to the user is quite low. (The cost to the database engineer is quite high; distributed transactional systems are difficult to build!)

Performance and scalability?
----------------------------

We know of no practical limits to the scalability or performance of systems supporting transactions. When the movement toward NoSQL databases began, early systems, such as Google Bigtable, adopted minimal designs tightly focused on the scalability and performance. Features familiar from relational databases had been aggressively shed and the supposition was that the abandoned features were unnecessary or even harmful for scalability and performance goals.

Those suppositions were wrong. It is becoming clear that supporting transactions is a matter of engineering effort, not a fundamental tradeoff in the design space. Algorithms for maintaining transactional integrity can be distributed and scale out like many other problems. Transactional integrity does come at a CPU cost, but in our experience *that cost is less than 10% of total system CPU*. This is a small price to pay for transactional integrity and can easily be made up elsewhere.

Write Latency?
--------------

Transactions guarantee the *durability* of writes (the "D" in ACID). This guarantee comes with some increase in write latency. Durability means that committed writes stay committed, even in the face of subsequent hardware failures. As such, durability is an important component of fault tolerance. NoSQL systems that don't support durability are necessarily weaker in regard to fault tolerance. Because of the importance of true fault tolerance, the write latency required for durable transactions is usually worth the cost. For those applications that have a hard requirement to minimize write latency, durability can be turned off without sacrificing the "ACI" properties.

Transactions are the future of NoSQL
====================================

As NoSQL databases become more broadly used for a wide variety of purposes, more applications built on them employ non-trivial concurrency from multiple clients. Without adequate concurrency control, all the traditional problems of concurrency re-emerge and create a significant burden for application developers. ACID transactions simplify concurrency for developers by providing strictly serializable operations that can be composed to properly engineer application software. If you're building an application that needs to be scalable and you don't have transactions, you will eventually be burned. Fortunately, the scalability, fault-tolerance, and performance of NoSQL databases are still achievable with transactions. The choice to use transactions is ultimately not a matter of fundamental tradeoffs but of sound engineering. As the technology matures, transactions will form a foundational capability for future NoSQL databases.

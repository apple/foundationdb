#############
Layer Concept
#############

When we started building FoundationDB, instead of thinking about all the features that it could have, we asked ourselves *what features could we take away?* Almost everything, we decided. We simplified the core, allowing us to focus on making it as strong as possible, and built additional features as layers. Here's how the approach works:

The old way
===========

When you choose a database today, you're not choosing one piece of technology, you're choosing three: storage technology, data model, and API/query language. For example, if you choose Postgres, you are choosing the Postgres storage engine, a relational data model, and the SQL query language. If you choose MongoDB you are choosing the MongoDB distributed storage engine, a document data model, and the MongoDB API. In systems like these, features are interwoven between all of the layers. For example, both of those systems provide indexes, and the notion of an index exists in all three layers.

Document databases, column-oriented, row-oriented, JSON, key-value, etc. all make sense in the right context, and often different parts of an application call for different choices. This creates a tough decision: Use a whole new database to support a new data model, or try to shoehorn data into your existing database.

The FoundationDB way
====================

FoundationDB decouples its data storage technology from its data model. FoundationDB's core ordered key-value storage technology can be efficiently adapted and remapped to a broad array of rich data models. Using indexing as an example, FoundationDB's core provides no indexing and never will. Instead, a layer provides indexing by storing two kinds of key-values, one for the data and one for the index.

For example, the ``people/alice/eye_color = blue`` key-value stores data about Alice's eye color and the ``eye_color/blue/alice = true`` key-value stores an index of people by eye color. Now, finding all people with blue eyes is as simple as finding all keys that start with ``eye_color/blue/``. Since FoundationDB's core keeps all keys in order and all those keys share a common prefix, the operation can be efficiently implemented with a single range-read operation.

Of course, any ordered key-value database could use this approach. The real magic comes when you mix in true ACID transactions. This allows the indexing layer to update both the data and the index in a single transaction, ensuring their consistency. The importance of this guarantee can't be overstated. Transactions allow layers to be built simply, reliably, and efficiently.

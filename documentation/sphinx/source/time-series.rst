.. highlight:: ruby

################
Time-Series Data
################

FoundationDB is a multi-model database that combines scalability, fault-tolerance, and high performance with multi-key ACID transactions. A common use case involves time-series data, which are easy to model in FoundationDB.

What is Time-Series Data?
=========================

Modern applications generate huge amounts of “event”-based data organized by the timestamp of when the event occurred. Sensor data, call or SMS data, network monitoring or analytics data, and financial applications all have time components, and data can be generated in huge volumes as automated “events” occur in connected networks and systems. Although "time-series” is often used to refer to huge, maybe read-only, data sets that are mined for insights using something like a Hadoop cluster, we are finding more and more people using “time-oriented” data as part of an operational workload supporting their real-time applications and systems.

We have a number of customers, ranging from network analytics providers to VoIP/telephony systems, who are taking a time-oriented view of their data stored in FoundationDB. They’re using an event timestamp to help with dashboards, customer UIs, and other reporting functions provided by their applications in real time, sometimes processing hundreds of thousands of operations per second.

Time-series Data and FoundationDB
=================================

If you only have a few fields to store per database record, it’s pretty simple to picture what a time-oriented record would look like. If you were tracking analytics for a website, you might have a few fields, such as a website identifier, a page identifier, and a browser type. That’s pretty simple to picture fitting into a defined, relational-database table that looks like this:

============= ========== ======= ==========
Timestamp     website_ID page_ID browser_ID
============= ========== ======= ==========
UTC_timestamp 0          24      6
UTC_timestamp 4          22      4
============= ========== ======= ==========

If you’re already thinking about the world in keys and values, you’ve probably figured out that those data fields translate easily to FoundationDB as well. But what you may not have already figured out is that keys that can be structured as **Tuples** that are exposed to applications through the Tuple API and have very useful properties. In FoundationDB, we could use a simple Tuple structure to store each event that you’re tracking as a data element that looks something like this::

    ([time_data_in_milliseconds, website_ID, page_ID, browser_ID], value)

So the key for a particular data element within the KV store is formed from a Tuple of sub-keys packed together by the Tuple layer into a single key using a pre-defined delimiter between each sub-key. This Tuple structure is really helpful because you can break up the time into key spaces that fit your reporting or querying patterns. 

FoundationDB’s KV store is also globally ordered, so the keys are arranged in increasing order over the entire database. Together, the ordering and tuple structure allow you to perform queries along defined time ranges (e.g. monthly) using an extremely simple range operation. If you take the previous tuple structure that used a time component in milliseconds and divide the keys into smaller parts, you get a tuple that looks like this::

    ([year, month, day, hour, minute, seconds], value)

You could also use a tuple for the value component of the key-value pair to serialize the data elements associated with the timestamp that are used for application-level reporting or logic after retrieval::

    ([year, month, day, hour, minute, seconds], [website_ID, page_ID, browser_ID])

Then a query (written in Ruby) for all data elements in a particular month is just::

    db.transact do |tr|
        results = tr.get_range_start_with(FDB::Tuple.pack(year, month,))
    end

(This transaction returns an array of matching elements that you can iterate over in your application.)

But it gets even more convenient because you can also create global, consistent indexes that help you track metrics about your event data as well! In the KV store, indexes are stored like regular data, and updating your index is handled atomically as part of a transaction. So in this example, maybe you want to create an index by website_ID so you can easily analyze data for a particular website over time. That index would look something like this::

    # define a subspace in your application for the index
    subspace_website_index = FDB::Subspace.new(['website-index'])

    # set up a transaction that stores information about a website in a website-oriented index
    tr.set(subspace_website_index.pack([website_ID, year, month, day, seconds]), value)

And let’s say you also want to create a couple of simple counters that track some common metrics for your websites: The first is a simple counter of the number of visits to your website each month::

    # again, you define the subspace that you want to use for the counter
    subspace_count = FDB::Subspace.new(['subspace-counters'])

    # then use the atomic add functionality to increment the monthly counter
    tr.add(subspace_count.pack([website_ID, year, month]), [1].pack('q<'))

Or maybe you want to track individual pages so you have a record of page access information over time. Again, that’s pretty simple::

    # define the subspace that you want to use for the page counters
    subspace_pages = FDB::Subspace.new(['pages-counter'])

    # then use atomic add again with a tuple sorted by time and counter
    tr.add(subpace_pages.pack([page_ID, year, month, day, hour]), [1].pack('q<'))

    # then retrieving any information about this page just requires specifying the time range you want to return the associated counter values
    tr.get(subspace_pages.pack([year, month, day, hour]))

or::

    # use a shorter prefix to get back a range so your application can aggregate the individual counters
    tr.get_range_start_with(subspace_pages.pack([year, month, day,]))

Putting it all together, your application just updates the primary copy of the data along with the index by wrapping everything in a single transaction::

    db.transact do |tr|
        tr.set(FDB::Tuple.pack([year, month, day, seconds]), FDB::Tuple.pack([website_ID, page_ID, browser_ID]))
        tr.set(subspace_website_index.pack([website_ID, year, month, day, seconds]), value)
        tr.add(subspace_count.pack([website_ID, year, month]), [1].pack('q<'))
        tr.add(subpace_pages.pack([page_ID, year, month, day, hour]), [1].pack('q<'))
    end

Ordering and Transactions
=========================

FoundationDB’s ability to let you structure your data in different ways, keep track of metrics, and search it with varying granularity is a direct result of two key features of our key-value store: global ordering and ACID transactions. And as you’ve seen from the code included above, the direct impact of these properties is simpler application code and overall faster development.

Global ordering makes a big difference if you’re attempting to process significant amounts of sequential information because the database can retrieve that information quickly and efficiently. So rather than having to package your data into a single database object or broadcast a request for many individual data elements that correspond to a given range of application data (e.g. time0, time1, time2, . . ., timeN), a globally ordered storage system, like FoundationDB, can generate a single range request to the database for the matching data. And internally, FoundationDB can further optimize requests by knowing which data resides on which machines, so there’s no need to broadcast the data request to all machines in the cluster.

Global indexing also makes a huge difference in terms of application complexity and database efficiency. Many non-relational databases provide node-specific indexing and secondary indexing, but if you wanted global indexes, you would have to build those at the application level to ensure the index and related data get updated atomically.

Because FoundationDB supports global indexing and ACID transactions, the database itself will handle updates to the relevant data without intervention or management by the application. (And because an index is just like regular data, it has the same properties, like ordering, for efficient access.) So now, the application interacting with the database can make simple requests to the database as part of a single transaction and avoid having to reason about whether the data requested from the data is actually up-to-date and valid.

Conclusion
==========

FoundationDB offers many great benefits for developers working with time-series data or any data that has a temporal component that’s used for reporting and organization. With ACID transactions, ordering, global indexes, and a data model that gives you a lot of flexibility, FoundationDB makes application development much simpler and more manageable.

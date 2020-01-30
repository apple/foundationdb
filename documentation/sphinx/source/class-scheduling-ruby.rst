########################
Class Scheduling in Ruby
########################

This tutorial provides a walkthrough of designing and building a simple application in Ruby using FoundationDB. In this tutorial, we use a few simple data modeling techniques. For a more in-depth discussion of data modeling in FoundationDB, see :doc:`data-modeling`.

The concepts in this tutorial are applicable to all the :doc:`languages <api-reference>` supported by FoundationDB. If you prefer, you can see a version of this tutorial in :doc:`Python <class-scheduling>`, :doc:`Java <class-scheduling-java>`, or :doc:`Go <class-scheduling-go>`.

.. _class-sched-ruby-first-steps:

First steps
===========

Let's begin with "Hello world."

If you have not yet installed FoundationDB, see :doc:`getting-started-mac` or :doc:`getting-started-linux`.

Open a Ruby interactive interpreter and import the FoundationDB API module::

    $ irb
    > require 'fdb'
    => true

Before using the API, we need to specify the API version. This allows programs to maintain compatibility even if the API is modified in future versions::

    > FDB.api_version 620
    => nil

Next, we open a FoundationDB database.  The API will connect to the FoundationDB cluster indicated by the :ref:`default cluster file <default-cluster-file>`. ::

    > @db = FDB.open
    => #<FDB::Database:0x007fc2309751e0 @dpointer=#<FFI::Pointer address=0x007fc231c139c0>, @options=#<FDB::DatabaseOptions:0x007fc230975168 @setfunc=#<Proc:0x007fc230975190@/Users/someone/.rvm/gems/ruby-2.0.0-p247/gems/fdb-1.0.0/lib/fdbimpl.rb:510 (lambda)>>>

We are ready to use the database. In Ruby, using the ``[]`` operator on the database object is a convenient syntax for performing a read or write on the database. First, let's simply write a key-value pair::

    > @db['hello'] = 'world'
    => "world"

When this command returns without exception, the modification is durably stored in FoundationDB! Under the covers, this function creates a transaction with a single modification. We'll see later how to do multiple operations in a single transaction. For now, let's read back the data::

    >>> print 'hello ', @db['hello']
    hello world => nil

If this is all working, it looks like we are ready to start building a real application. For reference, here's the full code for "hello world":

.. code-block:: ruby

    require 'fdb'
    FDB.api_version 620
    @db = FDB.open
    @db['hello'] = 'world'
    print 'hello ', @db['hello']

Class scheduling application
============================

Let's say we've been asked to build a class scheduling system for students and administrators. We'll walk through the design and implementation of this application. Instead of typing everything in as you follow along, look at the :ref:`class-sched-ruby-appendix` for a finished version of the program. You may want to refer to this code as we walk through the tutorial.

Requirements
------------

We'll need to let users list available classes and track which students have signed up for which classes. Here's a first cut at the functions we'll need to implement::

    available_classes()      # returns list of classes
    signup(studentID, class) # signs up a student for a class
    drop(studentID, class)   # drops a student from a class

.. _class-sched-ruby-data-model:

Data model
----------

First, we need to design a :doc:`data model <data-modeling>`. A data model is just a method for storing our application data using keys and values in FoundationDB. We seem to have two main types of data: (1) a list of classes and (2) a record of which students will attend which classes. Let's keep attending data like this::

    # ['attends', student, class] = ''

We'll just store the key with a blank value to indicate that a student is signed up for a particular class. For this application, we're going to think about a key-value pair's key as a :ref:`tuple <data-modeling-tuples>`. Encoding a tuple of data elements into a key is a very common pattern for an ordered key-value store.

We'll keep data about classes like this::

    # ['class', class_name] = seats_available

Similarly, each such key will represent an available class. We'll use ``seats_available`` to record the number of seats available.

Transactions
------------

We're going to rely on the powerful guarantees of transactions to help keep all of our modifications straight, so let's look at a nice way that the FoundationDB Ruby API lets you write a transactional function. The :meth:`transact` method ensures that a code block is executed transactionally. Let's write the very simple ``add_class`` function we will use to populate the database's class list:

.. code-block:: ruby

    def add_class(db_or_tr, c)
        db_or_tr.transact do |tr|
            tr[FDB::Tuple.pack(['class',c])] = '100'
        end
    end

A function using this approach has a parameter taking either a :class:`Database` or :class:`Transaction` on which it calls the :meth:`transact` method. The block passed to :meth:`transact` is parameterized by the transaction the function will use to do reads and writes.

When *calling* such a function, however, you can pass a :class:`Database` instead of a :class:`Transaction`. The method *automatically creates a transaction and implements a retry loop* to ensure that the transaction eventually commits.

For a FoundationDB database ``@db``:

.. code-block:: ruby

    add_class(@db, 'class1')

is equivalent to something like:

.. code-block:: ruby

    tr = @db.create_transaction
    committed = false
    while !committed
        begin
            tr[FDB::Tuple.pack(['class',c])] = '100'
            tr.commit.wait
            committed = true
        rescue FDB::Error => e
            tr.on_error(e).wait
        end
    end

If instead you pass a :class:`Transaction` for the ``db_or_tr`` parameter, the transaction will be used directly, and it is assumed that the caller implements appropriate retry logic for errors. This permits functions using this pattern to be composed into larger transactions.

Note that by default, the operation will be retried an infinite number of times and the transaction will never time out. It is therefore recommended that the client choose a default transaction retry limit or timeout value that is suitable for their application. This can be set either at the transaction level using the ``set_retry_limit`` or ``set_timeout`` transaction options or at the database level with the ``set_transaction_retry_limit`` or ``set_transaction_timeout`` database options. For example, one can set a one minute timeout on each transaction and a default retry limit of 100 by calling::

    @db.options.set_transaction_timeout(60000)  # 60,000 ms = 1 minute
    @db.options.set_transaction_retry_limit(100)

Making some sample classes
--------------------------

Let's make some sample classes and put them in the ``@class_names`` variable. We'll make individual classes from combinations of class types, levels, and times:

.. code-block:: ruby

    # Generate 1,620 classes like '9:00 chem for dummies'
    levels = ['intro', 'for dummies', 'remedial', '101',
              '201', '301', 'mastery', 'lab', 'seminar']
    types = ['chem', 'bio', 'cs', 'geometry', 'calc',
             'alg', 'film', 'music', 'art', 'dance']
    times = Array(2...20).map {|h| h.to_s.encode('UTF-8') + ':00'}
    class_combos = times.product(types, levels)
    @class_names = class_combos.map {|combo| combo.join(' ')}

Initializing the database
-------------------------
We initialize the database with our class list:

.. code-block:: ruby

    def init(db_or_tr)
        db_or_tr.transact do |tr|
            tr.clear_range_start_with(FDB::Tuple.pack(['attends']))
            tr.clear_range_start_with(FDB::Tuple.pack(['class']))
            @class_names.each do |class_name|
                add_class(tr, class_name)
            end
        end
    end

After :meth:`init` is run, the database will contain all of the sample classes we created above.

Listing available classes
-------------------------

Before students can do anything else, they need to be able to retrieve a list of available classes from the database. Because FoundationDB sorts its data by key and therefore has efficient range-read capability, we can retrieve all of the classes in a single database call. We find this range of keys with :meth:`get_range`:

.. code-block:: ruby

    def available_classes(db_or_tr)
        db_or_tr.transact do |tr|
            r = FDB::Tuple.range(['class'])
            tr.get_range(r[0], r[1]) {|kv| FDB::Tuple.unpack(kv.key)[1]}
        end
    end

In general, the :meth:`FDB::Tuple.range` method returns an Array of two elements representing the begin and end of the range of all the key-value pairs starting with the specified tuple. In this case, we want all classes, so we call :meth:`FDB::Tuple.range` with the tuple ``['class']``. :meth:`get_range` returns an enumerable of the key-values specified by its range. To extract the class name, we unpack the key into a tuple using :meth:`FDB::Tuple.unpack` and take its second part. (The first part is the prefix ``'class'``.)

Signing up for a class
----------------------

We finally get to the crucial function. A student has decided on a class (by name) and wants to sign up. The ``signup`` function will take a student (``s``) and a class (``c``):

.. code-block:: ruby

    def signup(db_or_tr, s, c)
        db_or_tr.transact do |tr|
            rec = FDB::Tuple.pack(['attends', s, c])
            tr[rec] = ''
        end
    end

We simply insert the appropriate record (with a blank value).

Dropping a class
----------------

Dropping a class is similar to signing up:

.. code-block:: ruby

    def drop(db_or_tr, s, c)
        db_or_tr.transact do |tr|
            rec = FDB::Tuple.pack(['attends', s, c])
            tr.clear(rec)
        end
    end

Of course, to actually drop the student from the class, we need to be able to delete a record from the database.  We do this with the :meth:`clear` method.

Done?
-----

We report back to the project leader that our application is done---students can sign up for, drop, and list classes. Unfortunately, we learn that a new problem has been discovered: popular classes are being over-subscribed. Our application now needs to enforce the class size constraint as students add and drop classes.

Seats are limited!
------------------

Let's go back to the data model. Remember that we stored the number of seats in the class in the value of the key-value entry in the class list. Let's refine that a bit to track the *remaining* number of seats in the class. The initialization can work the same way (in our example, all classes initially have 100 seats), but the ``available_classes``, ``signup``, and ``drop`` functions are going to have to change. Let's start with ``available_classes``:

.. code-block:: ruby

    def available_classes(db_or_tr)
        db_or_tr.transact do |tr|
            r = FDB::Tuple.range(['class'])
            tr.get_range(r[0], r[1]) {|kv| FDB::Tuple.unpack(kv.key)[1] if kv.value.to_i > 0}
        end
    end

This is easy -- we simply add a condition to check that the value is non-zero. Let's check out ``signup`` next:

.. code-block:: ruby
    :emphasize-lines: 4-13

    def signup(db_or_tr, s, c)
        db_or_tr.transact do |tr|
            rec = FDB::Tuple.pack(['attends', s, c])
            if not tr[rec].nil?
                return # already signed up
            end

            seats_left = tr[FDB::Tuple.pack(['class', c])].to_i
            if seats_left == 0
                raise 'No remaining seats'
            end

            tr[FDB::Tuple.pack(['class',c])] = (seats_left - 1).to_s.encode('UTF-8')
            tr[rec] = ''
        end
    end

We now have to check that we aren't already signed up, since we don't want a double sign up to decrease the number of seats twice. Then we look up how many seats are left to make sure there is a seat remaining so we don't push the counter into the negative. If there is a seat remaining, we decrement the counter.


Concurrency and consistency
---------------------------

The ``signup`` function is starting to get a bit complex; it now reads and writes a few different key-value pairs in the database. One of the tricky issues in this situation is what happens as multiple clients/students read and modify the database at the same time. Couldn't two students both see one remaining seat and sign up at the same time?

These are tricky issues without simple answers---unless you have transactions! Because these functions are defined as FoundationDB transactions, we can have a simple answer: Each transactional function behaves as if it is the only one modifying the database. There is no way for a transaction to 'see' another transaction change the database, and each transaction ensures that either all of its modifications occur or none of them do.

Looking deeper, it is, of course, possible for two transactions to conflict. For example, if two people both see a class with one seat and sign up at the same time, FoundationDB must allow only one to succeed. This causes one of the transactions to fail to commit (which can also be caused by network outages, crashes, etc.). To ensure correct operation, applications need to handle this situation, usually via retrying the transaction. In this case, the conflicting transaction will be retried automatically by the :meth:`transact` method and will eventually lead to the correct result, a 'No remaining seats' exception.

Idempotence
-----------

Occasionally, a transaction might be retried even after it succeeds (for example, if the client loses contact with the cluster at just the wrong moment). This can cause problems if transactions are not written to be idempotent, i.e. to have the same effect if committed twice as if committed once. There are generic design patterns for :ref:`making any transaction idempotent <developer-guide-unknown-results>`, but many transactions are naturally idempotent. For example, all of the transactions in this tutorial are idempotent.

Dropping with limited seats
---------------------------

Let's finish up the limited seats feature by modifying the drop function:

.. code-block:: ruby
    :emphasize-lines: 4-8

    def drop(db_or_tr, s, c)
        db_or_tr.transact do |tr|
            rec = FDB::Tuple.pack(['attends', s, c])
            if tr[rec].nil?
                return  # not taking this class
            end
            class_key = FDB::Tuple.pack(['class',c])
            tr[class_key] = (tr[class_key].to_i + 1).to_s.encode('UTF-8')
            tr.clear(rec)
        end
    end

This case is easier than signup because there are no constraints we can hit. We just need to make sure the student is in the class and to "give back" one seat when the student drops.

More features?!
---------------

Of course, as soon as our new version of the system goes live, we hear of a trick that certain students are using. They are signing up for all classes immediately, and only later dropping those that they don't want to take. This has led to an unusable system, and we have been asked to fix it. We decide to limit students to five classes:

.. code-block:: ruby
    :emphasize-lines: 13-17

    def signup(db_or_tr, s, c)
        db_or_tr.transact do |tr|
            rec = FDB::Tuple.pack(['attends', s, c])
            if not tr[rec].nil?
                return # already signed up
            end

            seats_left = tr[FDB::Tuple.pack(['class', c])].to_i
            if seats_left == 0
                raise 'No remaining seats'
            end

            r = FDB::Tuple.range(['attends', s])
            classes = tr.get_range(r[0], r[1])
            if classes.count == 5
                raise 'Too many classes'
            end

            tr[FDB::Tuple.pack(['class',c])] = (seats_left - 1).to_s.encode('UTF-8')
            tr[rec] = ''
        end
    end

Fortunately, we decided on a data model that keeps all of the attending records for a single student together. With this approach, we can use a single range read to retrieve all the classes that a student attends. We simply throw an exception if the number of classes has reached the limit of five.

Composing transactions
----------------------

Oh, just one last feature, we're told. We have students that are trying to switch from one popular class to another. By the time they drop one class to free up a slot for themselves, the open slot in the other class is gone. By the time they see this and try to re-add their old class, that slot is gone too! So, can we make it so that a student can switch from one class to another without this worry?

Fortunately, we have FoundationDB, and this sounds an awful lot like the transactional property of atomicity---the all-or-nothing behavior that we already rely on. All we need to do is to *compose* the ``drop`` and ``signup`` functions into a new ``switch`` function. This makes the ``switch`` function exceptionally easy:

.. code-block:: ruby

    def switch(db_or_tr, s, old_c, new_c)
        db_or_tr.transact do |tr|
            drop(tr, s, old_c)
            signup(tr, s, new_c)
        end
    end

The simplicity of this implementation belies the sophistication of what FoundationDB is taking care of for us.

By dropping the old class and signing up for the new one inside a single transaction, we ensure that either both steps happen, or that neither happens. The first notable thing about the ``switch`` function is that it is transactional, but it also calls the transactional functions ``signup`` and ``drop``. Because these transactional functions can accept either a database or an existing transaction as the ``tr`` argument, the ``switch`` function can be called with a database by a simple client, and a new transaction will be automatically created. However, once this transaction is created and passed in as ``tr``, the calls to ``drop`` and ``signup`` both share the same ``tr``. This ensures that they see each other's modifications to the database, and all of the changes that both of them make in sequence are made transactionally when the ``switch`` function returns. This compositional capability is very powerful.

Also note that, if an exception is raised, for example, in ``signup``, the exception is not caught by ``switch`` and so will be thrown to the calling function. In this case, the transaction object (owned by the :meth:`transact` method) is destroyed, automatically rolling back all database modifications, leaving the database completely unchanged by the half-executed function.

Are we done?
------------

Yep, we’re done and ready to deploy. If you want to see this entire application in one place plus some multithreaded testing code to simulate concurrency, look at the :ref:`class-sched-ruby-appendix`, below.

Deploying and scaling
---------------------

Since we store all state for this application in FoundationDB, deploying and scaling this solution up is impressively painless. Just run a web server, the UI, this back end, and point the whole thing at FoundationDB. We can run as many computers with this setup as we want, and they can all hit the database at the same time because of the transactional integrity of FoundationDB. Also, since all of the state in the system is stored in the database, any of these computers can fail without any lasting consequences.

Next steps
==========

* See :doc:`data-modeling` for guidance on using tuple and subspaces to enable effective storage and retrieval of data.
* See :doc:`developer-guide` for general guidance on development using FoundationDB.
* See the :doc:`API References <api-reference>` for detailed API documentation.

.. _class-sched-ruby-appendix:

Appendix: class_scheduling.rb
===============================

Here's the code for the scheduling tutorial:

.. code-block:: ruby

    require 'fdb'

    FDB.api_version 620

    ####################################
    ##        Initialization          ##
    ####################################

    # Data model:
    # ['attends', student, class] = ''
    # ['class', class_name] = seats_left

    @db = FDB.open
    @db.options.set_transaction_timeout(60000)  # 60,000 ms = 1 minute
    @db.options.set_transaction_retry_limit(100)

    def add_class(db_or_tr, c)
        db_or_tr.transact do |tr|
            tr[FDB::Tuple.pack(['class',c])] = '100'
        end
    end

    # Generate 1,620 classes like '9:00 chem for dummies'
    levels = ['intro', 'for dummies', 'remedial', '101',
              '201', '301', 'mastery', 'lab', 'seminar']
    types = ['chem', 'bio', 'cs', 'geometry', 'calc',
             'alg', 'film', 'music', 'art', 'dance']
    times = Array(2...20).map {|h| h.to_s.encode('UTF-8') + ':00'}
    class_combos = times.product(types, levels)
    @class_names = class_combos.map {|combo| combo.join(' ')}

    def init(db_or_tr)
        db_or_tr.transact do |tr|
            tr.clear_range_start_with(FDB::Tuple.pack(['attends']))
            tr.clear_range_start_with(FDB::Tuple.pack(['class']))
            @class_names.each do |class_name|
                add_class(tr, class_name)
            end
        end
    end

    def available_classes(db_or_tr)
        db_or_tr.transact do |tr|
            r = FDB::Tuple.range(['class'])
            tr.get_range(r[0], r[1]) {|kv| FDB::Tuple.unpack(kv.key)[1] if kv.value.to_i > 0}
        end
    end

    def signup(db_or_tr, s, c)
        db_or_tr.transact do |tr|
            rec = FDB::Tuple.pack(['attends', s, c])
            if not tr[rec].nil?
                return # already signed up
            end

            seats_left = tr[FDB::Tuple.pack(['class', c])].to_i
            if seats_left == 0
                raise 'No remaining seats'
            end

            r = FDB::Tuple.range(['attends', s])
            classes = tr.get_range(r[0], r[1])
            if classes.count == 5
                raise 'Too many classes'
            end

            tr[FDB::Tuple.pack(['class',c])] = (seats_left - 1).to_s.encode('UTF-8')
            tr[rec] = ''
        end
    end

    def drop(db_or_tr, s, c)
        db_or_tr.transact do |tr|
            rec = FDB::Tuple.pack(['attends', s, c])
            if tr[rec].nil?
                return  # not taking this class
            end
            class_key = FDB::Tuple.pack(['class',c])
            tr[class_key] = (tr[class_key].to_i + 1).to_s.encode('UTF-8')
            tr.clear(rec)
        end
    end

    def switch(db_or_tr, s, old_c, new_c)
        db_or_tr.transact do |tr|
            drop(tr, s, old_c)
            signup(tr, s, new_c)
        end
    end

    ####################################
    ##           Testing              ##
    ####################################

    def indecisive_student(i, ops)
        student_ID = "s%d" % i
        all_classes = @class_names
        my_classes = []

        Array(0...ops).each do |i|
            class_count = my_classes.length
            moods = []
            if class_count > 0
                moods.push('drop','switch')
            end
            if class_count < 5
                moods.push('add')
            end
            mood = moods.sample

            begin
                if all_classes.empty?
                    all_classes = available_classes(@db)
                end
                if mood == 'add'
                    c = all_classes.sample
                    signup(@db, student_ID, c)
                    my_classes.push(c)
                elsif mood == 'drop'
                    c = my_classes.sample
                    drop(@db, student_ID, c)
                    my_classes.delete(c)
                elsif mood == 'switch'
                    old_c = my_classes.sample
                    new_c = all_classes.sample
                    switch(@db, student_ID, old_c, new_c)
                    my_classes.delete(old_c)
                    my_classes.push(new_c)
                end
            rescue => e
                print e, "Need to recheck available classes."
                all_classes = []
            end
        end
    end

    def run(students, ops_per_student)
        threads = Array(0...students).map {|i| Thread.new(i, ops_per_student) {
                                           indecisive_student(i, ops_per_student)}
                                        }
        threads.each {|thr| thr.join}
        print "Ran %d transactions" % (students * ops_per_student)
    end

    if __FILE__ == $0
        init(@db)
        print "initialized"
        run(10, 10)
    end

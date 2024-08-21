################
Class Scheduling
################

This tutorial provides a walkthrough of designing and building a simple application in Python using FoundationDB. In this tutorial, we use a few simple data modeling techniques. For a more in-depth discussion of data modeling in FoundationDB, see :doc:`data-modeling`.

The concepts in this tutorial are applicable to all the :doc:`languages <api-reference>` supported by FoundationDB. If you prefer, you can see a version of this tutorial in:

.. toctree::
   :maxdepth: 2
   :titlesonly:

   Ruby <class-scheduling-ruby>
   Java <class-scheduling-java>
   Go <class-scheduling-go>

.. _tutorial-first-steps:

First steps
===========

Let's begin with "Hello world."

If you have not yet installed FoundationDB, see :doc:`getting-started-mac` or :doc:`getting-started-linux`.

Open a Python interactive interpreter and import the FoundationDB API module::

    $ python
    >>> import fdb

Before using the API, we need to specify the API version. This allows programs to maintain compatibility even if the API is modified in future versions::

    >>> fdb.api_version(730)

Next, we open a FoundationDB database.  The API will connect to the FoundationDB cluster indicated by the :ref:`default cluster file <default-cluster-file>`. ::

    >>> db = fdb.open()

We are ready to use the database. In Python, using the ``[]`` operator on the db object is a convenient syntax for performing a read or write on the database. First, let's simply write a key-value pair:

    >>> db[b'hello'] = b'world'

When this command returns without exception, the modification is durably stored in FoundationDB! Under the covers, this function creates a transaction with a single modification. We'll see later how to do multiple operations in a single transaction. For now, let's read back the data::

    >>> print 'hello', db[b'hello']
    hello world

If this is all working, it looks like we are ready to start building a real application. For reference, here's the full code for "hello world"::

    import fdb
    fdb.api_version(730)
    db = fdb.open()
    db[b'hello'] = b'world'
    print 'hello', db[b'hello']

Class scheduling application
============================

Let's say we've been asked to build a class scheduling system for students and administrators. We'll walk through the design and implementation of this application. Instead of typing everything in as you follow along, look at the :ref:`tutorial-appendix` for a finished version of the program. You may want to refer to this code as we walk through the tutorial.

Requirements
------------

We'll need to let users list available classes and track which students have signed up for which classes. Here's a first cut at the functions we'll need to implement::

    available_classes()      # returns list of classes
    signup(studentID, class) # signs up a student for a class
    drop(studentID, class)   # drops a student from a class

.. _tutorial-data-model:

Data model
----------

First, we need to design a :doc:`data model <data-modeling>`. A data model is just a method for storing our application data using keys and values in FoundationDB. We seem to have two main types of data: (1) a list of classes and (2) a record of which students will attend which classes. Let's keep attending data like this::

    # ('attends', student, class) = ''

We'll just store the key with a blank value to indicate that a student is signed up for a particular class. For this application, we're going to think about a key-value pair's key as a :ref:`tuple <data-modeling-tuples>`. Encoding a tuple of data elements into a key is a very common pattern for an ordered key-value store.

We'll keep data about classes like this::

    # ('class', class_name) = seats_available

Similarly, each such key will represent an available class. We'll use ``seats_available`` to record the number of seats available.

Directories and Subspaces
-------------------------

FoundationDB includes a few tools that make it easy to model data using this approach. Let's begin by
opening a :ref:`directory <developer-guide-directories>` in the database::

    import fdb
    fdb.api_version(730)

    db = fdb.open()
    scheduling = fdb.directory.create_or_open(db, ('scheduling',))

The :meth:`create_or_open` method returns a :ref:`subspace <developer-guide-sub-keyspaces>` where we'll store our application data. Each subspace has a fixed prefix it uses when defining keys. The prefix corresponds to the first element of a tuple. We decided that we wanted ``'attends'`` and  ``'class'`` as our prefixes, so we'll create new subspaces for them within the ``scheduling`` subspace.::

    course = scheduling['class']
    attends = scheduling['attends']

Subspaces have a :meth:`pack` method for defining keys. To store the records for our data model, we can use ``attends.pack((s, c))`` and ``course.pack((c,))``.

Transactions
------------

We're going to rely on the powerful guarantees of transactions to help keep all of our modifications straight, so let's look at a nice way that the FoundationDB Python API lets you write a transactional function. By using a decorator, an entire function is wrapped in a transaction. Let's write the very simple ``add_class`` function we will use to populate the database's class list::

    @fdb.transactional
    def add_class(tr, c):
        tr[course.pack((c,))] = fdb.tuple.pack((100,))

:py:func:`@fdb.transactional <fdb.transactional>` is a Python decorator that makes a normal function a transactional function. All functions decorated this way *need to have a parameter named* ``tr``. This parameter is passed the transaction that the function should use to do reads and writes.

When *calling* a transactionally decorated function, however, you can pass a database instead of a transaction for the ``tr`` parameter. The decorator *automatically creates a transaction and implements a retry loop* to ensure that the transaction eventually commits.

For a FoundationDB database ``db``::

    add_class(db, 'class1')

is equivalent to something like::

    tr = db.create_transaction()
    while True:
        try:
            add_class(tr, 'class1')
            tr.commit().wait()
            break
        except fdb.FDBError as e:
            tr.on_error(e).wait()

If instead you pass a :class:`Transaction` for the ``tr`` parameter, the transaction will be used directly, and it is assumed that the caller implements appropriate retry logic for errors. This permits transactionally decorated functions to be composed into larger transactions.

Note that by default, the operation will be retried an infinite number of times and the transaction will never time out. It is therefore recommended that the client choose a default transaction retry limit or timeout value that is suitable for their application. This can be set either at the transaction level using the ``set_retry_limit`` or ``set_timeout`` transaction options or at the database level with the ``set_transaction_retry_limit`` or ``set_transaction_timeout`` database options. For example, one can set a one minute timeout on each transaction and a default retry limit of 100 by calling::

    db.options.set_transaction_timeout(60000)  # 60,000 ms = 1 minute
    db.options.set_transaction_retry_limit(100)

Making some sample classes
--------------------------

Let's make some sample classes and put them in the ``class_names`` variable. The Python ``itertools`` module is used to make individual classes from combinations of class types, levels, and times::

    import itertools

    # Generate 1,620 classes like '9:00 chem for dummies'
    levels = ['intro', 'for dummies', 'remedial', '101',
              '201', '301', 'mastery', 'lab', 'seminar']
    types = ['chem', 'bio', 'cs', 'geometry', 'calc',
             'alg', 'film', 'music', 'art', 'dance']
    times = [str(h) + ':00' for h in range(2, 20)]
    class_combos = itertools.product(times, types, levels)
    class_names = [' '.join(tup) for tup in class_combos]

Initializing the database
-------------------------
We initialize the database with our class list::

    @fdb.transactional
    def init(tr):
        del tr[scheduling.range(())]  # Clear the directory
        for class_name in class_names:
            add_class(tr, class_name)

After :func:`init` is run, the database will contain all of the sample classes we created above.

Listing available classes
-------------------------

Before students can do anything else, they need to be able to retrieve a list of available classes from the database. Because FoundationDB sorts its data by key and therefore has efficient range-read capability, we can retrieve all of the classes in a single database call. We find this range of keys with :meth:`course.range`.::

    @fdb.transactional
    def available_classes(tr):
        return [course.unpack(k)[0] for k, v in tr[course.range(())]]

In general, the :meth:`Subspace.range` method returns a Python ``slice`` representing all the key-value pairs starting with the specified tuple. In this case, we want all classes, so we call :meth:`course.range` with the empty tuple ``()``. FoundationDB's ``tr[slice]`` function returns an iterable list of key-values in the range specified by the slice. We unpack the key ``k`` and value ``v`` in a comprehension. To extract the class name itself, we unpack the key into a tuple using the :meth:`Subspace.unpack` method and take the first field. (The first and second parts of the tuple, the ``scheduling`` and ``course`` subspace prefixes, are removed by the ``unpack`` hence the reason we take the first field of the tuple.)

Signing up for a class
----------------------

We finally get to the crucial function. A student has decided on a class (by name) and wants to sign up. The ``signup`` function will take a student (``s``) and a class (``c``)::

    @fdb.transactional
    def signup(tr, s, c):
        rec = attends.pack((s, c))
        tr[rec] = b''

We simply insert the appropriate record (with a blank value).

Dropping a class
----------------

Dropping a class is similar to signing up::

    @fdb.transactional
    def drop(tr, s, c):
        rec = attends.pack((s, c))
        del tr[rec]

Of course, to actually drop the student from the class, we need to be able to delete a record from the database.  We do this with the ``del tr[key]`` syntax.

Done?
-----

We report back to the project leader that our application is done---students can sign up for, drop, and list classes. Unfortunately, we learn that a new problem has been discovered: popular classes are being over-subscribed. Our application now needs to enforce the class size constraint as students add and drop classes.

Seats are limited!
------------------

Let's go back to the data model. Remember that we stored the number of seats in the class in the value of the key-value entry in the class list. Let's refine that a bit to track the *remaining* number of seats in the class. The initialization can work the same way. (In our example, all classes initially have 100 seats), but the ``available_classes``, ``signup``, and ``drop`` functions are going to have to change. Let's start with ``available_classes``:

.. code-block:: python
    :emphasize-lines: 4

    @fdb.transactional
    def available_classes(tr):
        return [course.unpack(k)[0] for k, v in tr[course.range(())]
                if fdb.tuple.unpack(v)[0]]

This is easy -- we simply add a condition to check that the value is non-zero. Let's check out ``signup`` next:

.. code-block:: python
    :emphasize-lines: 4,5,6,7,8,9

    @fdb.transactional
    def signup(tr, s, c):
        rec = attends.pack((s, c))
        if tr[rec].present(): return  # already signed up

        seats_left = fdb.tuple.unpack(tr[course.pack((c,))])[0]
        if not seats_left: raise Exception('No remaining seats')

        tr[course.pack((c,))] = fdb.tuple.pack((seats_left - 1,))
        tr[rec] = b''

We now have to check that we aren't already signed up, since we don't want a double sign up to decrease the number of seats twice. Then we look up how many seats are left to make sure there is a seat remaining so we don't push the counter into the negative. If there is a seat remaining, we decrement the counter.

Similarly, the ``drop`` function is modified as follows:

.. code-block:: python
    :emphasize-lines: 4,5

    @fdb.transactional
    def drop(tr, s, c):
        rec = attends.pack((s, c))
        if not tr[rec].present(): return  # not taking this class
        tr[course.pack((c,))] = fdb.tuple.pack((fdb.tuple.unpack(tr[course.pack((c,))])[0] + 1,))
        del tr[rec]

Once again we check to see if the student is signed up and if not, we can just return as we don't want to incorrectly increase the number of seats. We then adjust the number of seats by one by taking the current value, incrementing it by one, and then storing back.

Concurrency and consistency
---------------------------

The ``signup`` function is starting to get a bit complex; it now reads and writes a few different key-value pairs in the database. One of the tricky issues in this situation is what happens as multiple clients/students read and modify the database at the same time. Couldn't two students both see one remaining seat and sign up at the same time?

These are tricky issues without simple answers---unless you have transactions! Because these functions are defined as FoundationDB transactions, we can have a simple answer: Each transactional function behaves as if it is the only one modifying the database. There is no way for a transaction to 'see' another transaction change the database, and each transaction ensures that either all of its modifications occur or none of them do.

Looking deeper, it is, of course, possible for two transactions to conflict. For example, if two people both see a class with one seat and sign up at the same time, FoundationDB must allow only one to succeed. This causes one of the transactions to fail to commit (which can also be caused by network outages, crashes, etc.). To ensure correct operation, applications need to handle this situation, usually via retrying the transaction. In this case, the conflicting transaction will be retried automatically by the ``@fdb.transactional`` decorator and will eventually lead to the correct result, a 'No remaining seats' exception.

Idempotence
-----------

Occasionally, a transaction might be retried even after it succeeds (for example, if the client loses contact with the cluster at just the wrong moment). This can cause problems if transactions are not written to be idempotent, i.e. to have the same effect if committed twice as if committed once. There are generic design patterns for :ref:`making any transaction idempotent <developer-guide-unknown-results>`, but many transactions are naturally idempotent. For example, all of the transactions in this tutorial are idempotent.

More features?!
---------------

Of course, as soon as our new version of the system goes live, we hear of a trick that certain students are using. They are signing up for all classes immediately, and only later dropping those that they don't want to take. This has led to an unusable system, and we have been asked to fix it. We decide to limit students to five classes:

.. code-block:: python
    :emphasize-lines: 9,10

    @fdb.transactional
    def signup(tr, s, c):
        rec = attends.pack((s, c))
        if tr[rec].present(): return  # already signed up

        seats_left = fdb.tuple.unpack(tr[course.pack((c,))])[0]
        if not seats_left: raise Exception('No remaining seats')

        classes = tr[attends.range((s,))]
        if len(list(classes)) == 5: raise Exception('Too many classes')

        tr[course.pack((c,))] = fdb.tuple.pack((seats_left - 1,))
        tr[rec] = b''

Fortunately, we decided on a data model that keeps all of the attending records for a single student together. With this approach, we can use a single range read in the ``attends`` subspace to retrieve all the classes that a student is signed up for. We simply throw an exception if the number of classes has reached the limit of five.

Composing transactions
----------------------

Oh, just one last feature, we're told. We have students that are trying to switch from one popular class to another. By the time they drop one class to free up a slot for themselves, the open slot in the other class is gone. By the time they see this and try to re-add their old class, that slot is gone too! So, can we make it so that a student can switch from one class to another without this worry?

Fortunately, we have FoundationDB, and this sounds an awful lot like the transactional property of atomicity---the all-or-nothing behavior that we already rely on. All we need to do is to *compose* the ``drop`` and ``signup`` functions into a new ``switch`` function. This makes the ``switch`` function exceptionally easy::

    @fdb.transactional
    def switch(tr, s, old_c, new_c):
        drop(tr, s, old_c)
        signup(tr, s, new_c)

The simplicity of this implementation belies the sophistication of what FoundationDB is taking care of for us.

By dropping the old class and signing up for the new one inside a single transaction, we ensure that either both steps happen, or that neither happens. The first notable thing about the ``switch`` function is that it is transactionally decorated, but it also calls the transactionally decorated functions ``signup`` and ``drop``. Because these decorated functions can accept either a database or an existing transaction as the ``tr`` argument, the switch function can be called with a database by a simple client, and a new transaction will be automatically created. However, once this transaction is created and passed in as ``tr``, the calls to ``drop`` and ``signup`` both share the same ``tr``. This ensures that they see each other's modifications to the database, and all of the changes that both of them make in sequence are made transactionally when the switch function returns. This compositional capability is very powerful.

Also note that, if an exception is raised, for example, in ``signup``, the exception is not caught by ``switch`` and so will be thrown to the calling function. In this case, the transaction object (owned by the decorator) is destroyed, automatically rolling back all database modifications, leaving the database completely unchanged by the half-executed function.

Are we done?
------------

Yep, we’re done and ready to deploy. If you want to see this entire application in one place plus some multithreaded testing code to simulate concurrency, look at the :ref:`tutorial-appendix`, below.

Deploying and scaling
---------------------

Since we store all state for this application in FoundationDB, deploying and scaling this solution up is impressively painless. Just run a web server, the UI, this back end, and point the whole thing at FoundationDB. We can run as many computers with this setup as we want, and they can all hit the database at the same time because of the transactional integrity of FoundationDB. Also, since all of the state in the system is stored in the database, any of these computers can fail without any lasting consequences.

Next steps
==========

* See :doc:`data-modeling` for guidance on using tuple and subspaces to enable effective storage and retrieval of data.
* See :doc:`developer-guide` for general guidance on development using FoundationDB.
* See the :doc:`API References <api-reference>` for detailed API documentation.

.. _tutorial-appendix:

Appendix: SchedulingTutorial.py
===============================

Here's the code for the scheduling tutorial::

    import itertools
    import traceback

    import fdb
    import fdb.tuple

    fdb.api_version(730)


    ####################################
    ##        Initialization          ##
    ####################################

    # Data model:
    # ('attends', student, class) = ''
    # ('class', class_name) = seats_left

    db = fdb.open()
    db.options.set_transaction_timeout(60000)  # 60,000 ms = 1 minute
    db.options.set_transaction_retry_limit(100)
    scheduling = fdb.directory.create_or_open(db, ('scheduling',))
    course = scheduling['class']
    attends = scheduling['attends']

    @fdb.transactional
    def add_class(tr, c):
        tr[course.pack((c,))] = fdb.tuple.pack((100,))

    # Generate 1,620 classes like '9:00 chem for dummies'
    levels = ['intro', 'for dummies', 'remedial', '101',
              '201', '301', 'mastery', 'lab', 'seminar']
    types = ['chem', 'bio', 'cs', 'geometry', 'calc',
             'alg', 'film', 'music', 'art', 'dance']
    times = [str(h) + ':00' for h in range(2, 20)]
    class_combos = itertools.product(times, types, levels)
    class_names = [' '.join(tup) for tup in class_combos]

    @fdb.transactional
    def init(tr):
        del tr[scheduling.range(())]  # Clear the directory
        for class_name in class_names:
            add_class(tr, class_name)


    ####################################
    ##  Class Scheduling Functions    ##
    ####################################


    @fdb.transactional
    def available_classes(tr):
        return [course.unpack(k)[0] for k, v in tr[course.range(())]
                if fdb.tuple.unpack(v)[0]]


    @fdb.transactional
    def signup(tr, s, c):
        rec = attends.pack((s, c))
        if tr[rec].present(): return  # already signed up

        seats_left = fdb.tuple.unpack(tr[course.pack((c,))])[0]
        if not seats_left: raise Exception('No remaining seats')

        classes = tr[attends.range((s,))]
        if len(list(classes)) == 5: raise Exception('Too many classes')

        tr[course.pack((c,))] = fdb.tuple.pack((seats_left - 1,))
        tr[rec] = b''


    @fdb.transactional
    def drop(tr, s, c):
        rec = attends.pack((s, c))
        if not tr[rec].present(): return  # not taking this class
        tr[course.pack((c,))] = fdb.tuple.pack((fdb.tuple.unpack(tr[course.pack((c,))])[0] + 1,))
        del tr[rec]


    @fdb.transactional
    def switch(tr, s, old_c, new_c):
        drop(tr, s, old_c)
        signup(tr, s, new_c)

    ####################################
    ##           Testing              ##
    ####################################

    import random
    import threading

    def indecisive_student(i, ops):
        student_ID = 's{:d}'.format(i)
        all_classes = class_names
        my_classes = []

        for i in range(ops):
            class_count = len(my_classes)
            moods = []
            if class_count: moods.extend(['drop', 'switch'])
            if class_count < 5: moods.append('add')
            mood = random.choice(moods)

            try:
                if not all_classes:
                    all_classes = available_classes(db)
                if mood == 'add':
                    c = random.choice(all_classes)
                    signup(db, student_ID, c)
                    my_classes.append(c)
                elif mood == 'drop':
                    c = random.choice(my_classes)
                    drop(db, student_ID, c)
                    my_classes.remove(c)
                elif mood == 'switch':
                    old_c = random.choice(my_classes)
                    new_c = random.choice(all_classes)
                    switch(db, student_ID, old_c, new_c)
                    my_classes.remove(old_c)
                    my_classes.append(new_c)
            except Exception as e:
                traceback.print_exc()
                print("Need to recheck available classes.")
                all_classes = []

    def run(students, ops_per_student):
        threads = [
            threading.Thread(target=indecisive_student, args=(i, ops_per_student))
            for i in range(students)]
        for thr in threads: thr.start()
        for thr in threads: thr.join()
        print("Ran {} transactions".format(students * ops_per_student))

    if __name__ == "__main__":
        init(db)
        print("initialized")
        run(10, 10)

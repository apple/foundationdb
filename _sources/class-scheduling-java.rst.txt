########################
Class Scheduling in Java
########################

This tutorial provides a walkthrough of designing and building a simple application in Java using FoundationDB. In this tutorial, we use a few simple data modeling techniques. For a more in-depth discussion of data modeling in FoundationDB, see :doc:`data-modeling`.

The concepts in this tutorial are applicable to all the :doc:`languages <api-reference>` supported by FoundationDB. If you prefer, you can see a version of this tutorial in :doc:`Python <class-scheduling>`, :doc:`Ruby <class-scheduling-ruby>`, or :doc:`Go <class-scheduling-go>`.

.. _class-sched-java-first-steps:

First steps
===========

Let's begin with "Hello world."

If you have not yet installed FoundationDB, see :doc:`getting-started-mac` or :doc:`getting-started-linux`.

We'll start by importing the basic FoundationDB package, as well as the :class:`Tuple` class:

.. code-block:: java

  import com.apple.foundationdb.*;
  import com.apple.foundationdb.tuple.Tuple;

Before using the API, we need to specify the API version. This allows programs to maintain compatibility even if the API is modified in future versions. Next, we open a FoundationDB database.  The API will connect to the FoundationDB cluster indicated by the :ref:`default cluster file <default-cluster-file>`.

.. code-block:: java

  private static final FDB fdb;
  private static final Database db;

  static {
    fdb = FDB.selectAPIVersion(800);
    db = fdb.open();
  }

We're ready to use the database. First, let's write a key-value pair. We do this by executing a transaction with the :meth:`run` method. We'll also use methods of the :class:`Tuple` class to :meth:`pack` data for storage in the database:

.. code-block:: java

  db.run((Transaction tr) -> {
    tr.set(Tuple.from("hello").pack(), Tuple.from("world").pack());
    return null;
  });

When :meth:`run` returns without exception, the modification is durably stored in FoundationDB! This method creates a transaction with a single modification. We'll see later how to do multiple operations in a single transaction. For now, let's read back the data. We'll use :class:`Tuple` again to unpack the ``result`` as a string:

.. code-block:: java

  String hello = db.run((Transaction tr) -> {
    byte[] result = tr.get(Tuple.from("hello").pack()).join();
    return Tuple.fromBytes(result).getString(0);
  });
  System.out.println("Hello " + hello);

If this is all working, it looks like we are ready to start building a real application. For reference, here's the full code for "hello world":

.. code-block:: java

  import com.apple.foundationdb.*;
  import com.apple.foundationdb.tuple.Tuple;

  public class HelloWorld {

    private static final FDB fdb;
    private static final Database db;

    static {
      fdb = FDB.selectAPIVersion(800);
      db = fdb.open();
    }

    public static void main(String[] args) {
      // Run an operation on the database
      db.run((Transaction tr) -> {
        tr.set(Tuple.from("hello").pack(), Tuple.from("world").pack());
        return null;
      });
      // Get the value of 'hello' from the database
      String hello = db.run((Transaction tr) -> {
        byte[] result = tr.get(Tuple.from("hello").pack()).join();
        return Tuple.fromBytes(result).getString(0);
      });
      System.out.println("Hello " + hello);
    }
  }

Class scheduling application
============================

Let's say we've been asked to build a class scheduling system for students and administrators. We'll walk through the design and implementation of this application. Instead of typing everything in as you follow along, look at the :ref:`class-sched-java-appendix` for a finished version of the program. You may want to refer to this code as we walk through the tutorial.

Requirements
------------

We'll need to let users list available classes and track which students have signed up for which classes. Here's a first cut at the functions we'll need to implement::

    availableClasses()       // returns list of classes
    signup(studentID, class) // signs up a student for a class
    drop(studentID, class)   // drops a student from a class

.. _class-sched-java-data-model:

Data model
----------

First, we need to design a :doc:`data model <data-modeling>`. A data model is just a method for storing our application data using keys and values in FoundationDB. We seem to have two main types of data: (1) a list of classes and (2) a record of which students will attend which classes. Let's keep attending data like this::

    // ("attends", student, class) = ""

We'll just store the key with a blank value to indicate that a student is signed up for a particular class. For this application, we're going to think about a key-value pair's key as a :ref:`tuple <data-modeling-tuples>`. Encoding a tuple of data elements into a key is a very common pattern for an ordered key-value store.

We'll keep data about classes like this::

    // ("class", class_name) = seatsAvailable

Similarly, each such key will represent an available class. We'll use ``seatsAvailable`` to record the number of seats available.

Transactions
------------

We're going to rely on the powerful guarantees of transactions to help keep all of our modifications straight, so let's look at how the FoundationDB Java API lets you write a transactional function. We use the :meth:`run` method to execute a code block transactionally. Let's write the simple ``addClass`` function we'll use to populate the database's class list:

.. code-block:: java

  private static void addClass(TransactionContext db, final String c) {
    db.run((Transaction tr) -> {
      tr.set(Tuple.from("class", c).pack(), encodeInt(100));
      return null;
    });
  }

A function using this approach takes a :class:`TransactionContext` parameter. When *calling* such a function, you can pass either a :class:`Database` or :class:`Transaction`, each of which are subclasses of :class:`TransactionContext`. The function to be executed transactionally is parameterized by the :class:`Transaction` it will use to do reads and writes.

The :meth:`run` method *automatically creates a transaction and implements a retry loop* to ensure that the transaction eventually commits.

For a :class:`database` ``db``::

    addClass(db, "class1")

is equivalent to something like:

.. code-block:: java

    Transaction t = db.createTransaction();
    while (true) {
      try {
        tr.set(Tuple.from("class", "class1").pack(), encodeInt(100));
        t.commit().join();
      } catch (RuntimeException e) {
        t = t.onError(e).join();
      }
    }

If instead you pass a :class:`Transaction` for the :class:`TransactionContext` parameter, the transaction will be used directly, and it is assumed that the caller implements appropriate retry logic for errors. This permits functions using this pattern to be composed into larger transactions.

Note that by default, the operation will be retried an infinite number of times and the transaction will never time out. It is therefore recommended that the client choose a default transaction retry limit or timeout value that is suitable for their application. This can be set either at the transaction level using the ``setRetryLimit`` or ``setTimeout`` transaction options or at the database level with the ``setTransactionRetryLimit`` or ``setTransactionTimeout`` database options. For example, one can set a one minute timeout on each transaction and a default retry limit of 100 by calling::

    db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
    db.options().setTransactionRetryLimit(100);

Making some sample classes
--------------------------

Let's make some sample classes and put them in the ``classNames`` variable. We'll make individual classes from combinations of class types, levels, and times:

.. code-block:: java

  // Generate 1,620 classes like '9:00 chem for dummies'
  private static List<String> levels = Arrays.asList("intro", "for dummies",
    "remedial", "101", "201", "301", "mastery", "lab", "seminar");

  private static List<String> types = Arrays.asList("chem", "bio", "cs",
      "geometry", "calc", "alg", "film", "music", "art", "dance");

  private static List<String> times = Arrays.asList("2:00", "3:00", "4:00",
    "5:00", "6:00", "7:00", "8:00", "9:00", "10:00", "11:00", "12:00", "13:00",
    "14:00", "15:00", "16:00", "17:00", "18:00", "19:00");

  private static List<String> classNames = initClassNames();

  private static List<String> initClassNames() {
    List<String> classNames = new ArrayList<String>();
    for (String level: levels)
      for (String type: types)
        for (String time: times)
          classNames.add(time + " " + type + " " + level);
    return classNames;
  }

Initializing the database
-------------------------
We initialize the database with our class list:

.. code-block:: java

  private static void init(Database db) {
    db.run((Transaction tr) -> {
      tr.clear(Tuple.from("attends").range());
      tr.clear(Tuple.from("class").range());
      for (String className: classNames)
        addClass(tr, className);
      return null;
    });
  }

After :meth:`init` is run, the database will contain all of the sample classes we created above.

Listing available classes
-------------------------

Before students can do anything else, they need to be able to retrieve a list of available classes from the database. Because FoundationDB sorts its data by key and therefore has efficient range-read capability, we can retrieve all of the classes in a single database call. We find this range of keys with :meth:`getRange`:

.. code-block:: java

  private static List<String> availableClasses(TransactionContext db) {
    return db.run((Transaction tr) -> {
      List<String> classNames = new ArrayList<String>();
      for(KeyValue kv: tr.getRange(Tuple.from("class").range()))
        classNames.add(Tuple.fromBytes(kv.getKey()).getString(1));
      return classNames;
    });
  }

In general, the :meth:`Tuple.range` method returns a :class:`Range` representing all the key-value pairs starting with the specified tuple. In this case, we want all classes, so we call :meth:`Tuple.range` with the tuple ``("class")``. The :meth:`getRange` method returns an iterable of the key-values specified by its range. To extract the class name, we unpack the key using :meth:`Tuple.fromBytes` and take its second part. (The first part is the prefix ``"class"``.)

Signing up for a class
----------------------

We finally get to the crucial function. A student has decided on a class (by name) and wants to sign up. The ``signup`` function will take a student (``s``) and a class (``c``):

.. code-block:: java

  private static void signup(TransactionContext db, final String s, final String c) {
    db.run((Transaction tr) -> {
      byte[] rec = Tuple.from("attends", s, c).pack();
      tr.set(rec, Tuple.from("").pack());
      return null;
    });
  }

We simply insert the appropriate record (with a blank value).

Dropping a class
----------------

Dropping a class is similar to signing up:

.. code-block:: java

  private static void drop(TransactionContext db, final String s, final String c) {
    db.run((Transaction tr) -> {
      byte[] rec = Tuple.from("attends", s, c).pack();
      tr.clear(rec);
      return null;
    });
  }

Of course, to actually drop the student from the class, we need to be able to delete a record from the database.  We do this with the :meth:`clear` method.

Done?
-----

We report back to the project leader that our application is done---students can sign up for, drop, and list classes. Unfortunately, we learn that a new problem has been discovered: popular classes are being over-subscribed. Our application now needs to enforce the class size constraint as students add and drop classes.

Seats are limited!
------------------

Let's go back to the data model. Remember that we stored the number of seats in the class in the value of the key-value entry in the class list. Let's refine that a bit to track the *remaining* number of seats in the class. The initialization can work the same way (in our example, all classes initially have 100 seats), but the ``availableClasses``, ``signup``, and ``drop`` functions are going to have to change. Let's start with ``availableClasses``:

.. code-block:: java
  :emphasize-lines: 5

  private static List<String> availableClasses(TransactionContext db) {
    return db.run((Transaction tr) -> {
      List<String> classNames = new ArrayList<String>();
      for(KeyValue kv: tr.getRange(Tuple.from("class").range())) {
        if (decodeInt(kv.getValue()) > 0)
          classNames.add(Tuple.fromBytes(kv.getKey()).getString(1));
      }
      return classNames;
    });
  }

This is easy -- we simply add a condition to check that the value is non-zero. Let's check out ``signup`` next:

.. code-block:: java
  :emphasize-lines: 4-11

  private static void signup(TransactionContext db, final String s, final String c) {
    db.run((Transaction tr) -> {
      byte[] rec = Tuple.from("attends", s, c).pack();
      if (tr.get(rec).join() != null)
        return null; // already signed up

      int seatsLeft = decodeInt(tr.get(Tuple.from("class", c).pack()).join());
      if (seatsLeft == 0)
        throw new IllegalStateException("No remaining seats");

      tr.set(Tuple.from("class", c).pack(), encodeInt(seatsLeft - 1));
      tr.set(rec, Tuple.from("").pack());
      return null;
    });
  }

We now have to check that we aren't already signed up, since we don't want a double sign up to decrease the number of seats twice. Then we look up how many seats are left to make sure there is a seat remaining so we don't push the counter into the negative. If there is a seat remaining, we decrement the counter.


Concurrency and consistency
---------------------------

The ``signup`` function is starting to get a bit complex; it now reads and writes a few different key-value pairs in the database. One of the tricky issues in this situation is what happens as multiple clients/students read and modify the database at the same time. Couldn't two students both see one remaining seat and sign up at the same time?

These are tricky issues without simple answers---unless you have transactions! Because these functions are defined as FoundationDB transactions, we can have a simple answer: Each transactional function behaves as if it is the only one modifying the database. There is no way for a transaction to 'see' another transaction change the database, and each transaction ensures that either all of its modifications occur or none of them do.

Looking deeper, it is, of course, possible for two transactions to conflict. For example, if two people both see a class with one seat and sign up at the same time, FoundationDB must allow only one to succeed. This causes one of the transactions to fail to commit (which can also be caused by network outages, crashes, etc.). To ensure correct operation, applications need to handle this situation, usually via retrying the transaction. In this case, the conflicting transaction will be retried automatically by the :meth:`run` method and will eventually lead to the correct result, a 'No remaining seats' exception.

Idempotence
-----------

Occasionally, a transaction might be retried even after it succeeds (for example, if the client loses contact with the cluster at just the wrong moment). This can cause problems if transactions are not written to be idempotent, i.e. to have the same effect if committed twice as if committed once. There are generic design patterns for :ref:`making any transaction idempotent <developer-guide-unknown-results>`, but many transactions are naturally idempotent. For example, all of the transactions in this tutorial are idempotent.

Dropping with limited seats
---------------------------

Let's finish up the limited seats feature by modifying the drop function:

.. code-block:: java
  :emphasize-lines: 4-7

  private static void drop(TransactionContext db, final String s, final String c) {
    db.run((Transaction tr) -> {
      byte[] rec = Tuple.from("attends", s, c).pack();
      if (tr.get(rec).join() == null)
        return null; // not taking this class
      byte[] classKey = Tuple.from("class", c).pack();
      tr.set(classKey, encodeInt(decodeInt(tr.get(classKey).join()) + 1));
      tr.clear(rec);
      return null;
    });
  }

This case is easier than signup because there are no constraints we can hit. We just need to make sure the student is in the class and to "give back" one seat when the student drops.

More features?!
---------------

Of course, as soon as our new version of the system goes live, we hear of a trick that certain students are using. They are signing up for all classes immediately, and only later dropping those that they don't want to take. This has led to an unusable system, and we have been asked to fix it. We decide to limit students to five classes:

.. code-block:: java
  :emphasize-lines: 11-13

  private static void signup(TransactionContext db, final String s, final String c) {
    db.run((Transaction tr) -> {
      byte[] rec = Tuple.from("attends", s, c).pack();
      if (tr.get(rec).join() != null)
        return null; // already signed up

      int seatsLeft = decodeInt(tr.get(Tuple.from("class", c).pack()).join());
      if (seatsLeft == 0)
        throw new IllegalStateException("No remaining seats");

      List<KeyValue> classes = tr.getRange(Tuple.from("attends", s).range()).asList().join();
      if (classes.size() == 5)
        throw new IllegalStateException("Too many classes");

      tr.set(Tuple.from("class", c).pack(), encodeInt(seatsLeft - 1));
      tr.set(rec, Tuple.from("").pack());
      return null;
    });
  }

Fortunately, we decided on a data model that keeps all of the attending records for a single student together. With this approach, we can use a single range read to retrieve all the classes that a student attends. We simply throw an exception if the number of classes has reached the limit of five.

Composing transactions
----------------------

Oh, just one last feature, we're told. We have students that are trying to switch from one popular class to another. By the time they drop one class to free up a slot for themselves, the open slot in the other class is gone. By the time they see this and try to re-add their old class, that slot is gone too! So, can we make it so that a student can switch from one class to another without this worry?

Fortunately, we have FoundationDB, and this sounds an awful lot like the transactional property of atomicity---the all-or-nothing behavior that we already rely on. All we need to do is to *compose* the ``drop`` and ``signup`` functions into a new ``switchClasses`` function. This makes the ``switchClasses`` function exceptionally easy:

.. code-block:: java

  private static void switchClasses(TransactionContext db, final String s, final String oldC, final String newC) {
    db.run((Transaction tr) -> {
      drop(tr, s, oldC);
      signup(tr, s, newC);
      return null;
    });
  }

The simplicity of this implementation belies the sophistication of what FoundationDB is taking care of for us.

By dropping the old class and signing up for the new one inside a single transaction, we ensure that either both steps happen, or that neither happens. The first notable thing about the ``switchClasses`` function is that it is transactional, but it also calls the transactional functions ``signup`` and ``drop``. Because these transactional functions can accept either a database or an existing transaction as their ``db`` parameter, the ``switchClass`` function can be called with a database by a simple client, and a new transaction will be automatically created. However, once this transaction is created and passed in as ``tr``, the calls to ``drop`` and ``signup`` both share the same ``tr``. This ensures that they see each other's modifications to the database, and all of the changes that both of them make in sequence are made transactionally when the ``switchClass`` function returns. This compositional capability is very powerful.

Also note that, if an exception is raised, for example, in ``signup``, the exception is not caught by ``switchClasses`` and so will be thrown to the calling function. In this case, the transaction object (owned by the :meth:`run` method) is destroyed, automatically rolling back all database modifications, leaving the database completely unchanged by the half-executed function.

Are we done?
------------

Yep, we’re done and ready to deploy. If you want to see this entire application in one place plus some multithreaded testing code to simulate concurrency, look at the :ref:`class-sched-java-appendix`, below.

Deploying and scaling
---------------------

Since we store all state for this application in FoundationDB, deploying and scaling this solution up is impressively painless. Just run a web server, the UI, this back end, and point the whole thing at FoundationDB. We can run as many computers with this setup as we want, and they can all hit the database at the same time because of the transactional integrity of FoundationDB. Also, since all of the state in the system is stored in the database, any of these computers can fail without any lasting consequences.

Next steps
==========

* See :doc:`data-modeling` for guidance on using tuple and subspaces to enable effective storage and retrieval of data.
* See :doc:`developer-guide` for general guidance on development using FoundationDB.
* See the :doc:`API References <api-reference>` for detailed API documentation.

.. _class-sched-java-appendix:

Appendix: ClassScheduling.java
===============================

Here's the code for the scheduling tutorial:

.. code-block:: java

  import java.nio.ByteBuffer;
  import java.util.Arrays;
  import java.util.ArrayList;
  import java.util.List;
  import java.util.Random;

  import com.apple.foundationdb.*;
  import com.apple.foundationdb.tuple.Tuple;


  // Data model:
  // ("attends", student, class) = ""
  // ("class", class_name) = seatsLeft

  public class ClassScheduling {

    private static final FDB fdb;
    private static final Database db;

    static {
      fdb = FDB.selectAPIVersion(800);
      db = fdb.open();
      db.options().setTransactionTimeout(60000);  // 60,000 ms = 1 minute
      db.options().setTransactionRetryLimit(100);
    }

    // Generate 1,620 classes like '9:00 chem for dummies'
    private static List<String> levels = Arrays.asList("intro", "for dummies",
      "remedial", "101", "201", "301", "mastery", "lab", "seminar");

    private static List<String> types = Arrays.asList("chem", "bio", "cs",
        "geometry", "calc", "alg", "film", "music", "art", "dance");

    private static List<String> times = Arrays.asList("2:00", "3:00", "4:00",
      "5:00", "6:00", "7:00", "8:00", "9:00", "10:00", "11:00", "12:00", "13:00",
      "14:00", "15:00", "16:00", "17:00", "18:00", "19:00");

    private static List<String> classNames = initClassNames();

    private static List<String> initClassNames() {
      List<String> classNames = new ArrayList<String>();
      for (String level: levels)
        for (String type: types)
          for (String time: times)
            classNames.add(time + " " + type + " " + level);
      return classNames;
    }

    private static void addClass(TransactionContext db, final String c) {
      db.run((Transaction tr) -> {
        tr.set(Tuple.from("class", c).pack(), encodeInt(100));
        return null;
      });
    }

    private static byte[] encodeInt(int value) {
      byte[] output = new byte[4];
      ByteBuffer.wrap(output).putInt(value);
      return output;
    }

    private static int decodeInt(byte[] value) {
      if (value.length != 4)
        throw new IllegalArgumentException("Array must be of size 4");
      return ByteBuffer.wrap(value).getInt();
    }

    private static void init(Database db) {
      db.run((Transaction tr) -> {
        tr.clear(Tuple.from("attends").range());
        tr.clear(Tuple.from("class").range());
        for (String className: classNames)
          addClass(tr, className);
        return null;
      });
    }

    private static List<String> availableClasses(TransactionContext db) {
      return db.run((Transaction tr) -> {
        List<String> classNames = new ArrayList<String>();
        for(KeyValue kv: tr.getRange(Tuple.from("class").range())) {
          if (decodeInt(kv.getValue()) > 0)
            classNames.add(Tuple.fromBytes(kv.getKey()).getString(1));
        }
        return classNames;
      });
    }

    private static void drop(TransactionContext db, final String s, final String c) {
      db.run((Transaction tr) -> {
        byte[] rec = Tuple.from("attends", s, c).pack();
        if (tr.get(rec).join() == null)
          return null; // not taking this class
        byte[] classKey = Tuple.from("class", c).pack();
        tr.set(classKey, encodeInt(decodeInt(tr.get(classKey).join()) + 1));
        tr.clear(rec);
        return null;
      });
    }

    private static void signup(TransactionContext db, final String s, final String c) {
      db.run((Transaction tr) -> {
        byte[] rec = Tuple.from("attends", s, c).pack();
        if (tr.get(rec).join() != null)
          return null; // already signed up

        int seatsLeft = decodeInt(tr.get(Tuple.from("class", c).pack()).join());
        if (seatsLeft == 0)
          throw new IllegalStateException("No remaining seats");

        List<KeyValue> classes = tr.getRange(Tuple.from("attends", s).range()).asList().join();
        if (classes.size() == 5)
          throw new IllegalStateException("Too many classes");

        tr.set(Tuple.from("class", c).pack(), encodeInt(seatsLeft - 1));
        tr.set(rec, Tuple.from("").pack());
        return null;
      });
    }

    private static void switchClasses(TransactionContext db, final String s, final String oldC, final String newC) {
      db.run((Transaction tr) -> {
        drop(tr, s, oldC);
        signup(tr, s, newC);
        return null;
      });
    }

    //
    // Testing
    //

    private static void simulateStudents(int i, int ops) {

      String studentID = "s" + Integer.toString(i);
      List<String> allClasses = classNames;
      List<String> myClasses = new ArrayList<String>();

      String c;
      String oldC;
      String newC;
      Random rand = new Random();

      for (int j=0; j<ops; j++) {
        int classCount = myClasses.size();
        List<String> moods = new ArrayList<String>();
        if (classCount > 0) {
          moods.add("drop");
          moods.add("switch");
        }
        if (classCount < 5)
          moods.add("add");
        String mood = moods.get(rand.nextInt(moods.size()));

        try {
          if (allClasses.isEmpty())
            allClasses = availableClasses(db);
          if (mood.equals("add")) {
            c = allClasses.get(rand.nextInt(allClasses.size()));
            signup(db, studentID, c);
            myClasses.add(c);
          } else if (mood.equals("drop")) {
            c = myClasses.get(rand.nextInt(myClasses.size()));
            drop(db, studentID, c);
            myClasses.remove(c);
          } else if (mood.equals("switch")) {
            oldC = myClasses.get(rand.nextInt(myClasses.size()));
            newC = allClasses.get(rand.nextInt(allClasses.size()));
            switchClasses(db, studentID, oldC, newC);
            myClasses.remove(oldC);
            myClasses.add(newC);
          }
        } catch (Exception e) {
          System.out.println(e.getMessage() +  "Need to recheck available classes.");
          allClasses.clear();
        }

      }

    }

    private static void runSim(int students, final int ops_per_student) throws InterruptedException {
      List<Thread> threads = new ArrayList<Thread>(students);//Thread[students];
      for (int i = 0; i < students; i++) {
        final int j = i;
        threads.add(new Thread(() -> simulateStudents(j, ops_per_student)) );
      }
      for (Thread thread: threads)
        thread.start();
      for (Thread thread: threads)
        thread.join();
      System.out.format("Ran %d transactions%n", students * ops_per_student);
    }

    public static void main(String[] args) throws InterruptedException {
      init(db);
      System.out.println("Initialized");
      runSim(10,10);
    }

  }

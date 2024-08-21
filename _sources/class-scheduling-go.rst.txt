######################
Class Scheduling in Go
######################

This tutorial provides a walkthrough of designing and building a simple application in Go using FoundationDB. In this tutorial, we use a few simple data modeling techniques. For a more in-depth discussion of data modeling in FoundationDB, see :doc:`data-modeling`.

The concepts in this tutorial are applicable to all the :doc:`languages <api-reference>` supported by FoundationDB. If you prefer, you can see a version of this tutorial in :doc:`Python <class-scheduling>`, :doc:`Ruby <class-scheduling-ruby>`, or :doc:`Java <class-scheduling-java>`.

.. _class-sched-go-first-steps:

First steps
===========

Let's begin with "Hello world."

If you have not yet installed FoundationDB, see :doc:`getting-started-mac` or :doc:`getting-started-linux`.

We'll start by importing the basic FoundationDB package, as well as the ``log`` and ``format`` packages:

.. code-block:: go

  import (
    "github.com/apple/foundationdb/bindings/go/src/fdb"
    "log"
    "fmt"
  )

Before using the API, we need to specify the API version. This allows programs to maintain compatibility even if the API is modified in future versions:

.. code-block:: go

  fdb.MustAPIVersion(730)

Next, we open a FoundationDB database.  The API will connect to the FoundationDB cluster indicated by the :ref:`default cluster file <default-cluster-file>`.

.. code-block:: go

  db := fdb.MustOpenDefault()

We're ready to use the database. First, let's write a key-value pair.

.. code-block:: go

  _, err := db.Transact(func (tr fdb.Transaction) (ret interface{}, e error) {
      tr.Set(fdb.Key("hello"), []byte("world"))
      return
  })
  if err != nil {
      log.Fatalf("Unable to set FDB database value (%v)", err)
  }

When this function returns without error, the modification is durably stored in FoundationDB! Under the covers, this function creates a transaction with a single modification. We’ll see later how to do multiple operations in a single transaction. For now, let’s read back the data:

.. code-block:: go

  ret, err := db.Transact(func (tr fdb.Transaction) (ret interface{}, e error) {
      ret = tr.Get(fdb.Key("hello")).MustGet()
      return
  })
  if err != nil {
      log.Fatalf("Unable to read FDB database value (%v)", err)
  }

  v := ret.([]byte)
  fmt.Printf("hello, %s\n", string(v))

If this is all working, it looks like we are ready to start building a real application. For reference, here's the full code for "hello world":

.. code-block:: go

  package main

  import (
      "github.com/apple/foundationdb/bindings/go/src/fdb"
      "log"
      "fmt"
  )

  func main() {
      // Different API versions may expose different runtime behaviors.
      fdb.MustAPIVersion(730)

      // Open the default database from the system cluster
      db := fdb.MustOpenDefault()

      _, err := db.Transact(func (tr fdb.Transaction) (ret interface{}, e error) {
          tr.Set(fdb.Key("hello"), []byte("world"))
          return
      })
      if err != nil {
          log.Fatalf("Unable to set FDB database value (%v)", err)
      }

      ret, err := db.Transact(func (tr fdb.Transaction) (ret interface{}, e error) {
          ret = tr.Get(fdb.Key("hello")).MustGet()
          return
      })
      if err != nil {
          log.Fatalf("Unable to read FDB database value (%v)", err)
      }

      v := ret.([]byte)
      fmt.Printf("hello, %s\n", string(v))
  }

Class scheduling application
============================

Let's say we've been asked to build a class scheduling system for students and administrators. We'll walk through the design and implementation of this application. Instead of typing everything in as you follow along, look at the :ref:`class-sched-go-appendix` for a finished version of the program. You may want to refer to this code as we walk through the tutorial.

Requirements
------------

We'll need to let users list available classes and track which students have signed up for which classes. Here's a first cut at the functions we'll need to implement::

    availableClasses()       // returns list of classes
    signup(studentID, class) // signs up a student for a class
    drop(studentID, class)   // drops a student from a class

.. _class-sched-go-data-model:

Data model
----------

First, we need to design a :doc:`data model <data-modeling>`. A data model is just a method for storing our application data using keys and values in FoundationDB. We seem to have two main types of data: (1) a list of classes and (2) a record of which students will attend which classes. Let's keep attending data like this::

    // ("attends", student, class) = ""

We'll just store the key with a blank value to indicate that a student is signed up for a particular class. For this application, we're going to think about a key-value pair's key as a :ref:`tuple <data-modeling-tuples>`. Encoding a tuple of data elements into a key is a very common pattern for an ordered key-value store.

We'll keep data about classes like this::

    // ("class", class_name) = seatsAvailable

Similarly, each such key will represent an available class. We'll use ``seatsAvailable`` to record the number of seats available.

Directories and Subspaces
-------------------------

FoundationDB includes a few modules that make it easy to model data using this approach::

  import (
    "github.com/apple/foundationdb/bindings/go/src/fdb"
    "github.com/apple/foundationdb/bindings/go/src/fdb/directory"
    "github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
    "github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
  )

The :mod:`directory` module lets us open a :ref:`directory <developer-guide-directories>` in the database::

  schedulingDir, err := directory.CreateOrOpen(db, []string{"scheduling"}, nil)
  if err != nil {
    log.Fatal(err)
  }

The :func:`CreateOrOpen` function returns a :ref:`subspace <developer-guide-sub-keyspaces>` where we'll store our application data. Each subspace has a fixed prefix it uses when defining keys. The prefix corresponds to the first element of a tuple. We decided that we wanted ``"attends"`` and  ``"class"`` as our prefixes, so we'll create new subspaces for them within the ``scheduling`` subspace.::

  courseSS = schedulingDir.Sub("class")
  attendSS = schedulingDir.Sub("attends")

Subspaces have a :func:`Pack` function for defining keys. To store the records for our data model, we can use ``attendSS.Pack(tuple.Tuple{studentID, class})`` and ``courseSS.Pack(tuple.Tuple{class})``.

Transactions
------------

We're going to rely on the powerful guarantees of transactions to help keep all of our modifications straight, so let's look at how the FoundationDB Go API lets you write a transactional function. We use :func:`Transact` to execute a code block transactionally. For example, to ``signup`` a ``studentID`` for a ``class``, we might use:

.. code-block:: go

  func signup(t fdb.Transactor, studentID, class string) (err error) {
    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      tr.Set(attendSS.Pack(tuple.Tuple{studentID, class}), []byte{})
      return
    })
    return
  }

A function using this approach takes a parameter of type ``Transactor``. When *calling* such a function, you can pass an argument of type ``Database`` or ``Transaction``. The function to be executed transactionally is parameterized by the ``Transaction`` it will use to do reads and writes.

When using a ``Database``, :func:`Transact` *automatically creates a transaction and implements a retry loop* to ensure that the transaction eventually commits. If you instead pass a ``Transaction``, that transaction will be used directly, and it is assumed that the caller implements appropriate retry logic for errors. This permits functions using this pattern to be composed into larger transactions.

Without the :func:`Transact` method, signup would look something like:

.. code-block:: go

  func signup(db fdb.Database, studentID, class string) (err error) {
    tr, err := d.CreateTransaction()
    if err != nil {
      return
    }

    wrapped := func() {
      defer func() {
        if r := recover(); r != nil {
          e, ok := r.(Error)
          if ok {
            err = e
          } else {
            panic(r)
          }
        }
      }()

      tr.Set(attendSS.Pack(tuple.Tuple{studentID, class}), []byte{})

      err = tr.Commit().Get()
    }

    for {
      wrapped()

      if err == nil {
        return
      }

      fe, ok := err.(Error)
      if ok {
        err = tr.OnError(fe).Get()
      }

      if err != nil {
        return
      }
    }
  }

Furthermore, this version can only be called with a ``Database``, making it impossible to compose larger transactional functions by calling one from another.

Note that by default, the operation will be retried an infinite number of times and the transaction will never time out. It is therefore recommended that the client choose a default transaction retry limit or timeout value that is suitable for their application. This can be set either at the transaction level using the ``SetRetryLimit`` or ``SetTimeout`` transaction options or at the database level with the ``SetTransactionRetryLimit`` or ``SetTransactionTimeout`` database options. For example, one can set a one minute timeout on each transaction and a default retry limit of 100 by calling::

    db.Options().SetTransactionTimeout(60000)  // 60,000 ms = 1 minute
    db.Options().SetTransactionRetryLimit(100)

Making some sample classes
--------------------------

Let's make some sample classes and put them in the ``classNames`` variable. We'll make individual classes from combinations of class types, levels, and times:

.. code-block:: go

  var levels = []string{"intro", "for dummies", "remedial", "101", "201", "301", "mastery", "lab", "seminar"}
  var types = []string{"chem", "bio", "cs", "geometry", "calc", "alg", "film", "music", "art", "dance"}
  var times = []string{"2:00", "3:00", "4:00", "5:00", "6:00", "7:00", "8:00", "9:00", "10:00", "11:00",
                       "12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00"}

  classes := make([]string, len(levels) * len(types) * len(times))

  for i := range levels {
    for j := range types {
      for k := range times {
        classes[i*len(types)*len(times)+j*len(times)+k] = fmt.Sprintf("%s %s %s", levels[i], types[j], times[k])
      }
    }
  }

Initializing the database
-------------------------
Next, we initialize the database with our class list:

.. code-block:: go

  _, err = db.Transact(func (tr fdb.Transaction) (interface{}, error) {
    tr.ClearRange(schedulingDir)

    for i := range classes {
      tr.Set(courseSS.Pack(tuple.Tuple{classes[i]}), []byte(strconv.FormatInt(100, 10)))
    }

    return nil, nil
  })

After this code is run, the database will contain all of the sample classes we created above.

Listing available classes
-------------------------

Before students can do anything else, they need to be able to retrieve a list of available classes from the database. Because FoundationDB sorts its data by key and therefore has efficient range-read capability, we can retrieve all of the classes in a single database call. We find this range of keys with :func:`GetRange`:

.. code-block:: go

  func availableClasses(t fdb.Transactor) (ac []string, err error) {
    r, err := t.ReadTransact(func (rtr fdb.ReadTransaction) (interface{}, error) {
      var classes []string
      ri := rtr.GetRange(courseSS, fdb.RangeOptions{}).Iterator()
      for ri.Advance() {
        kv := ri.MustGet()
        t, err := courseSS.Unpack(kv.Key)
        if err != nil {
          return nil, err
        }
        classes = append(classes, t[0].(string))
      }
      return classes, nil
    })
    if err == nil {
      ac = r.([]string)
    }
    return
  }

The :func:`GetRange` function returns the key-values specified by its range. In this case, we use the subspace ``courseSS`` to get all the classes.

Signing up for a class
----------------------

We finally get to the crucial function (which we saw before when looking at :func:`Transact`). A student has decided on a class (by name) and wants to sign up. The ``signup`` function will take a ``studentID`` and a ``class``:

.. code-block:: go

  func signup(t fdb.Transactor, studentID, class string) (err error) {
    SCKey := attendSS.Pack(tuple.Tuple{studentID, class})

    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      tr.Set(SCKey, []byte{})
      return
    })
    return
  }

We simply insert the appropriate record (with a blank value).

Dropping a class
----------------

Dropping a class is similar to signing up:

.. code-block:: go

  func drop(t fdb.Transactor, studentID, class string) (err error) {
    SCKey := attendSS.Pack(tuple.Tuple{studentID, class})

    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      tr.Clear(SCKey)
      return
    })
    return
  }

Of course, to actually drop the student from the class, we need to be able to delete a record from the database.  We do this with the :func:`Clear` function.

Done?
-----

We report back to the project leader that our application is done---students can sign up for, drop, and list classes. Unfortunately, we learn that a new problem has been discovered: popular classes are being over-subscribed. Our application now needs to enforce the class size constraint as students add and drop classes.

Seats are limited!
------------------

Let's go back to the data model. Remember that we stored the number of seats in the class in the value of the key-value entry in the class list. Let's refine that a bit to track the *remaining* number of seats in the class. The initialization can work the same way (in our example, all classes initially have 100 seats), but the ``availableClasses``, ``signup``, and ``drop`` functions are going to have to change. Let's start with ``availableClasses``:

.. code-block:: go
  :emphasize-lines: 7-11

  func availableClasses(t fdb.Transactor) (ac []string, err error) {
    r, err := t.ReadTransact(func (rtr fdb.ReadTransaction) (interface{}, error) {
      var classes []string
      ri := rtr.GetRange(courseSS, fdb.RangeOptions{}).Iterator()
      for ri.Advance() {
        kv := ri.MustGet()
        v, err := strconv.ParseInt(string(kv.Value), 10, 64)
        if err != nil {
          return nil, err
        }
        if v > 0 {
          t, err := courseSS.Unpack(kv.Key)
          if err != nil {
            return nil, err
          }
          classes = append(classes, t[0].(string))
        }
      }
      return classes, nil
    })
    if err == nil {
      ac = r.([]string)
    }
    return
  }

This is easy -- we simply add a condition to check that the value is non-zero. Let's check out ``signup`` next:

.. code-block:: go
  :emphasize-lines: 6-19

  func signup(t fdb.Transactor, studentID, class string) (err error) {
    SCKey := attendSS.Pack(tuple.Tuple{studentID, class})
    classKey := courseSS.Pack(tuple.Tuple{class})

    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      if tr.Get(SCKey).MustGet() != nil {
        return // already signed up
      }

      seats, err := strconv.ParseInt(string(tr.Get(classKey).MustGet()), 10, 64)
      if err != nil {
        return
      }
      if seats == 0 {
        err = errors.New("no remaining seats")
        return
      }

      tr.Set(classKey, []byte(strconv.FormatInt(seats - 1, 10)))
      tr.Set(SCKey, []byte{})

      return
    })
    return
  }

We now have to check that we aren't already signed up, since we don't want a double sign up to decrease the number of seats twice. Then we look up how many seats are left to make sure there is a seat remaining so we don't push the counter into the negative. If there is a seat remaining, we decrement the counter.


Concurrency and consistency
---------------------------

The ``signup`` function is starting to get a bit complex; it now reads and writes a few different key-value pairs in the database. One of the tricky issues in this situation is what happens as multiple clients/students read and modify the database at the same time. Couldn't two students both see one remaining seat and sign up at the same time?

These are tricky issues without simple answers---unless you have transactions! Because these functions are defined as FoundationDB transactions, we can have a simple answer: Each transactional function behaves as if it is the only one modifying the database. There is no way for a transaction to 'see' another transaction change the database, and each transaction ensures that either all of its modifications occur or none of them do.

Looking deeper, it is, of course, possible for two transactions to conflict. For example, if two people both see a class with one seat and sign up at the same time, FoundationDB must allow only one to succeed. This causes one of the transactions to fail to commit (which can also be caused by network outages, crashes, etc.). To ensure correct operation, applications need to handle this situation, usually via retrying the transaction. In this case, the conflicting transaction will be retried automatically by the :func:`Transact` function and will eventually lead to the correct result, a 'No remaining seats' error.

Idempotence
-----------

Occasionally, a transaction might be retried even after it succeeds (for example, if the client loses contact with the cluster at just the wrong moment). This can cause problems if transactions are not written to be idempotent, i.e. to have the same effect if committed twice as if committed once. There are generic design patterns for :ref:`making any transaction idempotent <developer-guide-unknown-results>`, but many transactions are naturally idempotent. For example, all of the transactions in this tutorial are idempotent.

Dropping with limited seats
---------------------------

Let's finish up the limited seats feature by modifying the drop function:

.. code-block:: go
  :emphasize-lines: 6-15

  func drop(t fdb.Transactor, studentID, class string) (err error) {
    SCKey := attendSS.Pack(tuple.Tuple{studentID, class})
    classKey := courseSS.Pack(tuple.Tuple{class})

    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      if tr.Get(SCKey).MustGet() == nil {
        return // not taking this class
      }

      seats, err := strconv.ParseInt(string(tr.Get(classKey).MustGet()), 10, 64)
      if err != nil {
        return
      }

      tr.Set(classKey, []byte(strconv.FormatInt(seats + 1, 10)))
      tr.Clear(SCKey)

      return
    })
    return
  }

This case is easier than signup because there are no constraints we can hit. We just need to make sure the student is in the class and to "give back" one seat when the student drops.

More features?!
---------------

Of course, as soon as our new version of the system goes live, we hear of a trick that certain students are using. They are signing up for all classes immediately, and only later dropping those that they don't want to take. This has led to an unusable system, and we have been asked to fix it. We decide to limit students to five classes:

.. code-block:: go
  :emphasize-lines: 19-23

  func signup(t fdb.Transactor, studentID, class string) (err error) {
    SCKey := attendSS.Pack(tuple.Tuple{studentID, class})
    classKey := courseSS.Pack(tuple.Tuple{class})

    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      if tr.Get(SCKey).MustGet() != nil {
        return // already signed up
      }

      seats, err := strconv.ParseInt(string(tr.Get(classKey).MustGet()), 10, 64)
      if err != nil {
        return
      }
      if seats == 0 {
        err = errors.New("no remaining seats")
        return
      }

      classes := tr.GetRange(attendSS.Sub(studentID), fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).GetSliceOrPanic()
      if len(classes) == 5 {
        err = errors.New("too many classes")
        return
      }

      tr.Set(classKey, []byte(strconv.FormatInt(seats - 1, 10)))
      tr.Set(SCKey, []byte{})

      return
    })
    return
  }

Fortunately, we decided on a data model that keeps all of the attending records for a single student together. With this approach, we can use a single range read to retrieve all the classes that a student attends. We simply return an error if the number of classes has reached the limit of five.

Composing transactions
----------------------

Oh, just one last feature, we're told. We have students that are trying to switch from one popular class to another. By the time they drop one class to free up a slot for themselves, the open slot in the other class is gone. By the time they see this and try to re-add their old class, that slot is gone too! So, can we make it so that a student can switch from one class to another without this worry?

Fortunately, we have FoundationDB, and this sounds an awful lot like the transactional property of atomicity---the all-or-nothing behavior that we already rely on. All we need to do is to *compose* the ``drop`` and ``signup`` functions into a new ``swap`` function. This makes the ``swap`` function exceptionally easy:

.. code-block:: go

  func swap(t fdb.Transactor, studentID, oldClass, newClass string) (err error) {
    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      err = drop(tr, studentID, oldClass)
      if err != nil {
        return
      }
      err = signup(tr, studentID, newClass)
      return
    })
    return
  }

The simplicity of this implementation belies the sophistication of what FoundationDB is taking care of for us.

By dropping the old class and signing up for the new one inside a single transaction, we ensure that either both steps happen, or that neither happens. The first notable thing about the ``swap`` function is that it is transactional, but it also calls the transactional functions ``signup`` and ``drop``. Because these transactional functions can accept either a ``Database`` or an existing ``Transaction`` as their ``db`` parameter, the ``switchClass`` function can be called with a database by a simple client, and a new transaction will be automatically created. However, once this transaction is created and passed in as ``tr``, the calls to ``drop`` and ``signup`` both share the same ``tr``. This ensures that they see each other's modifications to the database, and all of the changes that both of them make in sequence are made transactionally when the ``switchClass`` function returns. This compositional capability is very powerful.

Also note that, if an exception is raised, for example, in ``signup``, the exception is not caught by ``swap`` and so will be thrown to the calling function. In this case, the transaction object (owned by the :func:`Transact` function) is destroyed, automatically rolling back all database modifications, leaving the database completely unchanged by the half-executed function.

Are we done?
------------

Yep, we’re done and ready to deploy. If you want to see this entire application in one place plus some multithreaded testing code to simulate concurrency, look at the :ref:`class-sched-go-appendix`, below.

Deploying and scaling
---------------------

Since we store all state for this application in FoundationDB, deploying and scaling this solution up is impressively painless. Just run a web server, the UI, this back end, and point the whole thing at FoundationDB. We can run as many computers with this setup as we want, and they can all hit the database at the same time because of the transactional integrity of FoundationDB. Also, since all of the state in the system is stored in the database, any of these computers can fail without any lasting consequences.

Next steps
==========

* See :doc:`data-modeling` for guidance on using tuple and subspaces to enable effective storage and retrieval of data.
* See :doc:`developer-guide` for general guidance on development using FoundationDB.
* See the :doc:`API References <api-reference>` for detailed API documentation.

.. _class-sched-go-appendix:

Appendix: classScheduling.go
============================

Here's the code for the scheduling tutorial:

.. code-block:: go

  package main

  import (
    "github.com/apple/foundationdb/bindings/go/src/fdb"
    "github.com/apple/foundationdb/bindings/go/src/fdb/directory"
    "github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
    "github.com/apple/foundationdb/bindings/go/src/fdb/tuple"

    "fmt"
    "log"
    "strconv"
    "errors"
    "sync"
    "math/rand"
  )

  var courseSS subspace.Subspace
  var attendSS subspace.Subspace

  var classes []string

  func availableClasses(t fdb.Transactor) (ac []string, err error) {
    r, err := t.ReadTransact(func (rtr fdb.ReadTransaction) (interface{}, error) {
      var classes []string
      ri := rtr.GetRange(courseSS, fdb.RangeOptions{}).Iterator()
      for ri.Advance() {
        kv := ri.MustGet()
        v, err := strconv.ParseInt(string(kv.Value), 10, 64)
        if err != nil {
          return nil, err
        }
        if v > 0 {
          t, err := courseSS.Unpack(kv.Key)
          if err != nil {
            return nil, err
          }
          classes = append(classes, t[0].(string))
        }
      }
      return classes, nil
    })
    if err == nil {
      ac = r.([]string)
    }
    return
  }

  func signup(t fdb.Transactor, studentID, class string) (err error) {
    SCKey := attendSS.Pack(tuple.Tuple{studentID, class})
    classKey := courseSS.Pack(tuple.Tuple{class})

    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      if tr.Get(SCKey).MustGet() != nil {
        return // already signed up
      }

      seats, err := strconv.ParseInt(string(tr.Get(classKey).MustGet()), 10, 64)
      if err != nil {
        return
      }
      if seats == 0 {
        err = errors.New("no remaining seats")
        return
      }

      classes := tr.GetRange(attendSS.Sub(studentID), fdb.RangeOptions{Mode: fdb.StreamingModeWantAll}).GetSliceOrPanic()
      if len(classes) == 5 {
        err = errors.New("too many classes")
        return
      }

      tr.Set(classKey, []byte(strconv.FormatInt(seats - 1, 10)))
      tr.Set(SCKey, []byte{})

      return
    })
    return
  }

  func drop(t fdb.Transactor, studentID, class string) (err error) {
    SCKey := attendSS.Pack(tuple.Tuple{studentID, class})
    classKey := courseSS.Pack(tuple.Tuple{class})

    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      if tr.Get(SCKey).MustGet() == nil {
        return // not taking this class
      }

      seats, err := strconv.ParseInt(string(tr.Get(classKey).MustGet()), 10, 64)
      if err != nil {
        return
      }

      tr.Set(classKey, []byte(strconv.FormatInt(seats + 1, 10)))
      tr.Clear(SCKey)

      return
    })
    return
  }

  func swap(t fdb.Transactor, studentID, oldClass, newClass string) (err error) {
    _, err = t.Transact(func (tr fdb.Transaction) (ret interface{}, err error) {
      err = drop(tr, studentID, oldClass)
      if err != nil {
        return
      }
      err = signup(tr, studentID, newClass)
      return
    })
    return
  }

  func main() {
    fdb.MustAPIVersion(730)
    db := fdb.MustOpenDefault()
    db.Options().SetTransactionTimeout(60000)  // 60,000 ms = 1 minute
    db.Options().SetTransactionRetryLimit(100)

    schedulingDir, err := directory.CreateOrOpen(db, []string{"scheduling"}, nil)
    if err != nil {
      log.Fatal(err)
    }

    courseSS = schedulingDir.Sub("class")
    attendSS = schedulingDir.Sub("attends")

    var levels = []string{"intro", "for dummies", "remedial", "101", "201", "301", "mastery", "lab", "seminar"}
    var types = []string{"chem", "bio", "cs", "geometry", "calc", "alg", "film", "music", "art", "dance"}
    var times = []string{"2:00", "3:00", "4:00", "5:00", "6:00", "7:00", "8:00", "9:00", "10:00", "11:00",
                         "12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00"}

    classes := make([]string, len(levels) * len(types) * len(times))

    for i := range levels {
      for j := range types {
        for k := range times {
          classes[i*len(types)*len(times)+j*len(times)+k] = fmt.Sprintf("%s %s %s", levels[i], types[j], times[k])
        }
      }
    }

    _, err = db.Transact(func (tr fdb.Transaction) (interface{}, error) {
      tr.ClearRange(schedulingDir)

      for i := range classes {
        tr.Set(courseSS.Pack(tuple.Tuple{classes[i]}), []byte(strconv.FormatInt(100, 10)))
      }

      return nil, nil
    })

    run(db, 10, 10)
  }

  func indecisiveStudent(db fdb.Database, id, ops int, wg *sync.WaitGroup) {
    studentID := fmt.Sprintf("s%d", id)

    allClasses := classes

    var myClasses []string

    for i := 0; i < ops; i++ {
      var moods []string
      if len(myClasses) > 0 {
        moods = append(moods, "drop", "switch")
      }
      if len(myClasses) < 5 {
        moods = append(moods, "add")
      }

      func() {
        defer func() {
          if r := recover(); r != nil {
            fmt.Println("Need to recheck classes:", r)
            allClasses = []string{}
          }
        }()

        var err error

        if len(allClasses) == 0 {
          allClasses, err = availableClasses(db)
          if err != nil {
            panic(err)
          }
        }

        switch moods[rand.Intn(len(moods))] {
        case "add":
          class := allClasses[rand.Intn(len(allClasses))]
          err = signup(db, studentID, class)
          if err != nil {
            panic(err)
          }
          myClasses = append(myClasses, class)
        case "drop":
          classI := rand.Intn(len(myClasses))
          err = drop(db, studentID, myClasses[classI])
          if err != nil {
            panic(err)
          }
          myClasses[classI], myClasses = myClasses[len(myClasses)-1], myClasses[:len(myClasses)-1]
        case "switch":
          oldClassI := rand.Intn(len(myClasses))
          newClass := allClasses[rand.Intn(len(allClasses))]
          err = swap(db, studentID, myClasses[oldClassI], newClass)
          if err != nil {
            panic(err)
          }
          myClasses[oldClassI] = newClass
        }
      }()
    }

    wg.Done()
  }

  func run(db fdb.Database, students, opsPerStudent int) {
    var wg sync.WaitGroup

    wg.Add(students)

    for i := 0; i < students; i++ {
      go indecisiveStudent(db, i, opsPerStudent, &wg)
    }

    wg.Wait()

    fmt.Println("Ran", students * opsPerStudent, "transactions")
  }

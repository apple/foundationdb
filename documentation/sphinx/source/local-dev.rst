#################
Local Development 
#################

Download the FoundationDB package
=================================

:doc:`Download the FoundationDB package <downloads>` for macOS (FoundationDB-\*.pkg) onto your local development machine.

Install the FoundationDB binaries
=================================

By default, the FoundationDB installer installs the binaries required to run both a client and a local development server. Begin installation by double-clicking on the downloaded FoundationDB package and following the displayed instructions.

For more details on installing on macOS, see :doc:`getting-started-mac`.

If you later wish to remove FoundationDB from a machine, follow the instruction for :ref:`uninstalling <administration-removing>`.

Check the status of the local database
======================================

You can verify the status of the local database with the following the command, which will show basic statistics::

    fdbcli --exec status

Basic tutorial
==============

Here's a :doc:`tutorial <class-scheduling>` that begins with "Hello world" code for connecting to the database and then walks through the basics of reading and writing data with transactions.

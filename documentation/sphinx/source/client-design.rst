#############
Client Design
#############

FoundationDB supports language bindings for application development using the ordered key-value store. The following documents cover use of the bindings, from getting started and design principles to best practices and data modeling. The latest changes are detailed in :ref:`release-notes`.

* :doc:`getting-started-mac` explains how to install a local FoundationDB server suitable for development on macOS.

* :doc:`getting-started-linux` explains how to install a local FoundationDB server suitable for development on Linux.

* :doc:`downloads` describes the FoundationDB packages available on our website.

* :doc:`developer-guide` explains principles of application development applicable across all language bindings.

* :doc:`data-modeling` explains recommended techniques for representing application data in the key-value store.

* :doc:`client-testing` Explains how one can use workloads to test client code.

* :doc:`api-general` contains information on FoundationDB clients applicable across all language bindings.

* :doc:`transaction-tagging` contains information about using transaction tags in your client code to enable targeted transaction throttling.

* :doc:`api-version-upgrade-guide` contains information about upgrading client code to a new API version.

* :doc:`transaction-profiler-analyzer` contains information about enabling transaction profiling and analyzing.

* :doc:`known-limitations` describes both long-term design limitations of FoundationDB and short-term limitations applicable to the current version.

.. toctree::
    :maxdepth: 1
    :titlesonly:
    :hidden:

    getting-started-mac
    getting-started-linux
    downloads
    developer-guide
    data-modeling
    client-testing
    api-general
    transaction-tagging
    known-limitations
    transaction-profiler-analyzer
    api-version-upgrade-guide

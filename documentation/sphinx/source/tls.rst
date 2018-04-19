########################
Transport Layer Security
########################

.. include:: guide-common.rst.inc

Introduction
============

Transport Layer Security (TLS) and its predecessor, Secure Sockets Layer (SSL), are protocols designed to provide communication security over public networks. Users exchange a symmetric session key that is used to encrypt data exchanged between the parties.

By default, a FoundationDB cluster uses *unencrypted* connections among client and server processes. This document describes the `Transport Layer Security <http://en.wikipedia.org/wiki/Transport_Layer_Security>`_ (TLS) capabilities of FoundationDB, which enable security and authentication through a public/private key infrastructure. TLS is provided in FoundationDB via a plugin-based architecture. This document will describe the basic TLS capabilities of FoundationDB and document the default plugin, which is based on `LibreSSL <https://www.libressl.org/>`_. TLS-enabled servers will only communicate with other TLS-enabled servers and TLS-enabled clients. Therefore, a cluster's machines must all enable TLS in order for TLS to be used.


Setting Up FoundationDB to use TLS
==================================

.. _enable-TLS:

Enabling TLS in a new cluster
-----------------------------

To set a new cluster to use TLS, use the ``-t`` flag on ``make-public.py``::

    user@host1$ sudo /usr/lib/foundationdb/make-public.py -t
    /etc/foundationdb/fdb.cluster is now using address 10.0.1.1 (TLS enabled)

This will configure the new cluster to communicate with TLS.

.. note:: Depending on your operating system, version and configuration, there may be a firewall in place that prevents external access to certain ports. If necessary, please consult the appropriate documentation for your OS and ensure that all machines in your cluster can reach the ports configured in your :ref:`configuration file <foundationdb-conf>`.

.. _converting-existing-cluster:

Converting an existing cluster to use TLS
=========================================

Enabling TLS on an existing (non-TLS) cluster cannot be accomplished without downtime because all processes must have TLS enabled to communicate. At startup, each server process enables TLS if the addresses in its cluster file are TLS-enabled. As a result, server processes must be stopped and restarted to convert them to use TLS. To convert the cluster to TLS in the most conservative way:

1) Stop all FoundationDB clients and :ref:`server processes <administration-running-foundationdb>` in the cluster.

2) Change all cluster files to have the ``:tls`` suffix for each coordinator.

3) Restart the cluster and the clients.

.. _configuring-tls-plugin:

Configuring the TLS Plugin
==========================

The location and operation of the TLS plugin are configured through four settings. These settings can be provided as command-line options, client options, or environment variables, and are named as follows:

======================== ==================== ============================ ==================================================
Command-line Option      Client Option        Environment Variable         Purpose
======================== ==================== ============================ ==================================================
``tls_plugin``           ``TLS_plugin``       ``FDB_TLS_PLUGIN``           Path to the file to be loaded as the TLS plugin
``tls_certificate_file`` ``TLS_cert_path``    ``FDB_TLS_CERTIFICATE_FILE`` Path to the file from which the local certificates
                                                                           can be loaded, used by the plugin
``tls_key_file``         ``TLS_key_path``     ``FDB_TLS_KEY_FILE``         Path to the file from which to load the private
                                                                           key, used by the plugin
``tls_verify_peers``     ``TLS_verify_peers`` ``FDB_TLS_VERIFY_PEERS``     The byte-string for the verification of peer
                                                                           certificates and sessions, used by the plugin
======================== ==================== ============================ ==================================================

The value for each setting can be specified in more than one way.  The actual valued used is determined in the following order:

1. An explicity specified value as a command-line option or client option, if one is given;
2. The value of the environment variable, if one has been set;
3. The default value

As with all other command-line options to ``fdbserver``, the TLS settings can be specified in the :ref:`[fdbserver] section of the configuration file <foundationdb-conf-fdbserver>`.

The settings for certificate file, key file, and peer verification are interpreted by the loaded plugin.

Default Values
--------------

Plugin default location
^^^^^^^^^^^^^^^^^^^^^^^

Similarly, if a value is not specified for the parameter ``tls_plugin``, the file will be specified by the environment variable ``FDB_TLS_PLUGIN`` or, if this variable is not set, the system-dependent location:

  * Linux: ``/usr/lib/foundationdb/plugins/FDBLibTLS.so``
  * macOS: ``/usr/local/foundationdb/plugins/FDBLibTLS.dylib``
  * Windows: ``C:\Program Files\foundationdb\plugins\FDBLibTLS.dll``

On Windows, this location will be relative to the chosen installation location. The environment variable ``FOUNDATIONDB_INSTALL_PATH`` will be used in place of ``C:\Program Files\foundationdb\`` to determine this location.

Certificate file default location
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default behavior when the certificate or key file is not specified is to look for a file named ``fdb.pem`` in the current working directory. If this file is not present, an attempt is made to load a file from a system-dependent location:

  * Linux: ``/etc/foundationdb/fdb.pem``
  * macOS: ``/usr/local/etc/foundationdb/fdb.pem``
  * Windows: ``C:\ProgramData\foundationdb\fdb.pem``

Default Peer Verification
^^^^^^^^^^^^^^^^^^^^^^^^^

The default peer verification is the empty string.

Parameters and client bindings
------------------------------

When loading a TLS plugin from a non-default location when using a client binding, the ``TLS_PLUGIN`` network option must be specified before any other TLS option. Because a loaded TLS plugin is allowed to reject the values specified in the other options, the plugin load operation will be forced by specifying one of the other options, if it not already specified.

The default LibreSSL-based plugin
=================================

FoundationDB offers a TLS plugin based on the LibreSSL library. By default, it will be loaded automatically when participating in a TLS-enabled cluster.

For the plugin to operate, each process (both server and client) must have an X509 certificate, its corresponding private key, and potentially the certificates with which is was signed. When a process begins to communicate with a FoundationDB server process, the peer's certificate is checked to see if it is trusted and the fields of the peer certificate are verified. Peers must share the same root trusted certificate, and they must both present certificates whose signing chain includes this root certificate.

If the local certificate and chain is invalid, a FoundationDB server process bound to a TLS address will not start. In the case of invalid certificates on a client, the client will be able to start but will be unable to connect any TLS-enabled cluster.

Formats
-------

The LibreSSL plugin can read certificates and their private keys in base64-encoded DER-formatted X.509 format (which is known as PEM). A PEM file can contain both certificates and a private key or the two can be stored in separate files.

Required files
--------------

A single file can contain the information as described in both of the following sections.

.. _tls-certificate-file:

The certificate file
^^^^^^^^^^^^^^^^^^^^

A file must be supplied that contains an ordered list of certificates. The first certificate is the process' own certificate. Each following certificate must sign the one preceding it.

All but the last certificate are provided to peers during TLS handshake as the certificate chain.

The last certificate in the list is the trusted certificate. All processes that want to communicate must have the same trusted certificate.

.. note:: If the certificate list contains only one certificate, that certificate *must* be self-signed and will be used as both the certificate chain and the trusted certificate.

Each of these certificates must be in the form::

  --------BEGIN CERTIFICATE--------
  xxxxxxxxxxxxxxx
  --------END CERTIFICATE--------

.. _tls-key-file:

The key file
^^^^^^^^^^^^

The key file must contain the private key corresponding to the process' own certificate. The private key must be in PKCS#8 format, which means that it must be surrounded by::

  -----BEGIN PRIVATE KEY-----
  xxxxxxxxxxxxxxx
  -----END PRIVATE KEY-----

Certificate creation
--------------------

If your organization already makes use of certificates for access control and securing communications, you should ask your security expert for organizational procedure for obtaining and verifying certificates. If the goal of enabling TLS is to make sure that only known machines can join or access the FoundationDB cluster and for securing communications, then creating your own certificates can serve these purposes.

The following set of commands uses the OpenSSL command-line tools to create a self-signed certificate and private key. The certificate is then joined with the private key in the output ``fdb.pem`` file::

  user@host:> openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout private.key -out cert.crt
  user@host:> cat cert.crt private.key > fdb.pem

Peer verification
=================

A FoundationDB server or client will only communicate with peers that present a certificate chain that meets the verification requirements. By default, the only requirement is that the provided certificate chain ends in a certificate signed by the local trusted certificate.

.. _tls-verify-peers:

Certificate field verification
------------------------------

With a peer verification string, FoundationDB servers and clients can adjust what is required of the certificate chain presented by a peer. These options can make the certificate requirements more rigorous or more lenient.

Turning down the validation
^^^^^^^^^^^^^^^^^^^^^^^^^^^

If the default checking of the certificate chain is too stringent, the verification string can contain settings to weaken what certificates are accepted.

=====================  =============================================================
Setting                Result
=====================  =============================================================
``Check.Valid=0``      Sets the current process to disable all further verification 
                       of a peer certificate.
``Check.Unexpired=0``  Disables date checking of peer certificates. If the clocks in 
                       the cluster and between the clients and servers are not to be 
                       trusted, setting this value to ``0`` can allow communications 
                       to proceed.
=====================  =============================================================

Adding verification requirements
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Requirements can be placed on the fields of the Issuer and Subject DNs in the peer's own certificate. These reqirements take the form of a comma-separated list of conditions. Each condition takes the form of ``field=value``. Only certain fields from a DN can be matched against.

======  ===================
Field   Well known name
======  ===================
``CN``  Common Name
``C``   County
``L``   Locality
``ST``  State
``O``   Organization
``OU``  Organizational Unit
======  ===================

The field of each condition may optionally have a DN prefix, which is otherwise considered to be for the Subject DN.

============================= ========
Prefix                        DN
============================= ========
``S.``, ``Subject.``, or none Subject
``I.``, or ``Issuer.``        Issuer
============================= ========

The value of a condition must be specified in a form derived from a subset of `RFC 4514 <http://www.ietf.org/rfc/rfc4514.txt>`_. Specifically, the "raw" notation (a value starting with the ``#`` character) is not accepted. Other escaping mechanisms, including specifying characters by hex notation, are allowed. The specified field's value must exactly match the value in the peer's certificate.

By default, the fields of a peer certificate's DNs are not examined.

Verification Examples
^^^^^^^^^^^^^^^^^^^^^

A verification string can be of the form::

  Check.Unexpired=0,I.C=US,C=US,S.O=XYZCorp\, LLC

This verification string would:

* Skip the check on all peer certificates that the certificate is not yet expired
* Require that the Issuer have a Country field of ``US``
* Require that the Subject have a Country field of ``US``
* Require that the Subject have a Organization field of ``XYZCorp, LLC``

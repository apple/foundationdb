########################
Transport Layer Security
########################

.. include:: guide-common.rst.inc

Introduction
============

Transport Layer Security (TLS) and its predecessor, Secure Sockets Layer (SSL), are protocols designed to provide communication security over public networks. Users exchange a symmetric session key that is used to encrypt data exchanged between the parties.

By default, a FoundationDB cluster uses *unencrypted* connections among client and server processes. This document describes the `Transport Layer Security <http://en.wikipedia.org/wiki/Transport_Layer_Security>`_ (TLS) capabilities of FoundationDB, which enable security and authentication through a public/private key infrastructure. TLS is compiled into each FoundationDB binary. This document will describe the basic TLS capabilities of FoundationDB and document its implementation, which is based on `LibreSSL <https://www.libressl.org/>`_. TLS-enabled servers will only communicate with other TLS-enabled servers and TLS-enabled clients. Therefore, a cluster's machines must all enable TLS in order for TLS to be used.


Setting Up FoundationDB to use TLS
==================================

.. _enable-TLS:

Enabling TLS in a new cluster
-----------------------------

To set a new cluster to use TLS, use the ``-t`` flag on ``make_public.py``::

    user@host1$ sudo /usr/lib/foundationdb/make_public.py -t
    /etc/foundationdb/fdb.cluster is now using address 10.0.1.1 (TLS enabled)

This will configure the new cluster to communicate with TLS.

.. note:: Depending on your operating system, version and configuration, there may be a firewall in place that prevents external access to certain ports. If necessary, please consult the appropriate documentation for your OS and ensure that all machines in your cluster can reach the ports configured in your :ref:`configuration file <foundationdb-conf>`.

.. _converting-existing-cluster-after-6.1:

Converting an existing cluster to use TLS (since v6.1)
======================================================

.. warning:: Release 6.2 removed the "connected_coordinators" field from status.

Since version 6.1, FoundationDB clusters can be converted to TLS without downtime. FoundationDB server can listen to TLS and unencrypted traffic simultaneously on two separate ports. As a result, FDB clusters can live migrate to TLS:

1) Restart each FoundationDB server individually, but with an additional listen address for TLS traffic::

     /path/to/fdbserver -C fdb.cluster -p 127.0.0.1:4500 -p 127.0.0.1:4600:tls

   Since, the server still listens to unencrypted traffic and the cluster file still contains the old address, rest of the processes will be able to talk to this new process.

2) Once all processes are listening to both TLS and unencrypted traffic, switch one or more coordinator to use TLS. Therefore, if the old coordinator list was ``127.0.0.1:4500,127.0.0.1:4501,127.0.0.1:4502``, the new one would be something like ``127.0.0.1:4600:tls,127.0.0.1:4501,127.0.0.1:4502``. Switching few coordinators to TLS at a time allows a smoother migration and a window to find out clients who do not yet have TLS configured. The number of coordinators each client can connect to can be seen via  ``fdbstatus`` (look for ``connected_coordinators`` field in ``clients``)::

    "clients" : {
        "count" : 2,
        "supported_versions" : [
            {
                "client_version" : "6.1.0",
                "connected_clients" : [
                    {
                        "address" : "127.0.0.1:42916",
                        "connected_coordinators": 3,
                        "log_group" : "default"
                    },
                    {
                        "address" : "127.0.0.1:42918",
                        "connected_coordinators": 2,
                        "log_group" : "default"
                    }
                ]
            }, ...
        ]
    }

3) If there exist a client (e.g., the client 127.0.0.1:42918 in the above example) that cannot connect to all coordinators after a coordinator is switched to TLS, it mean the client does not set up its TLS correctly. System operator should notify the client to correct the client's TLS configuration. Otherwise, when all coordinators are switched to TLS ports, the client will loose connection.

4) Repeat (2) and (3) until all the addresses in coordinator list are TLS.

5) Restart each FoundationDB server, but only with one public address that listens to TLS traffic only.

.. _converting-existing-cluster-before-6.1:

Converting an existing cluster to use TLS (< v6.1)
==================================================

Enabling TLS on an existing (non-TLS) cluster cannot be accomplished without downtime because all processes must have TLS enabled to communicate. At startup, each server process enables TLS if the addresses in its cluster file are TLS-enabled. As a result, server processes must be stopped and restarted to convert them to use TLS. To convert the cluster to TLS in the most conservative way:

1) Stop all FoundationDB clients and :ref:`server processes <administration-running-foundationdb>` in the cluster.

2) Change all cluster files to have the ``:tls`` suffix for each coordinator.

3) Restart the cluster and the clients.

.. _configuring-tls:

Configuring TLS
==========================

The operation of TLS is configured through five settings. These settings can be provided as command-line options, client options, or environment variables, and are named as follows:

======================== ==================== ============================ ==================================================
Command-line Option      Client Option        Environment Variable         Purpose
======================== ==================== ============================ ==================================================
``tls_certificate_file`` ``TLS_cert_path``    ``FDB_TLS_CERTIFICATE_FILE`` Path to the file from which the local certificates
                                                                           can be loaded
``tls_key_file``         ``TLS_key_path``     ``FDB_TLS_KEY_FILE``         Path to the file from which to load the private
                                                                           key
``tls_verify_peers``     ``TLS_verify_peers`` ``FDB_TLS_VERIFY_PEERS``     The byte-string for the verification of peer
                                                                           certificates and sessions
``tls_password``         ``TLS_password``     ``FDB_TLS_PASSWORD``         The byte-string representing the passcode for
                                                                           unencrypting the private key
``tls_ca_file``          ``TLS_ca_path``      ``FDB_TLS_CA_FILE``          Path to the file containing the CA certificates
                                                                           to trust
======================== ==================== ============================ ==================================================

The value for each setting can be specified in more than one way.  The actual valued used is determined in the following order:

1. An explicitly specified value as a command-line option or client option, if one is given;
2. The value of the environment variable, if one has been set;
3. The default value

For the password, rather than using the command-line option, it is recommended to use the environment variable ``FDB_TLS_PASSWORD``, as command-line options are more visible to other processes running on the same host.

As with all other command-line options to ``fdbserver``, the TLS settings can be specified in the :ref:`[fdbserver] section of the configuration file <foundationdb-conf-fdbserver>`.

The settings for certificate file, key file, peer verification, password and CA file are interpreted by the software.

Default Values
--------------

Certificate file default location
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The default behavior when the certificate or key file is not specified is to look for files named ``cert.pem`` or  ``key.pem`` respectively, in system-dependent locations as follows:

* Linux: ``/etc/foundationdb/cert.pem`` and ``/etc/foundationdb/key.pem``
* macOS: ``/usr/local/etc/foundationdb/cert.pem`` and ``/usr/local/etc/foundationdb/key.pem``
* Windows: ``C:\ProgramData\foundationdb\cert.pem`` and ``C:\ProgramData\foundationdb\key.pem``

Default Peer Verification
^^^^^^^^^^^^^^^^^^^^^^^^^

The default peer verification is ``Check.Valid=1``.

Default Password
^^^^^^^^^^^^^^^^

There is no default password. If no password is specified, it is assumed that the private key is unencrypted.

Permissions
-----------

All files used by TLS must have sufficient read permissions such that the user running the FoundationDB server or client process can access them. It may also be necessary to have similar read permissions on the parent directories of the files used in the TLS configuration.

Automatic TLS certificate refresh
---------------------------------

The TLS certificate will be automatically refreshed on a configurable cadence. The server will inspect the CA, certificate, and key files in the specified locations periodically, and will begin using the new versions if following criterion were met:

* They are changed, judging by the last modified time.
* They are valid certificates.
* The key file matches the certificate file.

The refresh rate is controlled by ``--knob-tls-cert-refresh-delay-seconds``. Setting it to 0 will disable the refresh.

The default LibreSSL-based implementation
=========================================

FoundationDB offers TLS based on the LibreSSL library. By default, it will be enabled automatically when participating in a TLS-enabled cluster.

For TLS to operate, each process (both server and client) must have an X509 certificate, its corresponding private key, and the certificates with which it was signed. When a process begins to communicate with a FoundationDB server process, the peer's certificate is checked to see if it is trusted and the fields of the peer certificate are verified. Peers must share the same root trusted certificate, and they must both present certificates whose signing chain includes this root certificate.

If the local certificate and chain is invalid, a FoundationDB server process bound to a TLS address will not start. In the case of invalid certificates on a client, the client will be able to start but will be unable to connect any TLS-enabled cluster.

Formats
-------

LibreSSL can read certificates and their private keys in base64-encoded DER-formatted X.509 format (which is known as PEM). A PEM file can contain both certificates and a private key or the two can be stored in separate files.

Required files
--------------

A single file can contain the information as described in both of the following sections.

.. _tls-certificate-file:

The certificate file
^^^^^^^^^^^^^^^^^^^^

A file must be supplied that contains an ordered list of certificates. The first certificate is the process' own certificate. Each following certificate must sign the one preceding it.

All but the last certificate are provided to peers during TLS handshake as the certificate chain.

The last certificate in the list is the trusted certificate.

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

It can optionally be encrypted by the password provided to tls_password.

Certificate creation
--------------------

If your organization already makes use of certificates for access control and securing communications, you should ask your security expert for organizational procedure for obtaining and verifying certificates. If the goal of enabling TLS is to make sure that only known machines can join or access the FoundationDB cluster and for securing communications, then creating your own certificates can serve these purposes.

The following set of commands uses the OpenSSL command-line tools to create a self-signed certificate and private key::

  user@host:> openssl req -x509 -nodes -days 365 -newkey rsa:2048 -keyout key.pem -out cert.pem

Optionally, the certificate can be joined with the private key as supplied as both certificate and key files::

  user@host:> cat cert.crt private.key > fdb.pem

Peer verification
=================

A FoundationDB server or client will only communicate with peers that present a certificate chain that meets the verification requirements. By default, the only requirement is that the provided certificate chain ends in a certificate signed by the local trusted certificate.

.. _tls-verify-peers:

Certificate field verification
------------------------------

With a peer verification string, FoundationDB servers and clients can adjust what is required of the certificate chain presented by a peer. These options can make the certificate requirements more rigorous or more lenient. You can specify multiple verification strings by providing additional tls_verify_peers command line arguments or concatenating them with ``|``. All ``,`` or ``|`` in the verify peers fields should be escaped with ``\``.

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

Requirements can be placed on the fields of the Issuer and Subject DNs in the peer's own certificate. These requirements take the form of a comma-separated list of conditions. Each condition takes the form of ``field[<>]?=value``. Only certain fields from a DN can be matched against.

=======  ===================
Field    Well known name
=======  ===================
``CN``   Common Name
``C``    Country
``L``    Locality
``ST``   State
``O``    Organization
``OU``   Organizational Unit
``UID``  Unique Identifier
``DC``   Domain Component
=======  ===================

The field of each condition may optionally have a DN prefix, which is otherwise considered to be for the Subject DN.

============================= ========
Prefix                        DN
============================= ========
``S.``, ``Subject.``, or none Subject
``I.``, or ``Issuer.``        Issuer
``R.``, or ``Root.``          Root
============================= ========

Additionally, the verification can be restricted to certificates signed by a given root CA with the field ``Root.CN``. This allows you to have different requirements for different root chains.

The value of a condition must be specified in a form derived from a subset of `RFC 4514 <http://www.ietf.org/rfc/rfc4514.txt>`_. Specifically, the "raw" notation (a value starting with the ``#`` character) is not accepted. Other escaping mechanisms, including specifying characters by hex notation, are allowed. The specified field's value must exactly match the value in the peer's certificate.

By default, the fields of a peer certificate's DNs are not examined.

In addition to DNs, restrictions can be placed against X509 extensions. They are specified in the same fashion as DN requirements.  The supported extensions are:

==================  ========================
Field               Well known name
==================  ========================
``subjectAltName``  Subject Alternative Name
==================  ========================

Within a subject alternative name requirement, the value specified is required to have the form ``prefix:value``, where the prefix specifies the type of value being matched against.  The following prefixes are supported:

======  ===========================
Prefix  Well known name
======  ===========================
DNS     Domain Name
URI     Uniform Resource Identifier
IP      IP Address
EMAIL   Email Address
======  ===========================

The following operators are supported:

=========  ============
Operator   Match Type
=========  ============
``=``      Exact Match
``>=``     Prefix Match
``<=``     Suffix Match
=========  ============

Verification Examples
^^^^^^^^^^^^^^^^^^^^^

Let's consider a certificate, whose abridged contents is::

    Certificate:
        Data:
            Version: 3 (0x2)
            Serial Number: 12938646789571341173 (0xb38f4eb406a5eb75)
        Signature Algorithm: sha1WithRSAEncryption
            Issuer: C=US, ST=California, L=Cupertino, O=Apple Inc., OU=FDB Team
            Subject: C=US, ST=California, L=Cupertino, O=Apple Inc., OU=FDB Team
            X509v3 extensions:
                X509v3 Subject Alternative Name: 
                    DNS:test.foundationdb.org
                    DNS:prod.foundationdb.com

A verification string of::

  Check.Unexpired=0,I.C=US,C=US,S.O=Apple Inc.

Would pass, and:

* Skip the check on all peer certificates that the certificate is not yet expired
* Require that the Issuer has a Country field of ``US``
* Require that the Subject has a Country field of ``US``
* Require that the Subject has a Organization field of ``Apple Inc.``

A verification string of::

  S.OU>=FDB,S.OU<=Team,S.subjectAltName=DNS:test.foundationdb.org

Would pass, and:

* Require that the Subject has an Organization field that starts with ``FDB``
* Require that the Subject has an Organization field that ends with ``Team``
* Require that the Subject has a Subject Alternative Name extension, which has one or more members of type DNS with a value of ``test.foundationdb.org``.

A verification string of::

  S.subjectAltName>=DNS:prod.,S.subjectAltName<=DNS:.org

Would pass, and:

* Require that the Subject has a Subject Alternative Name extension, which has one or more members of type DNS that begins with the value ``prod.``.
* Require that the Subject has a Subject Alternative Name extension, which has one or more members of type DNS that ends with the value ``.org``.

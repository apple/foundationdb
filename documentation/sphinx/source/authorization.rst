#############
Authorization
#############

.. warning :: Authorization is currently experimental and is not recommended for use in production.

Introduction
============

:ref:`Multi-tenant <multi-tenancy>` database implies a couple of new concepts that did not previously exist in FoundationDB.
The first is the concept of privilege levels: we have clients whose typical workload is limited to accessing a tenant keyspace.
On the other hand, we have *administrators* who may read or update cluster-wide configurations through system keyspaces.
These operations also include creation and deletion of tenants.
The second is access control: with multiple tenant keyspaces, it comes naturally that we would want to restrict database access of a client to a subset of them.

Privilege Levels
----------------

Authorization feature extends FoundationDB's existing TLS policy to distinguish administrators from data clients,
making TLS configuration a prerequisite for enabling authorization.
There are only two privilege levels: *trusted* versus *untrusted* clients.
Trusted clients are authorized to perform any operation that pre-authorization FoundationDB clients used to do, including accessing the system keyspace.
Untrusted clients may only request what is necessary to access tenant keyspaces for which they are authorized.
Untrusted clients are blocked from accessing anything in the system keyspace or issuing management operations that modifies the cluster in any way.

In order to be considered a trusted client, a client needs to be :ref:`configured with a valid chain of X.509 certificates and keys <enable-TLS>`,
and its certificate chain must be trusted by the server.
If the server was configured with trusted IP subnets, i.e. run with one or more ``--trusted-subnet-SUBNET_NAME`` followed by a CIDR block decribing the subnet,
then the client's IP as seen from the server must also belong to one of the subnets.

Choosing to respond with empty certificate chain during `client authentication <https://www.rfc-editor.org/rfc/rfc5246#section-7.4.6>`_,
or not belonging to the trusted subnet marks the client as untrusted.

.. note:: Presenting a bad or untrusted certificate chain causes the server to break the connection and eventually throttle the client.
          It does not let the client connect untrusted.

Access Control
--------------

To restrict client access only to a pertinent subset of tenant keyspaces, authorization feature allows database administrators
to grant tenant-scoped access in the form of `JSON Web Tokens <https://www.rfc-editor.org/rfc/rfc7519>`_.
Token verification is performed against a set of named public keys written in `JWK Set <https://www.rfc-editor.org/rfc/rfc7517#section-5>`_ format.
A token's header part must contain the key identifier (or `kid <https://www.rfc-editor.org/rfc/rfc7515.html#section-4.1.4>`_) of the public key which shall be used to verify itself.
Below is the list of token fields recognized by FoundationDB.
Note that some of the fields are *recognized* by FoundationDB but not actively used in enforcing security, pending future implementation.
Those fields are marked as *NOT required*.

=============== =========== ======== ==================================================== ===========================================================================
Containing Part Field Name  Required Purpose                                              Reference
=============== =========== ======== ==================================================== ===========================================================================
Header          ``typ``     YES      Type of JSON Web Signature. Must be ``JWT``.         `RFC7515 4.1.9 <https://www.rfc-editor.org/rfc/rfc7515#section-4.1.9>`_
Header          ``alg``     YES      Algorithm used to generate the signature. Only       `RFC7515 4.1.1 <https://www.rfc-editor.org/rfc/rfc7515#section-4.1.1>`_
                                     ``ES256`` and ``RS256`` are supported.
                                     Must match the ``alg`` attribute of public key.
Header          ``kid``     YES      Name of public key with which to verify the token.   `RFC7515 4.1.4 <https://www.rfc-editor.org/rfc/rfc7515#section-4.1.4>`_
                                     Must match the ``kid`` attribute of public key.
Claim           ``exp``     YES      Timestamp after which token is not accepted.         `RFC7519 4.1.4 <https://www.rfc-editor.org/rfc/rfc7519#section-4.1.4>`_
Claim           ``nbf``     YES      Timestamp before which token is not accepted.        `RFC7519 4.1.5 <https://www.rfc-editor.org/rfc/rfc7519#section-4.1.5>`_
Claim           ``iat``     YES      Timestamp at which token was issued.                 `RFC7519 4.1.6 <https://www.rfc-editor.org/rfc/rfc7519#section-4.1.6>`_
Claim           ``tenants`` YES      Tenants for which token holder is authorized.        N/A
                                     Must be an array.
Claim           ``iss``     NO       Issuer of the token.                                 `RFC7519 4.1.1 <https://www.rfc-editor.org/rfc/rfc7519#section-4.1.1>`_
Claim           ``sub``     NO       Subject of the token.                                `RFC7519 4.1.2 <https://www.rfc-editor.org/rfc/rfc7519#section-4.1.2>`_
Claim           ``aud``     NO       Intended recipients of the token. Must be an array.  `RFC7519 4.1.3 <https://www.rfc-editor.org/rfc/rfc7519#section-4.1.3>`_
Claim           ``jti``     NO       String that uniquely identifies a token.             `RFC7519 4.1.7 <https://www.rfc-editor.org/rfc/rfc7519#section-4.1.7>`_
=============== =========== ======== ==================================================== ===========================================================================

Keys with which to verify the token must be serialized in a `JWK Set <https://www.rfc-editor.org/rfc/rfc7517#section-5>`_ format and stored in a file.
The location of the key set file must be passed as command line argument ``--authorization-public-key-file`` to the ``fdbserver`` executable.
Public keys in the set must be either `RSA <https://datatracker.ietf.org/doc/html/rfc7518#section-6.3>`_ public keys
containing ``n`` and ``e`` parameters, each containing `Base64urlUInt <https://www.rfc-editor.org/rfc/rfc7518#section-2>`_-encoded modulus and exponent.
or `Elliptic Curve <https://datatracker.ietf.org/doc/html/rfc7518#section-6.2>`_ public keys on a ``P-256`` curve,
where ``crv`` parameter is set to ``P-256`` and ``x`` and ``y`` parameters contain
`base64url <https://datatracker.ietf.org/doc/html/rfc4648#section-5>`_-encoded affine coordinates.
In addition, each public key JSON object in set must contain ``kty`` (set to ``EC`` or ``RSA``) field to indicate public key algorithm,
along with ``kid``, and ``alg`` fields to be compared against their token header counterparts.
Private keys are strongly recommended against being included in the public key set and, if found, are excluded from consideration.

.. note:: By design, FoundationDB authorization feature does not support revocation of outstanding tokens. Use extra caution in assigning long token durations.

Enabling clients to use Authorization Tokens
============================================

In order to use an untrusted client with an authorization token, a client must be configured to trust the server's CA,
but must not be configured to use its certificates and keys. More concretely, a client's ``TLS_CA_FILE`` must include the server's root CA certificate,
and a client must not be configured with its own ``TLS_CERTIFICATE_FILE`` or ``TLS_KEY_FILE``, neither programmatically nor by environment variable.
Before performing a tenant data read or update, a client must set transaction option ``AUTHORIZATION_TOKEN`` with the token string as argument.
It is the client's responsibility to keep the token up-to-date.

Public Key Rotation
===================

Public key set automatically refreshes itself based on the file's latest content every ``PUBLIC_KEY_FILE_REFRESH_INTERVAL_SECONDS``.
The in-memory set of public keys does not update unless the key file is a correct JWK set.

Token Caching
=============

In a single-threaded runtime environment such as FoundationDB, it is important not to let the main thread be bogged down by computationally expensive operations,
such as cryptographic signature verification. FoundationDB internally caches the tokens that are considered valid at the time of verification in a fixed-size LRU cache,
whose size may be configured using ``TOKEN_CACHE_SIZE`` knob.

.. note:: Token cache is independent of the active public key set. Once the token reaches the cache, it is valid until its expiration time,
          regardless of any key rotation that takes place thereafter.

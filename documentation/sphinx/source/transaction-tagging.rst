.. _transaction-tagging:

###################
Transaction Tagging
###################

FoundationDB provides the ability to add arbitrary byte-string tags to transactions.

This document describes how to configure and observe the behavior of transaction tagging and throttling.

Adding tags to transactions
===========================

Tags are added to transaction by using transaction options. Each transaction can include up to five tags, and each tag must not exceed 16 characters. There are two options that can be used to add tags:

* ``TAG`` - Adds a tag to the transaction. This tag will not be used for auto-throttling and is not included with read or commit requests. Tags set in this way can only be throttled manually.
* ``AUTO_THROTTLE_TAG`` - Adds a tag to the transaction that can be both automatically and manually throttled. To support busy tag detection, these tags may be sent as part of read or commit requests.

See the documentation for your particular language binding for details about setting this option.

.. note:: If setting hierarchical tags, it is recommended that you not use auto-throttle tags at multiple levels of the hierarchy. Otherwise, the cluster will favor throttling those tags set at higher levels, as they will include more transactions.

.. note:: Tags must be included as part of all get read version requests, and a sample of read and commit requests will include auto-throttled tags. Additionally, each tag imposes additional costs with those requests. It is therefore recommended that you not use excessive numbers or lengths of transaction tags.

Tag throttling overview
=======================

The cluster has the ability to enforce a transaction rate for particular tags.  FIXME: discuss how this is done.

Tag throttles have the following attributes:

* tag - the tag being throttled.
* rate - the desired number of transactions to start per second for this tag.
* priority - the maximum priority to throttle. All lower priorities will also have this throttle applied.
* auto/manual - whether this tag was throttled automatically or manually.
* expiration - the time that the throttle will expire.

It is possible for multiple throttles to apply to the same transaction. For example, the transaction could use a tag that is both automatically and manually throttled, there may be throttles set at multiple priorities greater than or equal to the priority of the transaction, or it may set multiple tags. In these cases, the lowest limit of any throttle affecting the transaction will be used.

The limit set by the throttles is not a hard limit, meaning that it may be possible for clients to start more transactions in aggregate than allowed by the limit during a given window. The cluster should react to this condition by reducing the number of transactions that can be started on each client going forward.

``tag_throttled`` error
=======================

When a transaction tag is throttled, this information will be communicated to the client as part of the get read version request, which will then fail with a ``tag_throttled`` error. This error is retryable by ``on_error`` and the transaction retry loops provided in the language bindings. Subsequent requests from the same client that exceed the limit will be throttled without sending a request to the cluster at all, except for periodic requests intended to poll for updates to the throttles.


Automatic transaction tag throttling
====================================

When using the ``AUTO_THROTTLE_TAG`` transaction option, the cluster will monitor activity for the chosen tags and may choose to reduce a tag's transaction rate limit if a storage server gets busy enough and has a sufficient portion of its traffic coming from that one tag.

When a tag is auto-throttled, the default priority transaction rate will be decreased to reduce the percentage of traffic attributable to that tag to a reasonable amount of total traffic on the affected storage server(s), and batch priority transactions for that tag will be stopped completely. 

Auto-throttles are created with a duration of a few minutes, at the end of which the cluster will try to gradually lift the limit. If the cluster detects that it needs to continue throttling the tag, then the duration of the throttle will be extended.

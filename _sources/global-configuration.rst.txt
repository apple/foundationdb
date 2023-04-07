.. _global-configuration:
.. default-domain:: cpp
.. highlight:: cpp

====================
Global Configuration
====================

The global configuration framework is an eventually consistent configuration
mechanism to efficiently make runtime changes to all clients and servers. It
works by broadcasting updates made to the global configuration key space,
relying on individual machines to store existing configuration in-memory.

The global configuration framework provides a key-value interface to all
processes and clients in a FoundationDB cluster.

The global configuration framework is internal to FoundationDB and clients will
usually have no need to interact with it. The API is provided here for
reference.

Reading data
------------

The global configuration framework is exposed through the
``GlobalConfig::globalConfig()`` static function. There are separate ways to
read a value, depending on if it is an object or a primitive.

.. function:: template<class T> const T get(KeyRef name, T defaultVal)

   Returns the value associated with ``name`` stored in global configuration,
   or ``defaultVal`` if no key matching ``name`` exists. This templated
   function is enabled only when the ``std::is_arithmetic<T>`` specialization
   returns true.

   .. code-block:: cpp
   
      auto& config = GlobalConfig::globalConfig();
      double value = config.get<double>("path/to/key", 1.0);

.. function:: const Reference<ConfigValue> get(KeyRef name)

   Returns the value associated with ``name`` stored in global configuration.

   .. code-block:: cpp
   
      auto& config = GlobalConfig::globalConfig();
      auto configValue = config.get("path/to/key");
   
      // Check if value exists
      ASSERT(configValue.value.has_value());
      // Cast to correct type
      auto str = std::any_cast<StringRef>(configValue.value);

.. function:: const std::map<KeyRef, Reference<ConfigValue>> get(KeyRangeRef range)

   Returns all values in the specified range.

.. type:: ConfigValue

   Holds a global configuration value and the arena where it lives. ::

     struct ConfigValue : ReferenceCounted<ConfigValue> {
        Arena arena;
        std::any value;
     }

   ``arena``
       The arena where the value (and the associated key) lives in memory.

   ``value``
       The stored value.

Writing data
------------

Writing data to global configuration is done using transactions written to the
special key space range ``\xff\xff/global_config/ - \xff\xff/global_config/0``.
Values must always be encoded according to the :ref:`api-python-tuple-layer`.

.. code-block:: cpp

   // In GlobalConfig.actor.h
   extern const KeyRef myGlobalConfigKey;
   // In GlobalConfig.actor.cpp
   const KeyRef myGlobalConfigKey = LiteralStringRef("config/key");
   
   // When you want to set the value..
   Tuple value = Tuple().appendDouble(1.5);
   
   FDBTransaction* tr = ...;
   tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
   tr->set(GlobalConfig::prefixedKey(myGlobalConfigKey), value.pack());
   // commit transaction

The client is responsible for avoiding conflicts with other global
configuration keys. For most uses, it is recommended to create a new key space.
For example, an application that wants to write configuration data should use
the ``global_config/config/`` namespace, instead of storing keys in the top
level ``global_config/`` key space.

^^^^^^^^^^^^^
Clearing data
^^^^^^^^^^^^^

Data can be removed from global configuration using standard transaction
semantics. Submit a clear or clear range to the appropriate global
configuration keys in the special key space to clear data.

Watching data
-------------

Global configuration provides functionality to watch for changes.

.. function:: Future<Void> onInitialized()

   Returns a ``Future`` which will be triggered when global configuration has
   been successfully initialized and populated with data.

.. function:: Future<Void> onChange()

   Returns a ``Future`` which will be triggered when any key-value pair in
   global configuration changes.

.. function:: void trigger(KeyRef key, std::function<void(std::optional<std::any>)> fn)

   Registers a callback to be called when the value for global configuration
   key ``key`` is changed. The callback function takes a single argument, an
   optional which will be populated with the updated value when ``key`` is
   changed, or an empty optional if the value was cleared. If the value is an
   allocated object, its memory remains in control of global configuration.

############
Clang-Format
############

``clang-format`` enforces consistent code style across the FoundationDB codebase. It runs as part of CI on every pull request. If your changes introduce formatting violations, the PR build will fail.

Configuration is in the ``.clang-format`` file at the repository root, based on the Mozilla style with FDB-specific adjustments (120 column limit, tab width 4, etc.).

Running clang-format locally
============================

Format only your changed lines
-------------------------------

Use ``git clang-format`` to format only the lines you changed, avoiding reformatting untouched code:

.. code-block:: shell

   # Format staged changes (before committing)
   git clang-format

   # Format changes between your branch and main
   git clang-format origin/main

   # Preview what would change (dry run)
   git clang-format --diff origin/main

Format specific files
---------------------

If ``git clang-format`` complains about unstaged changes, or you want to format an entire file (not just your changed lines), use ``clang-format`` directly:

.. code-block:: shell

   clang-format -i path/to/file.cpp

   # Format multiple files
   clang-format -i fdbserver/DataDistribution.actor.cpp fdbclient/SystemData.cpp

.. note::

   This reformats the entire file, not just your changes. This may produce a large diff if the file was not previously formatted.

IDE integration
===============

**VS Code** (with clangd extension):

1. Open Settings (Cmd+,)
2. Set **Editor: Default Formatter** to ``llvm-vs-code-extensions.vscode-clangd``
3. Check **Editor: Format On Save**
4. Set **Editor: Format On Save Mode** to ``file``

Files will be automatically formatted on save using the ``.clang-format`` configuration.

Key style rules
===============

The ``.clang-format`` configuration enforces:

* **Column limit**: 120 characters
* **Indentation**: Tabs with width 4
* **Braces**: Attach style (opening brace on same line)
* **Arguments**: One per line when they don't fit
* **Pointer alignment**: Left (``int* p``, not ``int *p``)
* **Short functions**: Inline functions may be on a single line

See the ``.clang-format`` file in the repository root for the full configuration.

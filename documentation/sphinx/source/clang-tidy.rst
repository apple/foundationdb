##########
Clang-Tidy
##########

``clang-tidy`` is a static analysis tool that detects common programming errors, enforces coding standards, and suggests modern C++ improvements.
It runs as part of CI on every pull request targeting a branch that has a ``.clang-tidy`` file (currently only ``main``). Findings are reported in the build log but do not currently fail the build. To enforce failures, add ``WarningsAsErrors: '*'`` to ``.clang-tidy``.

This guide explains how to run ``clang-tidy`` locally so you can fix issues before pushing.

What clang-tidy checks
======================

FoundationDB enables 13 checks configured in the ``.clang-tidy`` file at the repository root. The
intent is to enable more as we go forward. Here are some example rules:

* **3 Bugprone rules** -- catch potential runtime errors (e.g., ``bugprone-use-after-move``)
* **4 Modernize rules** -- encourage modern C++ practices (e.g., ``modernize-use-auto``, ``modernize-use-override``)
* **1 Performance rule** -- avoid unnecessary copies (``performance-for-range-copy``)
* **5 Readability rules** -- improve code clarity (e.g., ``readability-container-contains``, ``readability-container-size-empty``)

Basic examples of ``clang-tidy`` style and performance improvement changes:

.. code-block:: cpp

   // Use empty() instead of size() == 0
   -if (str.size() == 0) {
   +if (str.empty()) {

   // Use contains() instead of count() > 0
   -if (classPath.count(path) > 0) {
   +if (classPath.contains(path)) {

   // Avoid unnecessary copies in range-for loops
   -for (auto state : states)
   +for (const auto& state : states)

   // Remove unnecessary const on return types
   -const Key keyServersKey(const KeyRef& k) {
   +Key keyServersKey(const KeyRef& k) {

clang-tidy vs :doc:`clang-format`
=================================

Both run in CI but serve different purposes:

.. list-table::
   :header-rows: 1
   :widths: 15 40 45

   * - Feature
     - ``clang-format`` (Code Style)
     - ``clang-tidy`` (Code Quality)
   * - **Focus**
     - Whitespace, indentation, braces
     - Bugs, performance, modern practices
   * - **Speed**
     - Very fast (syntax-only)
     - Slower (requires compilation database)
   * - **Action**
     - Modifies files in-place (``-i``)
     - Reports violations (read-only)
   * - **Example**
     - ``if(x){...}`` to ``if (x) { ... }``
     - ``vec.size() == 0`` to ``vec.empty()``

Running clang-tidy locally
==========================

Step 1: Generate the compilation database
------------------------------------------

``clang-tidy`` needs a ``compile_commands.json`` file to understand include paths and macros. Add ``-DCMAKE_EXPORT_COMPILE_COMMANDS=ON`` when you configure your build:

.. code-block:: shell

   cmake -B ~/build_output -S . -G Ninja -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

Then symlink it to your source root:

.. code-block:: shell

   ln -sf ~/build_output/compile_commands.json .

.. note::

   A full build is recommended so that generated files (e.g., ``.actor.g.cpp`` headers) exist and include paths resolve correctly. Without a build, ``clang-tidy`` may report false errors on files that depend on generated code. If your ``compile_commands.json`` was generated on a different machine (e.g., Okteto), fix the paths with:

   .. code-block:: shell

      sed -i '' 's|/root/src/foundationdb/|/Users/YOU/checkouts/foundationdb/|g' compile_commands.json

Step 2: Locate the helper script
--------------------------------

``clang-tidy-diff.py`` runs ``clang-tidy`` only on the lines changed in your diff, avoiding noise from existing code you did not touch.

On macOS (with Homebrew LLVM):

.. code-block:: shell

   export TIDY_DIFF=$(find $(brew --prefix llvm)/share/clang -name "clang-tidy-diff.py")

On Linux:

.. code-block:: shell

   export TIDY_DIFF=$(find /usr/lib/llvm-*/share/clang -name "clang-tidy-diff.py" | head -n 1)

To make this permanent, add to your ``~/.bashrc`` or ``~/.zshrc``:

.. code-block:: shell

   alias fdb-tidy='python3 $(find $(brew --prefix llvm 2>/dev/null || echo "/usr/lib/llvm-*") -name "clang-tidy-diff.py" | head -n 1) -p 1 -path .'

Step 3: Run against your changes
---------------------------------

Check all changes between your branch and ``main``:

.. code-block:: shell

   git diff -U0 origin/main...HEAD | grep -v -E '\.actor\.cpp' | python3 "$TIDY_DIFF" -p 1 -path .

   # Or with the alias:
   git diff -U0 origin/main...HEAD | grep -v -E '\.actor\.cpp' | fdb-tidy

Check a specific commit:

.. code-block:: shell

   git show -U0 <commit_hash> | python3 "$TIDY_DIFF" -p 1 -path .

.. important::

   Use ``-U0`` (no context lines). Without it, ``clang-tidy`` may lint surrounding lines that you did not change, reporting pre-existing issues.

Understanding the flags
-----------------------

The ``clang-tidy-diff.py`` script has different flag conventions from ``clang-tidy`` itself:

* ``-p 1`` -- strip one directory prefix from git diff paths (``a/src/main.cpp`` becomes ``src/main.cpp``)
* ``-path .`` -- directory containing ``compile_commands.json``
* ``-quiet`` -- suppress per-file warning count summaries
* ``-j N`` -- run N clang-tidy instances in parallel

.. warning::

   In the standard ``clang-tidy`` command, ``-p`` points to the build directory. In ``clang-tidy-diff.py``, ``-path`` points to the build directory and ``-p`` is for path stripping.

Running clang-tidy during builds (CMake integration)
=====================================================

FoundationDB's CMake build supports running ``clang-tidy`` automatically during C/C++ compilation:

.. code-block:: shell

   cmake -S . -B build -G Ninja -DUSE_CLANG_TIDY=ON
   ninja -C build fdbserver

This runs ``clang-tidy`` on every file as it compiles. It is slower than using ``clang-tidy-diff.py`` on your diff but catches issues in all compiled code.

Optional CMake variables:

* ``CLANG_TIDY`` -- path to the ``clang-tidy`` executable (auto-detected by default)
* ``CLANG_TIDY_EXTRA_ARGS`` -- additional space-separated arguments passed to ``clang-tidy``

Known limitations
-----------------

**``.actor.cpp`` files cannot be analyzed.** These files use FoundationDB's custom actor compiler syntax (``ACTOR``, ``wait()``, ``state``) that ``clang-tidy`` cannot parse. Exclude them from your diff when running locally:

Quick reference
===============

.. code-block:: shell

   git diff -U0 origin/main...HEAD | grep -v -E '\.actor\.cpp' | python3 "$TIDY_DIFF" -p 1 -path .

**GCC-built compile_commands.json with clang-tidy.** If your ``compile_commands.json`` was generated with GCC, it may contain GCC-specific flags that ``clang-tidy`` (which uses the clang frontend) does not recognize. Add ``-extra-arg=-Wno-unknown-warning-option`` to suppress these errors:

.. code-block:: shell

   git diff -U0 origin/main...HEAD | python3 "$TIDY_DIFF" -p 1 -path . -extra-arg=-Wno-unknown-warning-option

Quick reference

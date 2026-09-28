=======================
For AI coding agents
=======================

This page is the one-hop overflow from the **AI coding agent entry point**
in the root ``README.rst``. Read that section first. The routing table and
known traps stay there.

Annotated repository map
========================

::

    aerospike-client-python/
    |__ README.rst            entry point
    |__ AGENTS.md             pointer to that section
    |__ VERSION               authoritative version
    |__ BUILD.md              building from source
    |__ doc/                  API reference source (Sphinx)
    |   |__ README.md         how to build the docs locally
    |   |__ for-ai-agents.rst this page
    |   |__ examples/         doc-embedded snippets
    |__ aerospike-stubs/      type stubs (.pyi)
    |__ aerospike_helpers/    Python-level API surface, NOT in src/
    |   |__ batch/            batch record types
    |   |__ expressions/      filter and operation expressions
    |   |__ operations/       list, map, bit, HLL, string, expression operations
    |   |__ metrics/          extended client-side metrics
    |   |__ cdt_ctx.py        CDT context for nested list/map access
    |__ examples/
    |   |__ client/           canonical code examples that show how to use individual API's
    |   |__ string_ops/       canonical code examples for string operations and expressions
    |   |__ run_all_examples.py     used to validate all the canonical code examples
    |__ test/                 see test/README.md
    |   |__ new_tests/        primary suite
    |   |__ standalone/       manual tests; see test/standalone/README.md
    |   |__ misc/             unmaintained tests
    |__ src/                  C extension for the aerospike module
    |__ aerospike-client-c/   C client sources used to build this client; not the Python API
    |__ benchmarks/           see benchmarks/README.rst

Rendered API docs: https://aerospike-python-client.readthedocs.io/en/latest/

Also named from root, for completeness:

* ``.github/workflows/local-server-setup/README.md`` — script to deploy aerospike server locally with EE features

Canonical reference application
===============================

A Python SubMilliPost is not in this repository. ``examples/client/`` shows
what a single API call does; it does not show when or why to use it at
feature scale. When a Python SubMilliPost is published, it will sit between
tests and ``examples/`` in the precedence list.

Precedence when sources disagree
================================

Anything that contradicts the API reference is stale and should be reported,
not followed.

The API reference in ``doc/`` is hand-maintained rather than generated from the
C extension, so it can drift. Use the stubs as the cross-check.

1. ``doc/`` for signatures and parameter semantics — it is the API reference.
   How to build it locally: ``doc/README.md``.
2. ``aerospike-stubs/*.pyi`` as the cross-check against the C extension.
3. ``test/new_tests/`` for actual behavior, including edge cases.
   How to run the suite: ``test/README.md``.
4. ``examples/client/`` for single-call usage.
5. https://aerospike.com/docs for server-side semantics and version gates.

Trap notes
==========

The known traps live in the **AI coding agent entry point** in the root
``README.rst``. Notes below are extra detail.

* ``examples/client/operate.py`` shows the helper-function pattern.
* ``batch_read`` projects bins with the ``bins`` argument;
  ``client.select()`` is the single-record form;
  ``Query.select()`` / ``Scan.select()`` project bins on a query or scan.
* Nested CDT removal: pass ``RemoveResult().compile()`` as the modifying
  expression to ``aerospike_helpers.operations.operations.modify_by_path``
  (or ``ModifyByPath``). The path-modify flags do not delete.
  See ``aerospike_helpers.expressions.base.RemoveResult``.

Verifying generated code
========================

These commands need a running Aerospike server. Copy
``test/config.conf.template`` to ``test/config.conf`` and set the hosts
before running tests. Details: ``test/README.md``.

From the repository root::

    python3 -m examples.run_all_examples CE
    python3 -m pytest test/new_tests

``CE`` runs the community examples under ``examples/client/`` and
``examples/string_ops/``. ``EE`` runs the admin examples, which need
Aerospike Enterprise::

    python3 -m examples.run_all_examples EE

Optional, language-specific: many snippets in ``doc/`` are executable.
From ``doc/``::

    sphinx-build -b doctest . doctest

A passing examples run shows that the documented call did not throw against
the connected cluster. A passing ``test/new_tests`` run covers API behavior
and edge cases. Neither proves the generated code chose the right command
(batch versus a loop of single-record calls), set ``POLICY_KEY_SEND`` when
the user key matters, or composed operations from ``aerospike_helpers``.
Check those against the traps in the root README.

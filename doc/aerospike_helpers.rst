.. aerospike\_helpers:

*************************************************************************************************************
:mod:`aerospike\_helpers` --- Aerospike Helper Package for bin operations (list, map, bit, etc.), and metrics
*************************************************************************************************************

This package contains:

* Helpers to be used by the operate and operate_ordered methods for bin operations. (list, map, bitwise, etc.)
* Classes for metrics callbacks.
* The base class for a custom seed provider (see :mod:`aerospike_helpers.discovery`)
* The HyperLogLog data type
* The :class:`~aerospike_helpers.Vector` data type for vector similarity search (see :ref:`aerospike_helpers_vectors`)

.. automodule:: aerospike_helpers
    :members:
    :undoc-members:
    :show-inheritance:

.. _aerospike_helpers_vectors:

Vectors
=======

A :class:`~aerospike_helpers.Vector` bin holds a fixed-length array of numbers, such as an
embedding produced by a machine learning model. Comparing a query vector to stored vectors finds
the records most similar to it (a nearest-neighbor search).

.. note:: Requires server version >= 8.2.1

Each vector has one element type. The number of dimensions must be at least 1, and the elements
can take up at most 262144 bytes:

============  =========================================================  ==============
Element type  Factory                                                    Max dimensions
============  =========================================================  ==============
``FLOAT16``   :meth:`~aerospike_helpers.Vector.of_float16` or            131072
              :meth:`~aerospike_helpers.Vector.of_float16_bits`
``INT32``     :meth:`~aerospike_helpers.Vector.of_int32`                 65536
``FLOAT32``   :meth:`~aerospike_helpers.Vector.of_float32`               65536
``FLOAT64``   :meth:`~aerospike_helpers.Vector.of_float64`               32768
============  =========================================================  ==============

The factories check every element before anything is sent to the server. They raise
:exc:`TypeError` for an element of the wrong type, and :exc:`ValueError` for an element that is
NaN, infinite, or out of range for the element type.

A vector similarity search has these steps:

1. Store each vector as a top-level bin value, for example
   ``client.put(key, {"embedding": Vector.of_float32(values)})``. A vector can't be nested in a
   list or map. Reading the bin back returns a :class:`~aerospike_helpers.Vector`, whose
   :attr:`~aerospike_helpers.Vector.value` holds the elements.
2. Build a query vector with the same element type and number of dimensions as the stored
   vectors.
3. Compute the distance between the bin and the query vector with
   :class:`~aerospike_helpers.expressions.vector.VectorDistance`, using a
   :class:`~aerospike_helpers.expressions.vector.VectorDistanceMetric`. If a record's vector has
   a different element type or number of dimensions, the expression evaluates to unknown for it.
4. Filter records by the distance in an expression, or find the *k* best matches: project the
   distance as a result bin with
   :meth:`~aerospike_helpers.operations.expression_operations.expression_read`, and order by
   that bin with :meth:`Query.order_by <aerospike.Query.order_by>` and
   :attr:`Query.top_k <aerospike.Query.top_k>`.

Order the results in the direction that puts the best matches first:
:attr:`~aerospike.QueryOrder.ASCENDING` for ``EUCLIDEAN_SQUARED``, and
:attr:`~aerospike.QueryOrder.DESCENDING` for ``DOT_PRODUCT`` and ``COSINE_SIMILARITY``. This
is also described in :class:`~aerospike_helpers.expressions.vector.VectorDistanceMetric`.

.. seealso::
    :meth:`Query.order_by <aerospike.Query.order_by>` for a complete Top-K search example, and
    ``examples/client/vector_topk_query.py`` in the client repository for a runnable script.

String Helpers
==============

.. automodule:: aerospike_helpers.string_helpers
    :members:
    :undoc-members:
    :show-inheritance:

Subpackages
===========

.. toctree::

    aerospike_helpers.operations
    aerospike_helpers.expressions
    aerospike_helpers.cdt_ctx
    aerospike_helpers.batch
    aerospike_helpers.metrics
    aerospike_helpers.discovery

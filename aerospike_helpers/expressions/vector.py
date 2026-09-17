##########################################################################
# Copyright 2013-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################
"""
Vector expressions contain expressions for computing the distance between a
stored :class:`~aerospike_helpers.Vector` bin and a query vector.

    .. testsetup::

    {0}

Assume all inline code examples run this beforehand:

.. code-block:: Python

    {0}
"""

if __doc__:
    __doc__ = __doc__.format(
        """
        import aerospike
        import aerospike_helpers.expressions as exp
        from aerospike_helpers import Vector

        config = {"hosts": [("127.0.0.1", 3000)]}
        client = aerospike.client(config)
        key = ("test", "demo", 1)
        """
    )

from typing import Union

from aerospike_helpers.expressions.resources import _BaseExpr, _ExprOp
from aerospike_helpers.expressions.base import VectorBin

TypeBinName = Union[_BaseExpr, str]


class VectorDistanceMetric:
    """
    Distance metrics usable with :class:`VectorDistance`. Matches the C
    client's ``as_vector_distance_metric`` exactly.

    ``EUCLIDEAN_SQUARED`` sorts ascending (smallest distance is the closest
    match); ``DOT_PRODUCT``/``COSINE_SIMILARITY`` sort descending (largest
    value is the best match). Picking the wrong :meth:`Query.order_by
    <aerospike.Query.order_by>` direction for a given metric silently returns
    the *worst* matches instead of raising an error.
    """

    EUCLIDEAN_SQUARED = 0
    DOT_PRODUCT = 1
    COSINE_SIMILARITY = 2


_METRIC_TO_OP = {
    VectorDistanceMetric.EUCLIDEAN_SQUARED: _ExprOp.VECTOR_EUCLIDEAN_DIST,
    VectorDistanceMetric.DOT_PRODUCT: _ExprOp.VECTOR_DOT_PRODUCT,
    VectorDistanceMetric.COSINE_SIMILARITY: _ExprOp.VECTOR_COSINE_SIM,
}


class VectorDistance(_BaseExpr):
    """Create an expression that returns the distance between a stored vector
    bin and a query vector, as a float, using the given metric.
    """

    _rt = None  # float result; the C extension infers no explicit result type here.

    def __init__(self, metric: int, query: "aerospike_helpers.Vector", bin: "TypeBinName"):  # noqa: F821
        """Args:
            metric (int): One of :class:`VectorDistanceMetric`.
            query (aerospike_helpers.Vector): Complete serialized query vector
                (including its 8-byte header) - must match the stored
                vector's element type and dimension count, or the expression
                evaluates to the unknown-value.
            bin (TypeBinName): A :class:`~aerospike_helpers.expressions.base.VectorBin`
                expression, typically ``VectorBin(name)``.

        :return: (float value)

        Example:

        .. testcode::

            # Cosine similarity between a query vector and bin "embedding" is > 0.8.
            from aerospike_helpers import Vector
            from aerospike_helpers.expressions import vector as vector_exp
            from aerospike_helpers.expressions.base import VectorBin

            query_vector = Vector.of_float32([0.1, 0.2, 0.3, 0.4])
            dist = vector_exp.VectorDistance(
                vector_exp.VectorDistanceMetric.COSINE_SIMILARITY,
                query_vector,
                VectorBin("embedding"),
            )
            expr = exp.GT(dist, 0.8).compile()

        .. seealso::
            :meth:`~aerospike.Query.order_by` for a full Top-K / nearest-neighbor vector
            search example that projects :class:`VectorDistance` as a query result bin and
            orders by it.
        """
        try:
            self._op = _METRIC_TO_OP[metric]
        except KeyError:
            raise ValueError(f"Unknown VectorDistanceMetric: {metric}")

        self._children = (
            bytes(query),
            bin if isinstance(bin, _BaseExpr) else VectorBin(bin),
        )


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

import random

import aerospike
from aerospike_helpers import Vector
from aerospike_helpers.expressions import VectorBin
from aerospike_helpers.expressions.vector import VectorDistance, VectorDistanceMetric
from aerospike_helpers.operations import expression_operations as exp_ops
from aerospike_helpers.operations import operations as op_helpers
from .. import Example


class VectorTopKQuery(Example):
    """
    Vector similarity (nearest-neighbor / Top-K) search:

    1. Write records, each with an "id" integer bin and an "embedding" Vector bin.
    2. Project the distance between "embedding" and a query vector with an expression.
    3. Run a Top-K query (Query.order_by() and Query.top_k) to fetch the records whose
       "embedding" is closest to the query vector.
    """

    VECTOR_BIN_NAME = "embedding"
    DISTANCE_BIN_NAME = "dist"
    DIMENSIONS = 8
    RECORD_COUNT = 20
    TOP_K = 5

    def __init__(self):
        super().__init__(set_name="demo-vector")
        self.keys = [(self.namespace, self.set_name, i) for i in range(self.RECORD_COUNT)]

    def run(self):
        random.seed(0)
        for i, key in enumerate(self.keys):
            values = [random.uniform(-1, 1) for _ in range(self.DIMENSIONS)]
            embedding = Vector.of_float32(values)
            self.client.put(key, {"id": i, self.VECTOR_BIN_NAME: embedding})
        print(f"{len(self.keys)} records written.")

        query_values = [0.5] * self.DIMENSIONS
        query_vector = Vector.of_float32(query_values)

        # EUCLIDEAN_SQUARED sorts ascending: the smallest distance is the closest match.
        # DOT_PRODUCT and COSINE_SIMILARITY sort descending instead; see VectorDistanceMetric.
        vector_bin = VectorBin(self.VECTOR_BIN_NAME)
        distance = VectorDistance(VectorDistanceMetric.EUCLIDEAN_SQUARED, query_vector, vector_bin)
        distance_expr = distance.compile()

        query = self.client.query(self.namespace, self.set_name)
        query.add_ops([
            op_helpers.read(self.VECTOR_BIN_NAME),
            exp_ops.expression_read(self.DISTANCE_BIN_NAME, distance_expr),
        ])
        query.order_by(
            self.DISTANCE_BIN_NAME,
            aerospike.QueryOrderByType.DOUBLE,
            aerospike.QueryOrder.ASCENDING,
        )
        query.top_k = self.TOP_K

        try:
            records = query.results()
        except aerospike.exception.ParamError as e:
            print(f"Server does not support Top-K queries: {e.msg}")
            return

        print(f"Nearest {len(records)} records to {query_values}:")
        for _, _, bins in records:
            embedding = bins[self.VECTOR_BIN_NAME]
            embedding_values = list(embedding.value)
            print(f"  {self.DISTANCE_BIN_NAME}={bins[self.DISTANCE_BIN_NAME]:.4f}  "
                  f"{self.VECTOR_BIN_NAME}={embedding_values}")

    def cleanup(self):
        for key in self.keys:
            try:
                self.client.remove(key)
            except aerospike.exception.RecordNotFound:
                pass

        super().cleanup()

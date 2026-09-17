# -*- coding: utf-8 -*-
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
#
# This example demonstrates a vector similarity (nearest-neighbor / Top-K)
# search:
#
#   1. Write several records, each with an "id" integer bin and a
#      "embedding" Vector bin.
#   2. Build an expression that projects the distance between "embedding"
#      and a query vector, using aerospike_helpers.expressions.vector.
#   3. Run a Top-K query (Query.order_by() + Query.top_k) to fetch the k
#      records whose "embedding" is closest to the query vector.
#   4. Use Query.min()/Query.max() - convenience methods built on the same
#      order_by()/top_k mechanism - to find the lowest/highest "id" value
#      across the whole result set.
#
##########################################################################

from __future__ import print_function

import random
import sys

from optparse import OptionParser

import aerospike
from aerospike_helpers import Vector
from aerospike_helpers.expressions import VectorBin
from aerospike_helpers.expressions.vector import VectorDistance, VectorDistanceMetric
from aerospike_helpers.operations import expression_operations as exp_ops
from aerospike_helpers.operations import operations as op_helpers

##########################################################################
# Option Parsing
##########################################################################

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")

optparser.add_option(
    "-U", "--username", dest="username", type="string", metavar="<USERNAME>",
    help="Username to connect to database.")

optparser.add_option(
    "-P", "--password", dest="password", type="string", metavar="<PASSWORD>",
    help="Password to connect to database.")

optparser.add_option(
    "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
    help="Address of Aerospike server.")

optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
    help="Namespace to write demo records to.")

optparser.add_option(
    "-s", "--set", dest="set", type="string", default="demo-vector", metavar="<SET>",
    help="Set to write demo records to.")

optparser.add_option(
    "-b", "--bin", dest="bin", type="string", default="embedding", metavar="<BIN>",
    help="Name of the Vector bin.")

optparser.add_option(
    "-d", "--dimensions", dest="dimensions", type="int", default=8, metavar="<DIMENSIONS>",
    help="Number of dimensions in each vector.")

optparser.add_option(
    "-r", "--records", dest="num_records", type="int", default=20, metavar="<COUNT>",
    help="Number of demo records to write.")

optparser.add_option(
    "-k", "--top-k", dest="top_k", type="int", default=5, metavar="<K>",
    help="Number of nearest-neighbor records to return.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Client Configuration
##########################################################################

config = {
    'hosts': [(options.host, options.port)]
}

##########################################################################
# Application
##########################################################################

exitCode = 0

try:

    # ----------------------------------------------------------------------------
    # Connect to Cluster
    # ----------------------------------------------------------------------------

    client = aerospike.client(config).connect(
        options.username, options.password)

    # ----------------------------------------------------------------------------
    # Perform Operation
    # ----------------------------------------------------------------------------

    try:
        namespace = options.namespace if options.namespace and options.namespace != 'None' else None
        set = options.set if options.set and options.set != 'None' else None
        bin_name = options.bin
        dist_bin_name = "dist"

        # ------------------------------------------------------------------------
        # Write demo records, each with a random Vector bin.
        # ------------------------------------------------------------------------

        random.seed(0)
        keys = [(namespace, set, i) for i in range(options.num_records)]

        for i, key in enumerate(keys):
            values = [random.uniform(-1, 1) for _ in range(options.dimensions)]
            client.put(key, {"id": i, bin_name: Vector.of_float32(values)})

        print("---")
        print("OK, %d records written." % len(keys))

        # ------------------------------------------------------------------------
        # Build the query vector and the distance expression.
        # ------------------------------------------------------------------------

        query_values = [0.5] * options.dimensions
        query_vector = Vector.of_float32(query_values)

        # EUCLIDEAN_SQUARED sorts ascending: smallest distance is the closest
        # match. DOT_PRODUCT/COSINE_SIMILARITY sort descending instead - see
        # VectorDistanceMetric's docs.
        dist_expr = VectorDistance(
            VectorDistanceMetric.EUCLIDEAN_SQUARED, query_vector, VectorBin(bin_name)
        ).compile()

        # ------------------------------------------------------------------------
        # Run the Top-K query.
        # ------------------------------------------------------------------------

        query = client.query(namespace, set)
        query.add_ops([
            op_helpers.read(bin_name),
            exp_ops.expression_read(dist_bin_name, dist_expr),
        ])
        query.order_by(dist_bin_name, aerospike.QUERY_ORDER_BY_DOUBLE, aerospike.QUERY_ORDER_ASCENDING)
        query.top_k = options.top_k

        try:
            records = query.results()
        except aerospike.exception.ParamError as e:
            print("error: server does not support Top-K queries: {0}".format(e), file=sys.stderr)
            records = []
            exitCode = 2

        print("---")
        print("Nearest %d records to %s:" % (len(records), query_values))
        for _, _, bins in records:
            print("  dist=%.4f  %s=%s" % (bins[dist_bin_name], bin_name, list(bins[bin_name].value)))

        # ------------------------------------------------------------------------
        # Query.min()/Query.max(): convenience methods built on the same
        # order_by()/top_k mechanism used above, but reducing the whole result
        # set down to a single scalar value instead of returning records.
        # ------------------------------------------------------------------------

        try:
            min_id = client.query(namespace, set).min("id", aerospike.QUERY_ORDER_BY_INTEGER)
            max_id = client.query(namespace, set).max("id", aerospike.QUERY_ORDER_BY_INTEGER)

            print("---")
            print("Query.min()/Query.max() on 'id': min=%s max=%s" % (min_id, max_id))
        except aerospike.exception.ParamError as e:
            print("error: server does not support Top-K queries: {0}".format(e), file=sys.stderr)
            exitCode = 2

    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
        exitCode = 2

    finally:
        # ------------------------------------------------------------------------
        # Cleanup
        # ------------------------------------------------------------------------
        for key in keys:
            try:
                client.remove(key)
            except Exception:
                pass

    # ----------------------------------------------------------------------------
    # Close Connection to Cluster
    # ----------------------------------------------------------------------------

    client.close()

except Exception as eargs:
    print("error: {0}".format(eargs), file=sys.stderr)
    exitCode = 3

##########################################################################
# Exit
##########################################################################

sys.exit(exitCode)

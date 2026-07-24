
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


import aerospike
import json
import re
import sys
import os.path

from aerospike import predicates as p

##########################################################################
# Option Parsing
##########################################################################

usage = "usage: %prog [options] [where]"


def query_callback(option, opt, value, parser):
    setattr(parser.values, option.dest, value.split(','))

# optparser.add_option(
#     "--show-key", dest="show_key", action="store_true",
#     help="If set, displays the key/digest.")

# optparser.add_option(
#     "--show-meta", dest="show_meta", action="store_true",
#     help="If set, displays the metadata.")


from .. import ExampleWithIndex


config = {
    'lua': {
        'user_path': os.path.dirname(__file__)
    }
}


class QueryApply(ExampleWithIndex):
    def run(self):
        BIN = "bin"
        predicates = [
            p.equals(BIN, 1),
            # p.equals(BIN, "a"),
            # p.between(BIN, 1, 3)
        ]

        for predicate in predicates:
            # If predicate is provided, then perform a query
            BINS = [BIN]

            MODULE = "stream_example"
            FUNCTION = "count"
            ARGS = []
            query_id = self.client.query_apply(self.namespace, self.set_name, predicate, MODULE, FUNCTION, ARGS)

            while True:
                response = self.client.job_info(query_id, aerospike.JOB_QUERY)
                if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                    break

            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                print("Background query is successful")
            else:
                print("Query_apply failed")

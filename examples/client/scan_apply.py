
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
import sys

from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options] module function [args...]"


def scan_callback(option, opt, value, parser):
    setattr(parser.values, option.dest, value.split(','))


# optparser.add_option(
#     "-m", "--module", dest="module", type="string",
#     help="UDF Module.")

# optparser.add_option(
#     "-f", "--function", dest="function", type="string",
#     help="UDF Function.")

# optparser.add_option(
#     "-a", "--arg", dest="arguments", type="string", action="callback",
#     callback=scan_callback,  help="UDF Arguments.")

# optparser.add_option(
#     "-b", "--bins", dest="bins", type="string", action="append",
#     help="Bins to select from each record.")


exitCode = 0


def parse_arg(s):
    try:
        return json.loads(s)
    except ValueError:
        return s

from .. import Example


class ScanApply(Example):
    def run(self):
        # args.reverse()

        module = options.module
        function = options.function

        for i, param in enumerate(options.arguments):
            if param.isdigit():
                options.arguments[i] = int(param)

        policy = {}
        scan_id = client.scan_apply(
            namespace, set, module, function, options.arguments, policy)

        while True:
            response = client.job_info(scan_id, aerospike.JOB_SCAN)
            if response['status'] == aerospike.JOB_STATUS_COMPLETED:
                break

        if response['status'] == aerospike.JOB_STATUS_COMPLETED:
            print("Background scan is successful")
        else:
            print("Scan_apply failed")

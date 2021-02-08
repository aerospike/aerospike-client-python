# -*- coding: utf-8 -*-
##########################################################################
# Copyright 2013-2021 Aerospike, Inc.
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

from __future__ import print_function

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

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
    help="Address of Aerospike server.")

optparser.add_option(
    "-U", "--username", dest="username", type="string", metavar="<USERNAME>",
    help="Username to connect to database.")

optparser.add_option(
    "-P", "--password", dest="password", type="string", metavar="<PASSWORD>",
    help="Password to connect to database.")

optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")

optparser.add_option(
    "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-s", "--set", dest="set", type="string", default="demo", metavar="<SET>",
    help="Port of the Aerospike server.")

optparser.add_option(
    "-m", "--module", dest="module", type="string",
    help="UDF Module.")

optparser.add_option(
    "-f", "--function", dest="function", type="string",
    help="UDF Function.")

optparser.add_option(
    "-a", "--arg", dest="arguments", type="string", action="callback",
    callback=scan_callback,  help="UDF Arguments.")

optparser.add_option(
    "-b", "--bins", dest="bins", type="string", action="append",
    help="Bins to select from each record.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

if len(args) > 0:
    optparser.print_help()
    print()
    sys.exit(1)
###############################################################################
# Client Configuration
###############################################################################

config = {
    'hosts': [(options.host, options.port)]
}

###############################################################################
# Application
###############################################################################

exitCode = 0


def parse_arg(s):
    try:
        return json.loads(s)
    except ValueError:
        return s

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

        args.reverse()

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

    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
        rc = 1

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

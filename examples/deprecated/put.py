# -*- coding: utf-8 -*-
################################################################################
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
################################################################################

from __future__ import print_function

import aerospike
import sys

from optparse import OptionParser

################################################################
# Option Parsing
################################################################

usage = "usage: %prog [options] key"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
  "--help", dest="help", action="store_true",
  help="Displays this message.")

optparser.add_option(
  "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
  help="Address of Aerospike server.")

optparser.add_option(
  "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
  help="Port of the Aerospike server.")

optparser.add_option(
  "-n", "--namespace", dest="namespace", type="string", default="test", metavar="<NS>",
  help="Port of the Aerospike server.")

optparser.add_option(
  "-s", "--set", dest="set", type="string", default="demo", metavar="<SET>",
  help="Port of the Aerospike server.")

optparser.add_option(
  "--gen", dest="gen", type="int", default=10, metavar="<GEN>",
  help="Generation of the record being written.")

optparser.add_option(
 "--ttl", dest="ttl", type="int", default=1000, metavar="<TTL>",
  help="TTL of the record being written.")


(options, args) = optparser.parse_args()

if options.help:
  optparser.print_help()
  print()
  sys.exit(1)

if len(args) != 1:
  optparser.print_help()
  print()
  sys.exit(1)

################################################################
# Connect to Cluster
################################################################

config = {
  'hosts': [ (options.host, options.port) ]
}

client = aerospike.client(config).connect()

################################################################
# Perform Operation
################################################################

rc = 0
key = args.pop()

record = {
  'i': 123,
  's': 'abc',
  # 'b': bytearray(['d','e','f']),
  # 'l': [123, 'abc', bytearray(['d','e','f']), ['x', 'y', 'z'], {'x': 1, 'y': 2, 'z': 3}],
  # 'm': {'i': 123, 's': 'abc', 'b': bytearray(['d','e','f']), 'l': ['x', 'y', 'z'], 'd': {'x': 1, 'y': 2, 'z': 3}}
  'l': [123, 'abc', ['x', 'y', 'z'], {'x': 1, 'y': 2, 'z': 3}],
  'm': {'i': 123, 's': 'abc', 'l': ['x', 'y', 'z'], 'd': {'x': 1, 'y': 2, 'z': 3}}
}

try:
  meta = {'ttl': options.ttl, 'gen': options.gen}
  # meta = None
  # policy = { 'gen': 0 }
  policy = None
  client.key(options.namespace, options.set, key).put(record, meta)
  
  print(record)
  print("---")
  print("OK, 1 record written.")

except Exception as e:
  print("error: {0}".format(e), file=sys.stderr)
  rc = 1

################################################################
# Close Connection to Cluster
################################################################

client.close()

################################################################
# Exit
################################################################

sys.exit(rc)

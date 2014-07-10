# -*- coding: utf-8 -*-
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
  "--gen", dest="gen", type="int", default=None, metavar="<GEN>",
  help="Generation of the record being written.")

optparser.add_option(
 "--ttl", dest="ttl", type="int", default=None, metavar="<TTL>",
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
namespace = options.namespace if options.namespace and options.namespace != 'None' else None
set = options.set if options.set and options.set != 'None' else None
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
  keyt = (namespace, set, key)
  meta = {'ttl': options.ttl, 'gen': options.gen}
  policy = None
  client.put(keyt, record, meta, policy)
  
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

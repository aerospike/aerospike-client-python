# -*- coding: utf-8 -*-
from __future__ import print_function

import aerospike
import sys

from optparse import OptionParser

################################################################
# Option Parsing
################################################################

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
  "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
  help="Address of Aerospike server.")

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
  "-b", "--bins", dest="bins", type="string", action="append", 
  help="Bins to select from each record.")

(options, args) = optparser.parse_args()

if options.help:
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

try:
  s = client.scan(options.namespace, options.set)
  
  if options.bins and len(options.bins) > 0:
    s.select(*options.bins)

  records = []

  def callback((key, meta, record)):
    records.append(record)
    print(record)

  s.foreach(callback)

  print("---")
  if len(records) == 1:
    print("OK, 1 record found.")
  else:
    print("OK, %d records found." % len(records))

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

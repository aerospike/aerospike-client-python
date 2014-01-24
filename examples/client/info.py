# -*- coding: utf-8 -*-
from __future__ import print_function

import aerospike
import sys

from optparse import OptionParser

################################################################
# Option Parsing
################################################################

usage = "usage: %prog [options] [REQUEST]"

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
# Application
################################################################

rc = 0
request = "statistics"

if len(args) > 0:
  request = ' '.join(args)

try:
  for node,(err,res) in client.info(request).items():
    if res != None:
      res = res.strip()
      if len(res) > 0:
        entries = res.split(';')
        if len(entries) > 1:
          print("{0}:".format(node))
          for entry in entries:
            entry = entry.strip()
            if len(entry) > 0:
              count = 0
              for field in entry.split(','):
                (name,value) = field.split('=')
                if count > 0:
                  print("      {0}: {1}".format(name, value))
                else:
                  print("    - {0}: {1}".format(name, value))
                count += 1
        else:
          print("{0}: {1}".format(node, res))

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

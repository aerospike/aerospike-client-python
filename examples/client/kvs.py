# -*- coding: utf-8 -*-

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

(options, args) = optparser.parse_args()

if options.help:
  optparser.print_help()
  print ''
  sys.exit(1)

################################################################
# Application
################################################################

config = {
  'hosts': [ (options.host, options.port) ]
}

client = aerospike.client(config).connect()

print '########################################################################'
print 'PUT'
print '########################################################################'

for i in range(1000):
  # print 'a'
  # j = igloo
  rec = {
    'a': i,
    'b': 'xyz',
    'c': [i, 'x', ['y', 'z'], {'v': 20, 'w': 21}],
    'd': {'a': i, 'b': 'x', 'c': ['y', 'z'], 'd': {'v': 20, 'w': 21}}
  }
  print rec
  client.key('test','demo','key{0}'.format(i)).put(rec)

print '########################################################################'
print 'EXISTS'
print '########################################################################'

for i in range(1,1000):
  (key, metadata) = client.key('test','demo','key{0}'.format(i)).exists()
  print key, metadata


print '########################################################################'
print 'GET'
print '########################################################################'

for i in range(1,1000):
  (key, metadata, record) =  client.key('test','demo','key{0}'.format(i)).get()
  print key, metadata, record

# print '########################################################################'
# print 'APPLY'
# print '########################################################################'

# for i in range(1,1000):
#   val1 = client.key('test','demo','key{0}'.format(i)).apply('simple', 'add', ['a', 30000])
#   print val1

# print '########################################################################'
# print 'REMOVE'
# print '########################################################################'

# for i in range(1,1000):
#   client.key('test','demo','key{0}'.format(i)).remove()

# print '########################################################################'
# print 'GET'
# print '########################################################################'

# for i in range(1,1000):
#   rec1 = client.key('test','demo','key{0}'.format(i)).get()
#   print rec1

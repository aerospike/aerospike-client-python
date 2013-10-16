
import aerospike

config = {
  'hosts': [ 'localhost' ]
}

client = aerospike.connect(config)

s = client.scan("test","demo")

print '########################################################################'
print 'FOREACH CALLBACK'
print '########################################################################'

def each_record(rec):
  print rec

s.foreach(each_record)

print '########################################################################'
print 'FOR LOOP'
print '########################################################################'

for result in s.results():
  print result




import aerospike

config = {
  'hosts': [ 'localhost' ]
}

client = aerospike.connect(config)

q = client.query("test","demo").select('a','b').where('a', range=(1,10))

print '########################################################################'
print 'FOREACH CALLBACK'
print '########################################################################'

def each_record(rec):
  print rec

q.foreach(each_record)

print '########################################################################'
print 'FOR LOOP'
print '########################################################################'

for result in q.results():
  print result



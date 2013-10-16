
import sys
import aerospike

config = {
  'hosts': [ 'localhost' ]
}

client = aerospike.connect(config)
  
  for node,(err,res) in client.info(sys.argv[1]).items():
    if res != None:
      res = res.strip()
      if len(res) > 0:
        entries = res.split(';')
        if len(entries) > 1:
          print "{0}:".format(node)
          for entry in entries:
            entry = entry.strip()
            if len(entry) > 0:
              count = 0
              for field in entry.split(','):
                (name,value) = field.split('=')
                if count > 0:
                  print "      {0}: {1}".format(name, value)
                else:
                  print "    - {0}: {1}".format(name, value)
                count += 1
        else:
          print "{0}: {1}".format(node, res)

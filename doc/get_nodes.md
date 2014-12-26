
# aerospike.Client.get_nodes

aerospike.Client.get_nodes - retreives information about all the nodes in a particular cluster.

## Description

```
aerospike.Client.get_nodes()

```

**aerospike.Client.get_nodes()** retreives information about all the nodes in a particular cluster.
Returns a *list* containing details of all nodes in a cluster.


## Return Values
Returns *list*

## Examples

'''python

# -*- coding: utf-8 -*-
import aerospike
config = {
            'hosts': [('127.0.0.1', 3000)]
         }
client = aerospike.client(config).connet()
response = self.client.get_nodes()
print response

client.close()

'''
We expect to see:

'''python
[{'addr': '172.20.25.176', 'port': '3000'}]
'''

### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)

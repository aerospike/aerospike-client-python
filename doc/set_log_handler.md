
# aerospike.set_log_handler

aerospike.set_log_handler - sets the log handler for python client.

## Description

```
status = aerospike.set_log_handler ( callback )

```

**aerospike.set_log_handler()** will set the log handler for python client.
The callback specified by the **callback** is called whenever a log message is
encountered.

## Parameters

The function takes **callback** (required) as an argument.

## Return Values
The return value will be the status of setting the log handler.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
status = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
def handler(level, func, myfile, line):
	print "Inside handler"
status = aerospike.set_log_handler(handler)
print status

```

We expect to see:

```python
# Some sample data present in database
0
```



### See Also



- [Glossary](http://www.aerospike.com/docs/guide/glossary.html)

- [Aerospike Data Model](http://www.aerospike.com/docs/architecture/data-model.html)

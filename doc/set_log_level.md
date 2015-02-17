
# aerospike.set_log_level

aerospike.set_log_level - sets the log level for python client.

## Description

```
status = aerospike.set_log_level ( log_level )

```

**aerospike.set_log_level()** will set the log_level for python client.
All log messages above this level will be successfully logged.

## Parameters

The function takes **log_level** (required) as an argument. The **log_level**
can have following values:
**aerospike.LOG_LEVEL_DEBUG**
**aerospike.LOG_LEVEL_ERROR**
**aerospike.LOG_LEVEL_INFO**
**aerospike.LOG_LEVEL_WARN**
**aerospike.LOG_LEVEL_OFF**
**aerospike.LOG_LEVEL_TRACE**

## Return Values
The return value will be the status of setting the log level.

## Examples

```python

# -*- coding: utf-8 -*-
import aerospike
status = aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
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

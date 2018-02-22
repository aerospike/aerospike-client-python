# API Changes

## Version 3.1.0
### Backwards Incompatible API changes
* Updated the args passed to AerospikeError constructor internally to contain 5 arguments.
  The arguments previously were `error code`, `error message`, `error file`, `error line`.
  A fifth argument `in_doubt` has been added to the internal calls. so the arguments passed to the constructor are now : `error_code`, `error_message`, `error_file`, `error_line`, `in_doubt`

  This means that code such as the following will now raise a ValueError
 
```python
try:
    client.get(key)
except AerospikeError as e:
    code, msg, file, line = e.args
    print(code, msg, file, line)
```

  This can be fixed by unpacking the fifth value from the Error's `args` tuple

```python
try:
    client.get(key)
except AerospikeError as e:
    code, msg, file, line, in_doubt = e.args
    print(code, msg, file, line)
```


## Version 3.0.0


### Additional Features

* Updated to C client `4.3.1`
* Added a new list increment operation OP_LIST_INCREMENT . It can be used to increase an element of a list by a provided amount.
* Added the option to specify a specific node to run on a scan on, via a new optional nodename parameter.
* Added the option to specify that Query.results and Query.foreach should not return bins, via a new optional parameter options to both methods.
* Added aerospike.info_all() to allow sending of an info command to all nodes in the current cluster.
* Added linearize_read option to read policies. Requires Enterprise server >= 4.0.0

### Deprecations
* `client#info` has been deprecated. In order to send requests to the entire cluster, the new method `client#info_all` should be used. In order to send requests to hosts specified in a list, we recommend a  loop invoking multiple calls to aerospike.info_node. See below for an example implementation:

```python

def info_to_host_list(client, request, hosts, policy=None):
	output = {}
	for host in hosts:
	    try:
	        response = client.info_node(request, host, policy)
	        output[host] = response
	    except Exception as e:
	        #  Handle the error gracefully here
	        output[host] = e
	return output
```

* Setting of global policy defaults via top level entries in the 'policies' dictionary in the constructor config dict has been deprecated. See the constructor documentation for the new recommended method of specifying defaults.


### Backwards Incompatible API changes
#### LDT Removal
Removed LDT (`client#llist`) methods and support. Server version 3.14 is the last version of the Aerospike server to support the functionality.


#### Index Methods

* Methods which create indexes, ( `index_string_create`, `index_integer_create`, `index_list_create`, `index_map_keys_create`, `index_map_values_create` and, `index_geo2dsphere_create` ),  will now raise an `IndexFoundError` if the specified bin has already been indexed, or if an index with the same name already exists.

* Methods which drop indexes (`index_remove`), will now raise an `IndexNotFound` if the named index does not exist.

#### Shared memory layout change
Shared memory layout has changed, and accordingly the default SHM key has changed from `0xA6000000` to `0xA7000000` . If manually specifiying an
SHM key, it is crucial to ensure that a separate key is used in order to prevent this version's client from sharing memory with a previous version.

#### Changed and removed policy field names:
In all policies, (except for `info` and `admin`), `timeout` has been split into `total_timeout` and `socket_timeout`
`total_timeout` is an int representing total transaction timeout in milliseconds. The `total_timeout` is tracked on the client and sent to the server along with the transaction in the wire protocol. The client will most likely timeout first, but the server also has the capability to timeout the transaction. If `total_timeout` is not zero and `total_timeout` is reached before the transaction completes, the transaction will return error `TimeoutError`. If `total_timeout` is zero, there will be no total time limit. See the documentation for individual policies for the default values.

`socket_timeout` is an int representing socket idle timeout in milliseconds when processing a database command. If `socket_timeout` is not zero and the socket has been idle for at least `socket_timeout`, both max_retries and `total_timeout` are checked. If `max_retries` and `total_timeout` are not exceeded, the transaction is retried. If both `socket_timeout` and `total_timeout` are non-zero and `socket_timeout > total_timeout`, then `socket_timeout` will be set to `total_timeout`. If `socket_timeout` is zero, there will be no socket idle limit. See the documentation for individual policies for the default values.

`retry` has beeen removed.

`max_retries` has been added, it is an integer specifying the number of times a transaction should be retried before aborting. If `max_retries` is exceeded, `TimeoutError` will be raised.

WARNING: Database writes that are not idempotent (such as `client#increment`) should not be retried because the write operation may be performed multiple times if the client timed out previous transaction attempts. It’s important to use a distinct write policy for non-idempotent writes which sets `max_retries` = 0;

The default value for `max_retries` is 2.

#### Changes in policy defaults for `aerospike.client` constructor
In this version, individual config dictionaries (`read`, `write`, `apply`, `operate`, `scan`, `query`, `batch`, `remove`) for method types should be used inside of the top level `policies` dictionary for setting policies. In previous versions, individual policies were set at the top level `policies` dictionary rather than in per method type dictionaries. The type of policy which affects each method can be found in the documenation. See the main documentation for the keys and values available for these new configuration dictionaries. See below for an example of the change in constructor usage:

```python
#  Pre Python client 3.0.0

hosts = [('localhost', 3000)]

timeout = 2345
key_policy = aerospike.POLICY_KEY_SEND

# This changes defaults for all methods, requiring a config dictionary to be passed in to all methods which
# should use different values.
policies = {timeout': timeout, 'key': key_policy, 'retry': aerospike.POLICY_RETRY_ONCE}
config = {'hosts': hosts, 'policies': policies}

client = aerospike.client(config)
```

```python
#  Post Python client 3.0.0

hosts = [('localhost', 3000)]

write_total_timeout = 4321
read_total_timeout = 1234
write_key_policy = aerospike.POLICY_KEY_SEND

read_policy = {'total_timeout': read_total_timeout, 'max_retries': 1}
write_policy = {'total_timeout': write_total_timeout, 'key': write_key_policy, 'max_retries': 1}

# operate policies affect methods such as client#increment, so these should not be retried since they are not idempotent.
operate_policy = {'max_retries': 0}

# Change the defaults for read, write, and operate methods, all other methods will use builtin defaults.
policies = {'read': read_policy, 'write': write_policy, 'operate': operate_policy}

config = {'hosts': hosts, 'policies': policies}

client = aerospike.client(config)
```

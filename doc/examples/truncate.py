import time

client.put(("test", "demo", "key1"), {"bin": 4})

time.sleep(1)
# Take threshold time
current_time = time.time()
time.sleep(1)

client.put(("test", "demo", "key2"), {"bin": 5})

threshold_ns = int(current_time * 10**9)
# Remove all items in set `demo` created before threshold time
# Record using key1 should be removed
client.truncate('test', 'demo', threshold_ns)

# Remove all items in namespace
# client.truncate('test', None, 0)

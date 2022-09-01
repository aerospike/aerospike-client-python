# Adds record keys from a stream to a list
# But limits the number of keys to "lim"
def limit(lim: int, result: list):
    # Integers are immutable
    # so a list (mutable) is used for the counter
    c = [0]
    def key_add(record):
        key, metadata, bins = record
        if c[0] < lim:
            result.append(key)
            c[0] = c[0] + 1
        else:
            return False
    return key_add

from aerospike import predicates as p

keys = []
query.foreach(limit(2, keys))
print(len(keys)) # 2

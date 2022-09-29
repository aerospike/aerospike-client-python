from aerospike import predicates

query.select('score')
query.where(predicates.equals('score', 100))

records = query.results()
# Matches one record
print(records)
# [(('test', 'demo', None, bytearray(b'...')), {'ttl': 2592000, 'gen': 1}, {'score': 100})]

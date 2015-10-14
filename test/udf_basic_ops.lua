function create_record(rec, val)
    rec['bin'] = val
    aerospike:create(rec)
end

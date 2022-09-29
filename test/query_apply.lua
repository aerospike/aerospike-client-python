function mark_as_applied(rec, bin, unused)
    info("my transform: %s", tostring(record.digest(rec)))
    rec[bin] = 'aerospike'
    aerospike:update(rec)
end

function mark_as_applied_three_arg(rec, bin, unused, unused1)
    rec[bin] = 'aerospike'
    aerospike:update(rec)
end

function mark_as_applied_one_arg(rec, bin)
    rec[bin] = 'aerospike'
    aerospike:update(rec)
end

function mytransform(rec, bin, offset)
    info("my transform: %s", tostring(record.digest(rec)))
    rec['age'] = rec['age'] + offset
    aerospike:update(rec)
end
function mytransformextra(rec, bin, offset, offset1)
    rec['age'] = rec['age'] + offset
    aerospike:update(rec)
end
function mytransformless(rec, bin)
    rec['age'] = rec['age'] + offset
    aerospike:update(rec)
end


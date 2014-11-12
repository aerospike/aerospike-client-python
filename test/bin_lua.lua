function mytransform(rec, bin, offset)
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


-- Returns a bin's value unchanged, so a vector comes back to the client as a vector.
function read_bin(rec, bin)
  return rec[bin]
end

-- Writes a UDF argument to a bin.
function write_bin(rec, bin, value)
  rec[bin] = value
  if aerospike:exists(rec) then
    aerospike:update(rec)
  else
    aerospike:create(rec)
  end
end

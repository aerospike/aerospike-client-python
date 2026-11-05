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

-- Returns 3 bytes tagged as a vector (type 16): a version and an element
-- type, but no dimension count.
function truncated_vector(rec)
  local b = bytes(3)
  bytes.set_byte(b, 1, 1)
  bytes.set_byte(b, 2, 3)
  bytes.set_byte(b, 3, 0)
  bytes.set_type(b, 16)
  return b
end

-- Stream UDF that maps each record to the bytes from truncated_vector().
function truncated_vectors(stream)
  return stream : map(truncated_vector)
end

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

-- Returns the bytes type of a UDF argument, 16 for a vector.
function bytes_type(rec, value)
  return bytes.get_type(value)
end

local function embedding(rec)
  return rec["embedding"]
end

local function first(a, b)
  return a
end

local function append(values, value)
  list.append(values, value)
  return values
end

-- Runs on the client after the reduce, so it sees the vector as the client's Lua does.
local function tagged(value)
  return map{type = bytes.get_type(value), value = value}
end

-- Stream UDFs that pass each record's "embedding" vector through a map, a reduce and an aggregate stage.
function vector_map(stream)
  return stream : map(embedding)
end

function vector_reduce(stream)
  return stream : map(embedding) : reduce(first)
end

function vector_aggregate(stream)
  return stream : map(embedding) : aggregate(list(), append) : reduce(list.merge)
end

function vector_client_map(stream)
  return stream : map(embedding) : reduce(first) : map(tagged)
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

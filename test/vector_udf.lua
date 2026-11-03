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

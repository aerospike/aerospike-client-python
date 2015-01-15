function list_append(rec, bin, value)
  local l = rec[bin]
  list.append(l, value)
  rec[bin] = l
  aerospike:update(rec)
  return 0
end

function list_append_extra(rec, bin, value, value1)
  local l = rec[bin]
  list.append(l, value)
  rec[bin] = l
  aerospike:update(rec)
  return 0
end

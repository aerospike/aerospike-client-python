local function one(rec)
    return 1
end

local function add(a, b)
    return a + b
end

function count(stream)
    return stream : map(one) : reduce(add)
end

function count_extra(stream, extra_parameter)
    return stream : map(one) : reduce(add)
end

function count_less()
    return stream : map(one) : reduce(add);
end

local function having_ge_threshold(bin_having, ge_threshold)
    return function(rec)
        if rec[bin_having] < ge_threshold then
            return false
        end
        return true
    end
end

local function count_bins(group_by_bin)
  return function(group, rec)
    if rec[group_by_bin] then
      local bin_name = rec[group_by_bin]
      group[bin_name] = (group[bin_name] or 0) + 1
    end
    return group
  end
end

local function add_values(val1, val2)
  return val1 + val2
end

local function reduce_groups(a, b)
  return map.merge(a, b, add_values)
end

function group_count(stream, group_by_bin, bin_having, ge_threshold)
  if bin_having and ge_threshold then
    local myfilter = having_ge_threshold(bin_having, ge_threshold)
    return stream : filter(myfilter) : aggregate(map{}, count_bins(group_by_bin)) : reduce(reduce_groups)
  else
    return stream : aggregate(map{}, count_bins(group_by_bin)) : reduce(reduce_groups)
  end
end


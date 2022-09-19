-- Filter function
-- Filters records with a bin value >= a threshold
local function is_greater_than_or_equal(binname, threshold)
    return function(rec)
        if rec[binname] < threshold then
            return false
        end
        return true
    end
end

-- Creates an aggregate function that counts the number of times a specific bin value is found
local function count(bin_name)
    return function(counts_map, rec)
        -- Does record have that specific bin?
        if rec[bin_name] then
            -- Account for that bin value
            local bin_value = rec[bin_name]
            counts_map[bin_value] = (counts_map[bin_value] or 0) + 1
        end
        -- No changes to bin value counts
        return counts_map
    end
end

-- Helper function for reduce
local function add_values(val1, val2)
    return val1 + val2
end

-- Combines count maps into one
-- Need this function when the database runs multiple aggregations in parallel
local function reduce_groups(a, b)
    return map.merge(a, b, add_values)
end

-- First filter records with a bin binname that has value >= threshold (if those arguments are passed in)
-- Then count the number of times a value in bin "binname_to_group" is found
function group_count(stream, binname_to_group, binname, threshold)
    if binname and threshold then
        local filter = is_greater_than_or_equal(binname, threshold)
        return stream : filter(filter) : aggregate(map{}, count(binname_to_group)) : reduce(reduce_groups)
    else
        -- Don't filter records in this case
        return stream : aggregate(map{}, count(binname_to_group)) : reduce(reduce_groups)
    end
end

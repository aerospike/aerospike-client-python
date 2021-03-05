--[[UDF which performs arithmetic operation on bin containing
    integer value.
--]]
function bin_udf_operation_integer(record, bin_name, x, y)
    record[bin_name] = (record[bin_name] + x) + y
    if aerospike:exists(record) then
        aerospike:update(record)
    else
        aerospike:create(record)
    end
    return record[bin_name]
end

--[[UDF which performs concatenation operation on bin containing
    string.
--]]
function bin_udf_operation_string(record, bin_name, str)
    if (type(record[bin_name]) == "string" or type(record[bin_name]) == "number") and
       (type(str) == "string" or type(str) == "number") then
       record[bin_name] = record[bin_name] .. str
    end
    if aerospike:exists(record) then
        aerospike:update(record)
    else
        aerospike:create(record)
    end
    return record[bin_name]
end

function bin_udf_operation_bool(record , bin_name)
    return record[bin_name]
end

--[[UDF which modifies element of list.--]]
function list_iterate(record, bin, index_of_ele)
    local get_list = record[bin]
    list.append(get_list, 58)
    get_list[index_of_ele] = 222;
    record[bin] = get_list;
    if aerospike:exists(record) then
        aerospike:update(record)
    else
        aerospike:create(record)
    end
end

--[[UDF which modifies list and returns a list.--]]
function list_iterate_returns_list(record, bin, index_of_ele)
    local get_list = record[bin]
    list.append(get_list, 58)
    get_list[index_of_ele] = 222;
    record[bin] = get_list;
    if aerospike:exists(record) then
        aerospike:update(record)
    else
        aerospike:create(record)
    end
    return record[bin]
end

--[[UDF which sets value of each key in the map by
    set_value which is given by user.--]]
function map_iterate(record, bin, set_value)
    local put_map = record[bin]
    for key,value in map.pairs(put_map) do
        put_map[key] = set_value;
    end
    record[bin] = put_map;
    if aerospike:exists(record) then
        aerospike:update(record)
    else
        aerospike:create(record)
    end
end

--[[UDF which sets value of each key in the map by
    set_value which is given by user, And returns map--]]
function map_iterate_returns_map(record, bin, set_value)
    local put_map = record[bin]
    for key,value in map.pairs(put_map) do
        put_map[key] = set_value;
    end
    record[bin] = put_map;
    if aerospike:exists(record) then
        aerospike:update(record)
    else
        aerospike:create(record)
    end
    return record[bin]
end

--[[UDF which returns a whole record--]]
function udf_returns_record(rec)
    local mapped = map()
    for i, bin_name in ipairs(record.bin_names(rec)) do
        mapped[bin_name] = rec[bin_name];
    end
    return mapped
end

--[[UDF which accepts nothing and returns nothing--]]
function udf_without_arg_and_return(record)
end

--[[UDF which will put bytes array in DB.--]]
function udf_put_bytes(record, bin)
    local put_bytes = bytes(18)
    put_bytes[1] = 10
    put_bytes[2] = 85
    record[bin] = put_bytes
    if aerospike:exists(record) then
        aerospike:update(record)
    else
        aerospike:create(record)
    end
end

--[[UDF which will check for record's existence and return bool.--]]
function bool_check(rec)
    return aerospike:exists(rec)
end
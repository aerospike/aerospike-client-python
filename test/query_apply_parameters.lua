local function my_map(rec)
    return rec['age']
end

local function my_filter(params)
	return function(rec)
		return (rec[params[1]] > params[2])
	end
end

function query_params(stream, params)
    local myfilter = my_filter(params)
    return stream : filter(myfilter) : map(my_map);
end

function ones(s)
    return s : map(function() return 1 end) : reduce (function() return 1 end)
end

function count(s)
    return s : map(function() return 1 end) : reduce (function(a,b) return a+b end)
end

function doit(r, a, b)
    local l = list{1,2,3,4,5,6}
    for i in l do
    end
    return list{"foo ", tostring(a), tostring(b)}
end

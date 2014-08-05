function ones(s)
    return s : map(function() return 1 end) : reduce (function() return 1 end)
end
function count(s)
    return s : map(function() return 1 end) : reduce (function(a,b) return a+b end)
end
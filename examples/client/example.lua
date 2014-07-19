function ones(s)
    return s : map(function() return 1 end) : reduce (function() return 1 end)
end
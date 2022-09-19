-- Deduct tax and fees from bin
function tax(record, binName, taxRate, fees)
    if aerospike:exists(record) then
        record[binName] = record[binName] * (1 - taxRate) - fees
        aerospike:update(record)
    else
        record[binName] = 0
        aerospike:create(record)
    end
    return record[binName]
end

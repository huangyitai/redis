package redis

// BatchTestBit 判断指定的位置列表的比特位,如果均为1返回1,否则返回0
const BatchTestBit = `
local key = KEYS[1]
for i = 1, #(ARGV) do
	local val = redis.call("GETBIT", key, ARGV[i])
	if val == 0 then
		return 0
	end
end
return 1
`

// BatchSetBit 批量设置指定的位置列表为指定值
const BatchSetBit = `
local key = KEYS[1]
local val = ARGV[1]
for i = 2, #(ARGV) do
	redis.call("SETBIT", key, ARGV[i], val)
end
return 0
`

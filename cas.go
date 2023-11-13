package redis

//GetInt32BigEndian GetInt32BigEndian
func GetInt32BigEndian(input []byte) (res int32) {
	if len(input) != 4 {
		return
	}
	for i := 0; i < len(input); i++ {
		res <<= 8
		res |= int32(input[i])
	}
	return
}

//PutInt32BigEndian PutInt32BigEndian
func PutInt32BigEndian(input int32) (res []byte) {
	res = make([]byte, 4)
	move := 32
	for i := 0; i < 4; i++ {
		move -= 8
		res[i] = byte((input >> move) & 0xff)
	}
	return res
}

//Pack Pack
func Pack(data []byte, cas int32) (val []byte) {
	val = make([]byte, len(data)+4)
	copy(val[0:], PutInt32BigEndian(cas))
	copy(val[4:], data)
	return
}

//Unpack Unpack
func Unpack(reply []byte) (val []byte, cas int32, err error) {
	if len(reply) < 4 {
		err = ErrorValueNotValid
		return
	}

	cas = GetInt32BigEndian(reply[:4])
	val = reply[4:]
	return
}

// SetScriptKeyValue set
// 这里需要考虑cas溢出问题
// 如果Get到的是2147483647, Set的cas为0
// Get到的cas是其他, Set为cas+1
const SetScriptKeyValue = `
local key = KEYS[1]
local data = ARGV[1]
local casIn = tonumber(ARGV[2])
if casIn < 0 then
	redis.call("set", key, data)
	return 0
end
local value = redis.call("get", key)
local cas = 0
if value ~= false then
	if string.len(value) < 4 then
		return 10000
	end
	cas = struct.unpack(">i4", value)
end
if cas == casIn then
	cas = ((cas + 1) % 2147483648)
	data = struct.pack(">I4", cas) .. string.sub(data, 5)
	redis.call("set", key, data)
    return 0
else
    return 10001
end
`

// SetScriptKeyValueV2 带额外参数的Set
const SetScriptKeyValueV2 = `
local key = KEYS[1]
local data = ARGV[1]
local casIn = tonumber(ARGV[2])
local extparm = {}
for i=3,#(ARGV) do
    extparm[i-2] = ARGV[i]
end
if casIn < 0 then
	redis.call("set", key, data, unpack(extparm))
	return 0
end
local value = redis.call("getrange", key, 0, 3)
local cas = 0
local vallen = string.len(value)
if vallen ~= 0 then
	if vallen ~= 4 then
		return 10000
	end
	cas = struct.unpack(">i4", value)
end
if cas == casIn then
	cas = ((cas + 1) % 2147483648)
	data = struct.pack(">I4", cas) .. string.sub(data, 5)
	redis.call("set", key, data, unpack(extparm))
    return 0
else
    return 10001
end
`

// DelScriptCas del带cas实现
const DelScriptCas = `
local key = KEYS[1]
local casIn = tonumber(ARGV[1])
if casIn < 0 then
	redis.call("del", key)
	return 0
end
local value = redis.call("getrange", key, 0, 3)
local cas = 0
local vallen = string.len(value)
if vallen ~= 0 then
	if vallen ~= 4 then
		return 10000
	end
	cas = struct.unpack(">i4", value)
end
if cas == casIn then
	redis.call("del", key)
    return 0
else
    return 10001
end
`

//HSetScript hset
// 这里需要考虑cas溢出问题
// 如果Get到的是2147483647, Set的cas为0
// Get到的cas是其他, Set为cas+1
const HSetScript = `
local key = KEYS[1]
local field = ARGV[1]
local nv = ARGV[2]
local casIn = tonumber(ARGV[3])
if casIn < 0 then
	nv = struct.pack(">I4", 0) .. nv
	redis.call("hset", key, field, nv)
	return 0
end
local ov = redis.call("hget", key, field)
local cas = 0
if ov ~= false then
	if string.len(ov) < 4 then
		return 10000
	end
	cas = struct.unpack(">I4", ov)
end
if cas == casIn then
	cas = ((cas + 1) % 2147483648)
	nv = struct.pack(">I4", cas) .. nv
	redis.call("hset", key, field, nv)
    return 0
else
    return 10001
end
`

//HDelScript hdel
// 这里需要考虑cas溢出问题
// 如果Get到的是2147483647, Set的cas为0
// Get到的cas是其他, Set为cas+1
const HDelScript = `
local key = KEYS[1]
local field = ARGV[1]
local casIn = tonumber(ARGV[2])
if casIn < 0 then
	redis.call("hdel", key, field)
	return 0
end
local ov = redis.call("hget", key, field)
local cas = 0
if ov ~= false then
	if string.len(ov) < 4 then
		return 10000
	end
	cas = struct.unpack(">I4", ov)
end
if cas == casIn then
	cas = ((cas + 1) % 2147483648)
	redis.call("hdel", key, field)
    return 0
else
    return 10001
end
`

// HSetAndGetScript 设置并获取
const HSetAndGetScript = `
local key = KEYS[1]
local field = ARGV[1]
local nv = ARGV[2]
local casIn = tonumber(ARGV[3])
local retVal = {}
if casIn < 0 then
	nv = struct.pack(">I4", 0) .. nv
	redis.call("hset", key, field, nv)
	retVal[1] = 0
	retVal[2] = redis.call("hget", key, field)
	return retVal
end
local ov = redis.call("hget", key, field)
local cas = 0
if ov ~= false then
	if string.len(ov) < 4 then
		retVal[1] = 10000
		return retVal
	end
	cas = struct.unpack(">I4", ov)
end
if cas == casIn then
	cas = ((cas + 1) % 2147483648)
	nv = struct.pack(">I4", cas) .. nv
	redis.call("hset", key, field, nv)
    retVal[1] = 0
    retVal[2] = redis.call("hget", key, field)
    return retVal
else
    retVal[1] = 10001
    return retVal
end
`

// HMSetWithKeyCas key维度cas的HMset
var HMSetWithKeyCas = `
local key = KEYS[1]
local casIn = tonumber(ARGV[1])
local ov = redis.call("HGET", key, "_cas")
local cas = 0
if ov ~= false then
	cas = tonumber(ov)
end
if cas == casIn then
	cas = ((cas + 1) % 2147483648)
	return redis.call("HMSET", key, "_cas", cas, select(2, unpack(ARGV)))
else
    return tostring(10001)
end
`

// HMGetWithKeyCas key维度cas的HMGet
var HMGetWithKeyCas = `
return redis.call("HMGET", KEYS[1], "_cas", unpack(ARGV))
`

// HDelWithKeyCas key维度cas的HDel
var HDelWithKeyCas = `
local key = KEYS[1]
local casIn = tonumber(ARGV[1])
local ov = redis.call("HGET", key, "_cas")
local cas = 0
if ov ~= false then
	cas = tonumber(ov)
end
if cas == casIn then
	cas = ((cas + 1) % 2147483648)
	redis.call("HMSET", key, "_cas", cas)
	return redis.call("HDEL", key, select(2, unpack(ARGV)))
else
    return -10001
end
`

---
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by lhq.
--- DateTime: 2021/7/18 17:38
---

local v = redis.call('get', KEYS[1])
if v == false then
    return 1
if v ~= KEYS[2] then
    return 0
end
return redis.call('del', KEYS[1])
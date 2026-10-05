-- ==============================================================
-- 预算原子扣减 · M3
-- ==============================================================
-- 解决 M1 三步操作的并发超扣问题：
--   1. 读余额
--   2. 校验余额是否够
--   3. 扣减
-- 三步之间有窗口，两个并发请求可能都读到同一份余额、各自通过校验、各自扣减，
-- 结果余额变成负数，超扣发生。
--
-- 本脚本把三步收进一次原子执行：Redis 执行 Lua 期间不会处理其他命令，
-- 因此不存在「读到旧值再写回」的中间态。
--
-- KEYS[1]  预算 key，格式 budget:{adId}:{yyyyMMdd}
-- ARGV[1]  本次扣减金额（分）
-- ARGV[2]  初始额度（分），key 不存在时用它初始化
--
-- 返回：
--   扣减成功后返回新的余额（分）
--   余额不足返回 -1
--   key 不存在且初始化失败返回 -2
--
-- 【为什么脚本必须极短】
-- Lua 执行期间会阻塞 Redis 单线程，脚本里任何耗时操作都会拖慢所有请求。
-- 本脚本只做「读、比较、减、写」四步，微秒级，没有循环、没有外部调用。
-- ==============================================================

local key = KEYS[1]
local cost = tonumber(ARGV[1])
local initAmount = tonumber(ARGV[2])

-- 读当前余额
local balance = redis.call('GET', key)

if balance == false then
    -- key 不存在：用初始额度初始化
    -- SETNX 保证不覆盖其他请求刚写入的值
    redis.call('SETNX', key, tostring(initAmount))
    balance = redis.call('GET', key)
    if balance == false then
        return -2
    end
end

balance = tonumber(balance)

-- 校验：余额不足则不扣
if balance < cost then
    return -1
end

-- 扣减
local remain = redis.call('DECRBY', key, cost)
return remain

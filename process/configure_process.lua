local redis = require 'redis_config'
local data = require 'data_rdf_wd'

local M = {
        _COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
}



local client6379 = redis.getRedis(6379)
local client6380 = redis.getRedis(6380)

local args = {...}

-------------------------configuration-----------------------------------------------
local sequence = args[1]
local database = args[2]

local length = client6379:hget("config_search:"..sequence,"length")  or 4
local links = client6379:hget("config_search:"..sequence,"links")  or 30000
local distance = client6379:hget("config_search:"..sequence,"distance")  or 10
local low_patterns = client6379:hget("config_search:"..sequence,"coherent")  or 5

data.MAX_LINKS = tonumber(links)
data.MAX_DISTANCE = tonumber(distance)
data.DISTANCE = tonumber(length)

local lig = client6379:hgetall("hlist_ignore:"..sequence)

if not lig then
        lig = nil
end

data.property_ignore =  lig
---------------------------------------------------------------------------------------
local start_node = client6379:get("START_NODE:"..sequence)
local end_node = client6379:get("END_NODE:"..sequence)

data.start_node = start_node
data.end_node = end_node

--------------------------------------------------------------------------------------
data.init(sequence,0,"extract",database, start_node, "L", client6379, client6380,start_node,end_node,true)
data.individualEntity()
data.init(sequence,0,"extract",database, end_node  , "R", client6379, client6380,start_node,end_node,true)
data.individualEntity()
---------------------------------------------------------------------------------------
local code_start = client6380:hget(start_node, "code")
local code_end = client6380:hget(end_node, "code")

client6379:lpush("l_object_immediate_class:"..sequence..":0",code_start.."@"..code_start)
client6379:lpush("l_object_immediate_class:"..sequence..":0",code_end.."@"..code_end)
---------------------------------------------------------------------------------------

--wikidata ----------------------------------------------------------------------------
if not code_start then
	code_start  = start_node
end
if not code_end then
        code_end  = end_node
end
--------------------------------------------------

client6379:hsetnx("hobjects:"..sequence,code_start,"")
client6379:lpush("list_objects:"..sequence,code_start)
client6379:hsetnx("hobjects:"..sequence,code_end,"")
client6379:lpush("list_objects:"..sequence,code_end)

local number_processes = 10

for i = 1, number_processes  do
  print(i.." ".. sequence.." " ..database)
end

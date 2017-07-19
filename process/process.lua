#!/usr/bin/env lua5.2

local redis = require 'redis_config'
local socket = require "socket"
local url = require"socket.url"
local io = require"io"
local sparql = require"sparql"
local intersection = require 'intersection'
local data = require 'data_rdf_wd'

local run_start = true

---parameters------------------------------------------
local par = ...
local parameters ={}
local ip=1
for word in string.gmatch(par, "%S+") do
    parameters [ip] = word
    ip =ip + 1
end

local ID_PROCESS = parameters[1]
local sequence = parameters[2]
local database = parameters[3]
local nivel = 0

local client6379 = redis.getRedis(6379)
local client6380 = redis.getRedis(6380)

if not sequence then
        return
end
local start_node = client6379:get("START_NODE:"..sequence)
local end_node = client6379:get("END_NODE:"..sequence)

data.start_node = start_node
data.end_node = end_node

local length = client6379:hget("config_search:"..sequence,"length")  or 4
local links = client6379:hget("config_search:"..sequence,"links")  or 30000
local distance = client6379:hget("config_search:"..sequence,"distance")  or 10

length = tonumber(length)

data.MAX_LINKS = tonumber(links)
data.MAX_DISTANCE = tonumber(distance)
data.DISTANCE = length

distance = length/2 

local lig = client6379:hgetall("hlist_ignore:"..sequence)
if not lig  then
        lig = nil
end
data.property_ignore =  lig



------------------------------------------------------
local callbacks ={
                get_code={ dbpedia =function(url)
                                        return data.getCodeUrl(url)
                                 end
                }
}
------------------------------------------------------

if callbacks.get_code[database] then
        start_node = callbacks.get_code[database](start_node)
        end_node = callbacks.get_code[database](end_node)
	data.start_node = start_node
	data.end_node = end_node
end

local stop_process = false
local function process_intersection(size_jobs)

	local value = client6379:get("PROCESS:"..nivel..":"..sequence)
	if value then
                value = tonumber(value)
	        if  (client6379:exists("PROCESS:"..nivel..":"..sequence)) and (value >= 0)then
        	        local index = client6379:decr("PROCESS:"..nivel..":"..sequence)
	                index = tonumber(index)
        	        client6379:hset("list_inter_process:"..nivel..":"..sequence, index,"")
	                if index >= 0 then
        	                if not intersection.getIntersection(index,size_jobs) then
				end
	                else
			     client6379:hdel("list_inter_process:"..nivel..":"..sequence, index)
			     return false
	                end
	                client6379:hdel("list_inter_process:"..nivel..":"..sequence, index)
		else
			return false
	        end
	end
	return true
end

local id_range = tonumber(ID_PROCESS)
id_range =  id_range -1

local interval = 10
local stop = 0

local function process_expand(name_list)

        local start = (id_range * interval) + stop
        local end_range = start + interval
        local list_entity=client:lrange(name_list,start,end_range)
        if list_entity and #list_entity > 0 then
            for k, entity in pairs(list_entity) do
                    client6379:hset("list_expand:"..sequence,start,"START")
		    if entity then
        	        data.generalProcess(entity)
		     end
	    end	
	else
		return false 
	end

        stop = interval*interval + stop
        client6379:hdel("list_expand:"..sequence,start)
        return true
	
end

local function wait_process(f, name)
        while true do
                local len = client:hlen(name..parameters[2])
                len = tonumber(len)
                if len>0 then
                 -- execute process
                        if f then
                                 f()
                        end
                else
                        break
                end
        end
end

local function start_process(f,init)
        if init then
                init()
        end
        while true do
                if not f() then
                        break
                end
        end
end



local function step_itersection()
        client6379:hset("list_inter_process:"..nivel..":"..sequence, "start:"..tostring(ID_PROCESS) ,"start") 
								
        local size_jobs = client6379:hlen("h_teste_intersection:"..sequence..":"..nivel)

        if ID_PROCESS =="1" then
                        client6379:set("PROCESS:"..nivel..":"..sequence,size_jobs)
        end
        intersection.init(ID_PROCESS, nivel,sequence,start_node,end_node,distance) 
        while true do
                        if client6379:hlen("list_inter_process:"..nivel..":"..sequence) > 0 then
                                if process_intersection(size_jobs) then
				else
					client6379:hdel("list_inter_process:"..nivel..":"..sequence,"start:"..tostring(ID_PROCESS))
				end
                        else    break   end
        end
end

local function step_extend(name_list,action)
	                final1 = 0

                        data.init(sequence,nivel,action,database, nil, nil, client6379, client6380,start_node,end_node,run_start)
                        while true do  ---wait
                                if process_expand(name_list) then
                                else    break   end
                        end
end
-----------------------------------------------------------------------------------------------------------------


local interval_class = 10
local stop_class= 0

local function step_immediate_class()
	local start = (id_range * interval_class) + stop_class
	local end_range = start + interval_class
        local list_entity=client:lrange("l_object_immediate_class:"..parameters[2]..":"..nivel,start,end_range)
	if list_entity and #list_entity > 0 then
 		client:hset("list_process_immediate_class:"..parameters[2],start,end_range)
		for k, entity in pairs(list_entity) do
	                data.init(sequence,nivel,"immediate_class",database,nil,nil, client6379, client6380, start_node,end_node,run_start)
        	        data.get_immediate_class(entity)
		end
	else	
                return false
        end
	stop_class = interval_class*interval_class + stop_class
        client:hdel("list_process_immediate_class:"..parameters[2],start)
        return true

end

local function get_paths()
client6379:hset("principal_process:"..sequence,ID_PROCESS,"START")
while true do
       if tonumber(nivel) < distance then
	        final = 0
                pcall( start_process, step_immediate_class)
                wait_process(nil, "list_process_immediate_class:")
	
                nivel =  nivel + 1
               	
		local previous = nivel -1
	        previous =  tostring(previous)
                local name_list = "l_wobject_extend:"..sequence..":"..previous
	        step_extend(name_list,"extend")
        	wait_process(nil, "list_expand:")
		run_start = false
		step_itersection()
		if stop_process then
			break
		end
		if tonumber(nivel) < distance  then

			name_list = "l_wobject_extract:"..sequence..":"..previous
        	        step_extend(name_list,"extract")
			wait_process(nil, "list_expand:")
		end
        else
		nivel = nivel + 1

                local name_list = "l_new_extend:"..sequence
                step_extend(name_list,"extract")
                wait_process(nil, "list_expand:")

                final = 0
                pcall( start_process,step_immediate_class)
                wait_process(nil, "list_process_immediate_class:")

                name_list = "l_wobject_extend:"..sequence..":"..(nivel-1)
                step_extend(name_list,"extend")
                wait_process(nil, "list_expand:")
	
                step_itersection() 
		break
        end
end
client6379:hdel("principal_process:"..sequence,ID_PROCESS)
end



local function getFrequencyClass(str_classes)
        local par = 1
	local class_key  =""
	local class_value = 0	
        for class in string.gmatch(str_classes, "([^,]+)") do
                if not(par % 2 == 0) then
			class_key =  class
		else
			
			class_value = tonumber(class)
			if class_key == "Thing" then
				class_value = 0
			end
			client:zincrby("zentity_classes:"..parameters[2],class_value,class_key)
			client:hincrby("hentity_classes:"..parameters[2],class_key,1)
                end
		par =  par + 1 
	end
end

local function specify_class_process()
        local entity = client:lpop("_objects:"..parameters[2])
        if entity then
                client:hset("specify_process:"..parameters[2],entity,"START")
		local url = client6380:hget("decode_url",entity)
                local str_classes = client6380:hget(url,"immediate_class")
                if str_classes then
			getFrequencyClass(str_classes)
		end	
        else
                return false
        end
        client:hdel("specify_process:"..parameters[2],entity)
        return true
end

local function query_components()
        local kquery=client:lpop("l_experiment_triple_object:"..parameters[2])
        if kquery then
                client:hset("query_components:"..parameters[2],kquery,"START")
		local squery = client6380:hget("experiment_triple_object", kquery)
		squery =  "SELECT count(*) WHERE { "..squery.." }"
		local result = sparql.query ({query=squery})
		local temp_key = kquery:gsub("query","value")

		if result and result[1] then
			local value = result[1][1]
			value = tonumber(value)
			client6380:hset("experiment_triple_object", temp_key, value)
		else
			client6380:hset("experiment_triple_object", temp_key, 0)
		end

        else
                return false
        end
        client:hdel("query_components:"..parameters[2],kquery)
        return true
end


------------------------------------------------------------------
--wait process 1 and execute process 2
get_paths()

wait_process (nil,"principal_process")

wait_process (specify_class_process,"principal_process")

--start process 2
start_process(specify_class_process)
--wait process 2 and execute processo3
wait_process (nil,"specify_process:")
------------------------------------------------------------------

--start process 3
start_process(query_components)
wait_process (nil,"query_components:")
------------------------------------------------------------------


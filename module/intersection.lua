local redis = require 'redis_config'

local M = {
        _COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
}

local client6379 = redis.getRedis(6379)
local client6380 = redis.getRedis(6380)


local nivel = nil
local sequence = nil
local start = nil
local send = nil
local distance = nil
local id_process = nil
local number_path = 0 
function M.init(ID, nv,seq,st,se,dis)
	nivel = nv
	sequence = seq
	start = st
	send = se
	distance = dis
	id_process = ID
end

local function recurrent(string)
	local index = 1
        local rev={}
	local list_properties ={}
	local list_path ={}
        for word in string.gmatch(string, "%S+") do
		if not(index % 2 == 0) then 
	                if rev[word] then
        	                return false
	                else
        	                rev[word] =""
	                end
		else
			table.insert(list_properties,word) 
		end
		index = index + 1
		table.insert(list_path,word)
        end
        return true, list_path,list_properties
end

function setObjectIntersection(object)
	local a = client6379:hsetnx("hobjects:"..sequence,object,"")
        if (nivel <= (distance)) and  a then
                   client6379:lpush("list_objects:"..sequence,object) --
                   if (nivel == distance) then
                            if client6379:hsetnx("h_new_intersection:"..sequence,object,"") then
                                    client6379:rpush("l_new_extend:"..sequence,object)
                            end
                   end
       end

end

local function getObjectTriple(s, p, o)


        local key_st = s..":"..p
	local key_ot = p..":"..o


        local tp = p:gsub("@","")
 	      tp = client6380:hget("decode_url", tp)
	local ts = client6380:hget("decode_url", s)
	local to = client6380:hget("decode_url", o)
	

        local triple_s = "<"..ts.."> ".."<"..tp.."> ?x"
	local triple_o = "?x <"..tp.."> ".."<"..to..">"

	if p:find("@") then
		p = p:gsub("@","")
		key_st = p..":"..s
		key_ot = o..":"..p
        	triple_s = "?x <" ..tp.."> ".."<"..ts..">"
		triple_o = "<" ..to.."> ".."<"..tp.."> ?x"

	end

	if  not client6380:hexists("experimento_triple_object", "value:subject:"..s) then
	        client6380:hset("experimento_triple_object", "query:subject:"..s, "<"..ts.."> ?p ?o")
		if client6379:hsetnx("hexperimento_triple_object:"..sequence, "value:subject:"..s,"") then
	                client6379:lpush("lexperimento_triple_object:"..sequence,"query:subject:"..s)
		end
	end
	if  not client6380:hexists("experimento_triple_object","value:object:"..s) then
                client6380:hset("experimento_triple_object", "query:object:"..s, "?s ?p <"..ts..">")	
		if client6379:hsetnx("hexperimento_triple_object:"..sequence,"query:object:"..s,"") then
			client6379:lpush("lexperimento_triple_object:"..sequence,"query:object:"..s)
		end
	end

        if  not client6380:hexists("experimento_triple_object", "value:subject:"..o) then
                client6380:hset("experimento_triple_object", "query:subject:"..o, "<"..to.."> ?p ?o")
                if client6379:hsetnx("hexperimento_triple_object:"..sequence, "value:subject:"..o,"") then
                        client6379:lpush("lexperimento_triple_object:"..sequence,"query:subject:"..o)
                end
        end
        if  not client6380:hexists("experimento_triple_object","value:object:"..o) then
                client6380:hset("experimento_triple_object", "query:object:"..o, "?s ?p <"..to..">")
                if client6379:hsetnx("hexperimento_triple_object:"..sequence,"query:object:"..o,"") then
                        client6379:lpush("lexperimento_triple_object:"..sequence,"query:object:"..o)
                end
        end



	if not client6380:hexists("experimento_triple_object","value:property:"..p) then
                client6380:hset("experimento_triple_object", "query:property:"..p, "?s".." <"..tp.."> ".."?o")
		if client6379:hsetnx("hexperimento_triple_object:"..sequence,"query:property:"..p,"" ) then
			client6379:lpush("lexperimento_triple_object:"..sequence,"query:property:"..p)
		end
	end
	if not client6380:hexists("experimento_triple_object","value:"..key_st) then
                client6380:hset("experimento_triple_object", "query:"..key_st,  triple_s)
		if client6379:hsetnx("hexperimento_triple_object:"..sequence,"query:"..key_st,"") then
			client6379:lpush("lexperimento_triple_object:"..sequence,"query:"..key_st)
		end
	end

        if not client6380:hexists("experimento_triple_object","value:"..key_ot) then
                client6380:hset("experimento_triple_object", "query:"..key_ot,  triple_o)
                if client6379:hsetnx("hexperimento_triple_object:"..sequence,"query:"..key_ot,"") then
                        client6379:lpush("lexperimento_triple_object:"..sequence,"query:"..key_ot)
                end
        end

end

local function getPropertyTriple( p1 , p2)
	local tp1= p1:gsub("@","")
	local tp2 = p2:gsub("@","")

	tp1 =  client6380:hget("decode_url", tp1)
 	tp2 =  client6380:hget("decode_url", tp2)



        local triple1 = "?s <"..tp1.."> ?o ."
	local triple2 = " ?o <"..tp2.."> ?o1 . "
	
	local key_ot = p1..":"..p2
	if p1 > p2 then
		key_ot = p2..":"..p1
	end

	if p1:find("@") then
		 triple1 = "?o <"..tp1.."> ?s ."
	end
        if p2:find("@") then
                 triple2 = " ?o1 <"..tp1.."> ?o ."
        end

        if tp1 ~= tp2 then
			client6380:hset( "experimento_triple_object","query:"..key_ot, triple1..triple2)
			if client6379:hsetnx("hexperimento_triple_object:"..sequence, "query:"..key_ot,"") then
				 client6379:lpush("lexperimento_triple_object:"..sequence,"query:"..key_ot)
			end
        end
end

function createPathAndPattern( keyPath,path, list_properties,string_path)
	   local key_path=""
	   local list_arrows ={}
           if #path > 0 then

                   if client6379:hsetnx("hlist_paths:"..sequence,string_path,"") then
			number_path = number_path + 1

			key_path = keyPath ..":"..tonumber(number_path)

                        local duplicate_pair={}
			local idx_path = 0
                        local pair_properties = {}
			local list_objects = {}

                        for i=1,#list_properties do--{
			    idx_path = idx_path + 2
                            local s = path[idx_path-1]
  	 		    local p = path[idx_path]
			    local o = path[idx_path+1]
			
			    local arrow = s .. " " ..p.. " ".. o
			    getObjectTriple(s, p, o)

			    if client6379:hsetnx("hobjects:"..sequence,s,"") then
				   table.insert(list_objects,s)
			    end
                            if client6379:hsetnx("hobjects:"..sequence,o,"") then
				   table.insert(list_objects,o)
                            end
			    table.insert(list_arrows, arrow)
                        	local prop1 = list_properties[i]
                                local k = i+1
                                for j=k, #list_properties do --{for
                                        prop2 = list_properties[j]
                                        if prop1~=prop2 then -- if{
                                                local key =prop2..","..prop1
                                                if prop1 < prop2  then
                                                           key = prop1..","..prop2
                                                end
                                                if key and not duplicate_pair[key] then
                                                        if client6379:hsetnx("hcorr_prop:"..sequence,key,1) then 
									table.insert(pair_properties, key)
									 getPropertyTriple( prop1, prop2)
                                                        end
                                                        duplicate_pair[key]=""
                                                end
                                        end --} if
                                end --} for
                        end--}
			if #list_objects > 0 then
				client6379:lpush("list_objects:"..sequence,unpack(list_objects))
			end
			if #pair_properties> 0 then
				 client6379:rpush("list_property_pair:"..sequence, unpack(pair_properties))
			end
			if #list_arrows > 0 then
			       client6379:rpush(key_path,unpack(list_arrows))
			       client6379:rpush("list_paths_experimento:"..sequence,key_path)
			end
                  end --}
           end

end

local function delete_vertex_meta_path(lmeta,rmeta,node)

	lmeta= lmeta:gsub(":","")
        rmeta= rmeta:gsub(":","")

	local lkey_redis =lmeta..":meta:"..sequence..":".."L"
	local lmetapth = client6379:hgetall(lkey_redis) --vertice, path

        local rkey_redis =rmeta..":meta:"..sequence..":".."R"
        local rmetapath = client6379:hgetall(rkey_redis)

	for vertice,path in pairs(lmetapth) do
		if not (vertice== node) then
			local common = rmetapath[vertice]
			if common then
				client6379:hset("deshabilitado",path.."@"..common,"")
			end
		end
	end

end

function M.getIntersection(index,size_jobs)


	local keyPath = "path_triple:"..sequence..":"..id_process..":"..tostring(nivel) --name
	index = size_jobs - (index+1)
        local object =  client6379:zrevrange("z_wobject_direction:"..sequence..":"..nivel,index,index)

	object = object[1]

        if not object then
                return true
        end

	local insert_object = false
        local lpath = client6379:hgetall(object..":"..sequence..":".."L")
	local rpath = client6379:hgetall(object..":"..sequence..":".."R")
        
	if  object == send then
                  for lv,_ in pairs(lpath) do
                                	        lv = lv:gsub(object..":", object)
						lv = lv:gsub("  ", " ") --<--
						local recurr, path, properties = recurrent(lv)
                                        	if recurr then
							setObjectIntersection(object)
							 createPathAndPattern( keyPath,path, properties,lv) 
	                                        end
        	  end
	else
                  for lv,_ in pairs(lpath) do
	                                	for rv,_ in pairs(rpath) do
	        	                                	rv = rv:gsub(":"..object.." ", "")
		        	                                local tlv = lv:gsub(" "..object..":", "")
        		        	                        tlv = tlv .." ".. object.." "..rv
								tlv = tlv:gsub("  ", " ") --<----
								local recurr, path, properties = recurrent(tlv)
                	                		        if recurr then --get path
									local size= #properties
									size =  size/2
									if size <= distance then
										setObjectIntersection(object)
										createPathAndPattern( keyPath,path, properties,tlv) 
									end	
								end
	                	               end
	          end
	end
        if  object == start then
                  for rv,_ in pairs(rpath) do
					        rv = rv:gsub("  ", " ") --<--
                                	        rv = rv:gsub(":"..object, object)
						local recurr, path, properties = recurrent(rv)
                                        	if recurr then
							setObjectIntersection(object)
							createPathAndPattern(keyPath,path, properties,rv) 
        	                                end

                  end
	end
	return true
end
return M



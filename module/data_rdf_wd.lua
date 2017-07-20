local xmlp = require"xmlparser"
local redis = require 'redis_config'
local url_web = require"socket.url"
local local_data = require"mysql_data"
local entity_class = require"abstraction_entity_class"

local M = {
        _COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
	start_node = nil,
	end_node = nil,
        property_ignore=nil,
	MAX_LINKS = 30000,
	MAX_DISTANCE = 4,
        DISTANCE = 4,	
}

local client6379 =nil
local client6380 =nil

local start = nil
local direction = nil
local wiki_url=""
local url = nil
local sequence = nil
local action = nil
local database =nil
local nivel = nil
local run_start = false

local mapping_class={
"artist",
"movie",
"genre",
"television",
"actor",
"cinematographer",
"director",
"dubbing_performance",
"editor",
"film",
"musiccontributor",
"producer",
"production",
"writer"
 }

function M.init(seq,niv,act,db, node, direc, cli6379, cli6380, node_start, node_end, run_start_)
	sequence = seq
	action = act
	database = db
	nivel = niv

	if node then 
		start = node
		direction = direc
	end
	client6379 = cli6379
	client6380 = cli6380

	run_start = run_start_
		
        local_data.client6380 = cli6380
end

function M.getCodeUrl(url)
	local code =local_data.getCodeUrl(url)
	client6380:hsetnx("decode_url",code,url)
	return code
end

local function dbpedia_is(url,is_property, ntype)
          if url then
					isurl = nil
					if is_property then
	                                        isurl = url:find("^http")
					else
						isurl = url:find("^http://dbpedia.org/resource/")
					end
                                        if isurl then
							if not(M.property_ignore[url]) then
								local code_set = nil
                                                                if is_property then
									code = local_data.getCodeUrlProperty(url)
									code_set = code
									ntype =  tonumber(ntype)		
                                                                        code = (ntype>2) and code.."@" or code --get code properties in path
								else
									code =local_data.getCodeUrl(url)
									code_set = code	
                                                                end
                                                                client6380:hsetnx("decode_url",code_set,url)

                                                                return true, code
                                                        else
                                                                return false, nil

                                                        end

                                        else
                                                return false, nil
                                        end
          else
                                        return false,nil
           end

end
local filter=
{
		is= { 
			wikidata= function(url)
			        if url and (string.match(url, "^Q") ) then
					return true
				else
					return false
				end
			 end,
			 dbpedia = function(url,is_property, ntype)
				  return dbpedia_is(url,is_property, ntype)			
			 end
		},
		start={ 
			dbpedia = function(url,is_property, ntype)
				return dbpedia_is(url,is_property, ntype) --get code
			end
		},
		code ={
			dbpedia = function(code)
				return client6380:hget("decode_url",code)--get url
			end
		},
		
}

local function split(string)
local parts ={}
local str =""
local temp ={}
for word in string.gmatch(string, "%S+") do
	if not temp [word] then
	       temp [word] = ""
		table.insert(parts,word)
	       str = word .." ".. str .. " "
	end
end
return str,parts

end



local function reverseTable(t)
	local rev={}
	for i=#t, 1, -1 do
		if (i%2) == 0 then
			local is_inv  =t[i]:find("@")
			if is_inv then
				t[i] = t[i]:gsub("@","")
				table.insert(rev,t[i])
			else
				table.insert(rev,t[i].."@")
			end
		else
			table.insert(rev,t[i])
		end
	end
	local str = table.concat(rev, " ")
	return str
end

local function recurrent(string,direction)
        local rev={}
	local index = 1
	local lword =""
	for word in string.gmatch(string, "%S+") do
		lword =  word
		if not((index%2)==0) then --nodes
			if rev[word] then
				return false
			else
				rev[word] =""
			end
		end
		if index ==1 and direction =="L" then
			if not( word==M.start_node) then
				return false
			end
		end
		index =  index + 1
        end

        if direction =="R" then
                        if not( lword==M.end_node) then
                                return false
                        end
          end

	return true

end
local callback

local function load_description (desc)
        local temp=  "{"..desc.."}"
        local entity_property =  loadstring("return "..temp)()
	return entity_property
end
local function get_data_description(url,code)

    local description = client6380:hget(url,"description")

    if not description then
              description = local_data.get_description(code) --code
    end

    local entity_property = nil
    if not description then

            local temp_url = url:gsub("http://dbpedia.org/resource/","")
            local n3_url =  "http://dbpedia.org/page/"..temp_url

            temp_url = "http://dbpedia.org/page/"..url_web.escape(temp_url)
            entity_property = xmlp.getStructure("dbpedia",temp_url, n3_url)
            description=""
	    if entity_property then
        	    for property, values in pairs(entity_property) do
                	local tmp_desc = xmlp.callbacks.serializedb[database](values,property)
                	description = description..tmp_desc..","
            	    end
	    end
        else

	    local  status, e = pcall(load_description, description)
	    if not status then
	      return nil, nil
	    end
            entity_property =  e 
    end
    if description and  #description >0 then
            return description, entity_property
    end
    return nil, nil
end

local function get_number_links(entity_property)
        local count = 0
        for property, values in pairs(entity_property) do
                if values then
                        for _, values in pairs(values) do
                                if type(values)=="table" then
                                        for _,link in pairs(values) do
                                                local f = link:find("^http://")
                                                if f then
                                                        count = count +1
                                                end
                                        end
                                end
                        end
                end
        end
        return count
end
local function isIntheDomain(entity_class,  code)
	if not entity_class then
		return
	end
        local isindomain = false
        entity_class = entity_class:lower()
        for k, v in pairs(mapping_class) do
                if entity_class:find(v) then
                        isindomain =  true
                        break
                end
        end
        if isindomain then
                client6379:hset("domain:entity", code,1)
        end
end
local function execute_immediate_class(entity_property, description, url,class_instance,code)
		local str = nil
                if class_instance then
			isIntheDomain(class_instance, code)
                        client6379:hset("h_wobject_direction:"..sequence ,url,class_instance)
                        str = entity_class.get_abstract_class(code,sequence,client6379,client6380,class_instance, description)
                        if str then
                                client6380:hset(url,"immediate_class",str)
                                client6380:hset(url,"description",description)
                                local size = get_number_links(entity_property)
                                client6380:hset(url,"size",size)
                        end
                end
		return str

end
local function get_immediate_class(url,code)
        local str = client6380:hget(url,"immediate_class")
        local exist_domain = client6379:hexists("domain:entity", code)
	local description = nil
	local entity_property = nil
        local class_instance = nil

	if (not str) or (not exist_domain) then
		description, entity_property = get_data_description(url,code)
		local _,ci = callback.type_entity[database](entity_property)
		class_instance =  ci

	end
	
	if not exist_domain then
		isIntheDomain(class_instance,code)
	end


        if not  str then
        	 if description and entity_property then
			str = execute_immediate_class(entity_property, description, url, class_instance,code)
	        end
	        if str then
        	        return str
	        end
	end
end
local function give_immediate_class(url,code)
	local str = client6380:hget(url,"immediate_class")
        local temp=nil
        if str then
                for name_class in string.gmatch(str, "([^,]+)") do
                        temp = name_class
                        break
                end
        end
        return temp
end

local function set_intersection_node(node, direction)
      if not client6379:hsetnx("h_intersection_direction:"..sequence,node..":"..direction,"") or ( node == M.start_node or node == M.end_node ) then
                if client6379:hsetnx("h_teste_intersection:"..sequence..":"..nivel,node,"") then
	                local page_rank = local_data.get_page_rank(node)
        	        client6379:zadd("z_wobject_direction:"..sequence..":"..nivel,page_rank,node)
		end
      end
end

local side ={
        ["R"] = function(path,father,father_class,metapath)
                local final = path[#path]
                local string = reverseTable(path)
                local meta_string = reverseTable(metapath)
                local temp_meta = meta_string

                local temp = string
                string = ":"..string
                meta_string = ":"..meta_string

                if not client6379:exists(father..":"..sequence..":"..direction) then
                        if recurrent(temp,"R") then
                                client6379:hset(final..":"..sequence..":"..direction,string,meta_string )
                                set_intersection_node(final, "L")

                        end
                else
                        local listPaths = client6379:hgetall(father..":"..sequence..":"..direction)
                        for value, meta in pairs(listPaths) do
                                local _,o = value:gsub(" ", " ")
                                if o <= (M.DISTANCE) then
                                        value = value:gsub(":"..father.." ", "")
                                        meta = meta:gsub(":"..father_class.." ", "")

                                        local tem =  temp .." "..value
                                        value = string.." "..value
					local temp_m = temp_meta .." "..meta
                                        meta = meta_string .." "..meta
                                        if recurrent(tem,"R") then
                                                client6379:hset(final..":"..sequence..":"..direction,value,meta)
						set_intersection_node(final, "L")

                                        end
                                end
                        end
                end
        end,

        ["L"] = function(path,father, father_class, metapath)
                local final = path[#path]
                local string = table.concat(path, " ")
                local meta_string = table.concat(metapath, " ")
                local temp = string
                local temp_meta = meta_string
                string = string..":"
                meta_string = meta_string..":"
                if not client6379:exists(father..":"..sequence..":"..direction) then
                        if recurrent(temp,"L") then
                                client6379:hset(final..":"..sequence..":"..direction,string,meta_string )
                                set_intersection_node(final, "R")
                        end
                else
                        local listPaths = client6379:hgetall(father..":"..sequence..":"..direction)
                        for value,meta in pairs(listPaths) do
                                local _,o = value:gsub(" ", " ")
                                if o <= (M.DISTANCE) then

                                        value = value:gsub(" "..father..":", "")
                                        meta = meta:gsub(" "..father_class..":", "")
                                        local tem =  value.." " .. temp
                                        value = value .. " " .. string
                                        meta = meta .. " " ..meta_string
                                        value = value:gsub("  ", " ")
                                        meta = meta:gsub("  ", " ")
                                         if recurrent(tem,"L") then
                                                client6379:hset(final..":"..sequence..":"..direction,value,meta)
						set_intersection_node(final, "R")

                                        end
                                end
                        end
                end
        end

}

local function insertObject(code,value,typev)
	if (typev=="name") and (not client6380:hexists(code,typev)) then
		if not value then
			value="No label defined"
		end
		client6380:hset(code,typev,value)
	end
        if (value) and (not client6380:hexists(code,typev)) then
		 if value then
	               client6380:hset(code,typev,value)
		 end
        end
end

local function insertNode(node)
	if direction then
	       if client6379:hsetnx("h_wobject_direction:"..sequence..":"..nivel,node.."@"..direction,"") then
			client6379:lpush("l_wobject_extend:"..sequence..":"..nivel,node.."@"..direction)
			client6379:lpush("l_wobject_remote:"..sequence..":"..nivel,node.."@"..direction) 
		end
       end
end

local function execute_process(node, entity_property, exists)
        local description, triples
        local instance, class_instance
        description =""
	if exists then
		local size_links = client6380:hget(url,"size")
		if size_links then
				xmlp.size_links =  tonumber(size_links)
		end
		if xmlp.size_links==0 then
                        local nlinks = get_number_links(entity_property)
                        insertObject(url,nlinks,"size")
			xmlp.size_links = nlinks
			if (not nlinks) or  nlinks ==0 then
			     xmlp.size_links = 0
			end
		end
	end

	local temporal_description = ""

        for property, values in pairs(entity_property) do
               local code_start =M.start_node
               local code_end =M.end_node

               if run_start then
                      code_start =local_data.getCodeUrl(M.start_node)
                      code_end =local_data.getCodeUrl(M.end_node)
               end
	       local node_objective = false
               if (start == code_start) or (start == cod_end) then
			node_objective = true	
	       end

		if  nivel and (node_objective or run_start or (xmlp.size_links>0 and xmlp.size_links<M.MAX_LINKS) ) then 
			if action=="extract" then
				insertNode(start)
			        local temp_desc = callback.get_list[database](property, values) --immediate class
				if temp_desc then
                                        temporal_description = temporal_description..temp_desc..","
				end
			end
			if action=="extend"  then
				callback.extend[database](property, values) --get class
			end
                end
                if xmlp.callbacks.serializedb[database] and  (not exists) then --dbpedia
			local tmp_desc = xmlp.callbacks.serializedb[database] (values,property)
                        description = description..tmp_desc..","
                end
        end
	if action=="extract" then

		if not (temporal_description=="") then
                         client6379:hsetnx("hlist_entity_extend:"..sequence, url,temporal_description)
                end

		instance,class_instance = callback.type_entity[database](entity_property)
		client6379:hsetnx("h_wobject_direction:"..sequence,node,class_instance)
	end
	return description,instance,xmlp.size_links

end


callback ={
	extract = { 
		 dbpedia = function ()
                        local description =""
                        if not client6380:hexists(url,"description") and (action=="extract")then
				description = local_data.get_description(start)
				local size = nil
				if not description then
					local temp_url = url:gsub("http://dbpedia.org/resource/","")
			                local n3_url =  "http://dbpedia.org/page/"..temp_url
					temp_url = "http://dbpedia.org/page/"..url_web.escape(temp_url)
					local  entity_property= xmlp.getStructure("dbpedia", temp_url, n3_url)
			                if entity_property then
		                	        description,_,size = execute_process(start,entity_property, false)
					end
					
				else
  			                local  status, tb_description = pcall(load_description, description)
					if status then
	                                        local nlinks = get_number_links(tb_description)
						insertObject(url,nlinks,"size")
                		                execute_process(start, tb_description, true)
						size = nlinks
					end
				end
				if not(description  == "") then
	        	                insertObject(url,description,"description")
				end
                                insertObject(url,size,"size")
                        else

				if action=="extract" then
		                        description = client6380:hget(url,"description")
				else
					description = client6379:hget("hlist_entity_extend:"..sequence, url)
				end

				local  status, tb_description = pcall(load_description, description)
				if status then
                        		execute_process(start, tb_description, true)
				end
                        end
                end

	},
	extend = {
                dbpedia = function (property, values)
                        local code_start =M.start_node
                        local code_end =M.end_node
                        if run_start then
                               code_start =local_data.getCodeUrl(M.start_node)
                               code_end =local_data.getCodeUrl(M.end_node)
                        end
				
                        local path = {}
			local meta_path ={}
			local ic_start =  give_immediate_class(url,start)

			local validate, code_prop = filter.is[database](property,true,values.ntype)
                        if validate and ic_start and #ic_start>0 then
                                for  _,single_val in pairs(values.value) do
                                        path = {}
                                        meta_path ={}
                                        table.insert(path, start) 
                                        table.insert(meta_path,ic_start)
					single_val = url_web.unescape(single_val)
                                        local val, cod = filter.is[database](single_val,nil,nil)
                                        if val then
							
						if client6379:hexists("domain:entity", cod) then
        	                                	local ic =  give_immediate_class(single_val,cod)
							if ic and #ic >0 then
                                        	                table.insert(path,code_prop)
                                                	        table.insert(path,cod)
	                                                        table.insert(meta_path,code_prop)
        	                                                table.insert(meta_path,ic)
                        	                                insertNode(cod)
        	                               	                side[direction](path,start,ic_start, meta_path)
                        	        	         end
						end
					end
        		        end
                        end
 
                end,

	},
        get_list = {
                dbpedia = function (property, values)
                        local validate, code_prop = filter.is[database](property,true,values.ntype)

			local description=""
                        local code_start =M.start_node
                        local code_end =M.end_node

			if run_start then
	                       code_start =local_data.getCodeUrl(M.start_node)
        	               code_end =local_data.getCodeUrl(M.end_node)
			end
                        if validate then
				local val_list ={}
				local cod_list={}

	                        local temp_values ={["ntype"] = values.ntype, ["value"]={}}

                                for  _,single_val in pairs(values.value) do
					single_val = url_web.unescape(single_val)
                                        local val, cod = filter.is[database](single_val,nil,nil)

                                        if val then
						--local close = true
						--if not ((cod == code_start) or (cod==cod_end)) then
						--	close = local_data.distance_r(cod, code_start, code_end,M.MAX_LINKS,M.MAX_DISTANCE )
						--end
						--if close then	
		                                                local t = single_val:gsub('"','\\"')
		                                                t = t:gsub("'","\\'")	
								table.insert(temp_values.value, t)

		       						if client6379:hsetnx("h_wobject_immediate_extract:"..sequence,cod,"") then
									table.insert(cod_list, cod)
								end
								if client6379:hsetnx("h_wobject_immediate_class:"..sequence, start .."@"..cod,"")then 
								        table.insert(val_list,start.."@"..cod)
								end
						--end

                                        end
                                end
				if #temp_values.value >0 then

					description = xmlp.callbacks.serializedb[database] (temp_values,property)
				end
				if #cod_list>0 then
	                                client6379:rpush("l_wobject_extract:"..sequence..":"..nivel,unpack(cod_list))
				 end
					
			 	 if #val_list>0 then
	       		                        client6379:lpush("l_object_immediate_class:"..sequence..":"..nivel,unpack(val_list))
				 end
                        end

                        if not(description =="") then
                                return description
                        end
                        return nil

                end,

        },
        immediate_class = {
                dbpedia = function (url_target,code_target,url_source, code_source)

				pcall(get_immediate_class, url_target,code_target)
				local str = client6380:hget(url_target,"immediate_class")
				if str == nil then
					client6380:hset(url_target,"immediate_class", "Thing,0")
				end 

                end,

        },
	type_entity ={
			dbpedia = function(entity)
				local  name_property = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
				local class_instance =""
				if entity[name_property] then
					local classes ={}
					local list = entity[name_property].value
					for  _,value in pairs(list) do
						table.insert(classes, value)					
					end 
                                        if #classes > 0 then
                                                         class_instance = table.concat(classes," ")
                                        end
 
				end
				return nil, class_instance
				
			end,
	},
	transformation = {
			dbpedia = function(value)
				
			end,
	}
	
}

function M.generalProcess(entity)
                local x,y = string.find(entity, "@[%w]$")
                if x then
                        direction =  entity:sub(x,y)
                        start =  entity:gsub(direction,"")
                        direction =  direction:gsub("@","")
                else
                        start = entity
                end

                url = start 

                if filter.code[database] then -- only dbpedia
                        url = filter.code[database](start) --get url
                end
                callback.extract[database]()

end
function M.individualEntity()
	if start then
        	url = start --url

		if filter.start[database] then
			_,start =  filter.start[database](start,nil,nil) --get code
		end
		
		if not start then
			return
		end
		callback.extract[database]()
	end
end

function M.get_immediate_class(code_father_son)
		local subs =  code_father_son:find("@")
		local size = #code_father_son

	       local  code_entity =  code_father_son:sub(subs+1, size)
		local code_father =  code_father_son:sub(1, subs-1)

               local url_entity = client6380:hget("decode_url",code_entity)
               local url_father = client6380:hget("decode_url",code_father)

		if code_entity and code_father and url_entity and url_father then
	             callback.immediate_class[database](url_entity, code_entity, url_father,code_father )
		end
end

return M

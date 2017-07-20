#!/usr/bin/env lua5.2
local redis = require 'redis_config'

local array_distance ={}
local sort_distances={}
local classes_entity = {}
local texto
local classes_number
local description_entity 
local M = {
                _COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
                _DESCRIPTION = "Get Immediate Class",
}

local client6380 
local client6379

local function getDescriptionEntity(code,sequence)
	local list_classes =  client6379:hget("h_wobject_direction:"..sequence,code)
	local url = client6380:hget("decode_url",code)
	local description =  client6380:hget(url,"description")
	return list_classes, description
end
local function getName(url)
	local revers = url:reverse()
	local index = 0
	local index_char1 = revers:find("/")
	local index_char2 = revers:find("#")
	if index_char1  then
		index = index_char1	
	end
	if index_char2  then
		index = index_char2
	end

	if index_char1 and index_char2 then
		index = (index_char1 < index_char2) and index_char1 or index_char2
	end

	if index and (index >1) then
		revers = revers:sub(1,index-1)
		revers = revers:reverse()
		revers = revers:match("[^0-9]+")
		return revers
	end
end

local temp={}
local importance={}
local function sortImportanceClass(score,class)
	local row={}
        if #temp == 0 then
                row[1]=class
                row[2]=score
		table.insert(temp,row)
		table.insert(importance,class..",".. score)
		return
        end
	local indice = 1
	for k,v in pairs(temp) do
		indice = indice + 1
		if score <=v[2] then
		else
			indice = k
			break
		end	
	end
        row[1]=class
        row[2]=score
	if indice <= #temp then
        	table.insert(temp,indice,row)
        	table.insert(importance,indice,class..",".. score)
	else
		table.insert(temp,row)
		table.insert(importance,class..",".. score)
	
	end
        return
	
end
local function getRankingPopularity()
	for i=1,#sort_distances do
		local dist = sort_distances[i]
		distance =  tonumber(dist)
		if distance >=1 then
	 		local classes = array_distance[dist]
			for _,c in pairs(classes) do
				local dbpedia_class = c:lower()
				c = c:gsub("http://dbpedia.org/ontology/","")
				local name=dbpedia_class:gsub("http://dbpedia.org/ontology/","")
				local important =0
				local frequency = 1

                                local a,is_dbpedia_class = texto:gsub(dbpedia_class,"")
                                if (is_dbpedia_class >0)then
                                            important =10
                               end

				if  distance > 2 then
	                                local _,freq = description_entity:gsub(name,"")
					frequency = freq
					if frequency == 0 then
				   		frequency =  1		
					end
				end
                                local importance = ((distance+important) * (frequency/classes_number)) 
				sortImportanceClass(importance,c)
			end
		end
	end
end


local function getDirectClass(class)
	local distances = client6380:hget("list_class",class)
	if not classes_entity[class] then
	 	classes_entity[class]= distances
        	local d={}
		if (not array_distance[distances]) then
			table.insert(d,class)
			array_distance[distances] = d
			table.insert(sort_distances,distances)
		else
			d = array_distance[distances]
			table.insert(d,class)
		end
	end
end

local function getClassNames(list)
	for _,class in pairs (list) do
		local dbpedia, occu = class:gsub("http://dbpedia.org/ontology/","")
		if occu > 0 then
			getDirectClass(class)
		else
			local equiclass = client6380:lrange("equiclass_sec:"..class, 0, -1)
		        for _,ec in pairs (equiclass) do
		                dbpedia, occu = ec:gsub("http://dbpedia.org/ontology/","")
        	        	if occu > 0 then
					getDirectClass(ec)
				end
			end
		end
	end
end

local function getResult()
        table.sort(sort_distances)
	getRankingPopularity()
	if #importance > 0 then
		local string =table.concat(importance,",")
		return string	
	else
		return "Thing,0"
	end
 end

local function getListClass(result)
       for url in string.gmatch(result, "%S+") do
		classes_number =  classes_number + 1
                local name = getName(url)
		if client6380:hexists("hclass_ontology",name) then
	                client6380:zincrby("class_mapping:"..name,1, url)
		end
                if name then
			
                        name = name:match(("%a+"))
                        local class_wiki1 = client6380:lrange("synonym:"..name, 0, -1)
                        getClassNames(class_wiki1)
        	end
	end
 
end

local function getClass(name_entity,sequence)
	local local_texto, description = getDescriptionEntity(name_entity,sequence)
	if local_texto and description and (#local_texto > 0) and (#description > 0) then
		getListClass(local_texto)
		description_entity = description:lower()
		texto = local_texto:lower()
		return getResult()
	end
	return "Thing,0" 
end

function M.getClassPrincipal(name_string,sequence,cli6379, cli6380)
	client6380 = cli6380
	client6379 = cli6379
	array_distance ={}
	sort_distances={}
	classes_entity = {}
	importance = {}
	temp={}
	texto=""
	description_entity=""
	classes_number =0
  	return  getClass(name_string, sequence)
end

local function get_abstractClass(lclass,desc)
        local local_texto = lclass
        local  description =  desc
        if (#local_texto > 0) and (#description > 0) then
                getListClass(local_texto)
                description_entity = description:lower()
                texto = local_texto:lower()
                return getResult()
        end
        return "Thing,0"
end

function M.get_abstract_class(name_string,sequence,cli6379, cli6380,lclass,desc)
        client6380 = cli6380
        client6379 = cli6379
        array_distance ={}
        sort_distances={}
        classes_entity = {}
        importance = {}
        temp={}
        texto=""
        description_entity=""
        classes_number =0

        return  get_abstractClass(lclass,desc)
end


return M

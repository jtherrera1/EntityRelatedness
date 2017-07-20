#!/usr/bin/env lua5.2

local lxp= require"lxp"
local cURL = require("cURL")
local url_web = require"socket.url"
local http = require"socket.http"


local M = {
        _COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
}
local literal = false
local next_tag = false
local types=""
local property_name =nil
local entity_name=nil
local description=nil

local property ={}
local statement = 0
local index  = 0
local reference = false

local row = false
local column = 0
local valueProperty = false
local insertValue = false

local name_space ={}
local start_node =""
local object_value =""
local property_value = false
local temporal_list ={}

M.size_links = 0

local  function split_line(s)
        local auxLines = function ()
                for line in string.gmatch(s,'[^\r\n]+') do
                      coroutine.yield (line)
                end
        end
        return coroutine.wrap(auxLines)
end

local function getName(url)
        if url then
               url = url:gsub("/wiki/Property:","")
	       url = url:gsub("/wiki/","")
	       return url	
        else
                return url
        end
end

local function gethtml(url)
        local a,b,c = http.request( url)
        return a
end

function serialize(t, key)
  local serializedValues = {}
  local value, serializedValue
  for k,value in pairs(t) do
    local format = "[\"%s\"]=\"%s\""
    if type(k)=="number" then
        format = "[%s]=\"%s\""
    end
    serializedValue = (type(value)=='table' and serialize(value, k)) or string.format(format, k,value)
    table.insert(serializedValues, serializedValue)
  end
  if not key  then
    return string.format("{%s}", table.concat(serializedValues, ', ') )
 else
            local format = "[\"%s\"]={%s}"
            if type(key)=="number" then
                format = "[%s]={%s}"
            end

            return string.format(format, key,table.concat(serializedValues, ', ') )
 end

end

function ifExistReplace(list, value)
        if not property[property_name] then
                property[property_name] = list
        else
                list = property[property_name]
        end
        table.insert(list.value,value)
end
function getNameRDF(name)
        local ns = name:sub(1,4)
        if name_space[ns] then
                   name= name:gsub(ns..":",name_space[ns])
         else
                    ns = name:sub(1,3)
                    if name_space[ns] then
                             name= name:gsub(ns..":",name_space[ns])
                    end
                    return name
       end
       return name
end


local wiki_name = nil
M.callbacks ={
	wikidata = {
	StartElement = function( parser, name, attributes ) 
        if attributes["class"]=="wikibase-statementgroupview-property"  then
		types ="property"
		literal = true
		next_tag =  true
		statement = 0;
		index = 0
	end
	if attributes["class"]=="wikibase-title" then
		types ="entity_name"
	end

	 if attributes["class"]=="wikibase-entitytermsview-heading-description " then
		types ="description"
	end
	
	  
	if attributes["class"]=="wikibase-snakview-value wikibase-snakview-variation-valuesnak" then
		types = "value"
		literal = true
	end	

	if attributes["class"]=="wikibase-statementview-references-container" then
		reference = true;
	end

	if attributes["class"]=="wikibase-statementview-mainsnak-container" then
		types =""
		statement = statement+1
		index =  1
		reference = false
	end

	if next_tag and name =="a" then
		if types == "property" then
			property [ getName(attributes["href"])]={["list_statement"]={}} 
			property_name = getName(attributes["href"]) --//1
		end
		if types == "value" then 
			next_tag = false
		 	literal = true
			local list = property [property_name]["list_statement"]
			
                        if #list > 0 then
                                        local stat = list[statement]
					if not stat then
			                        stat ={["url"]=  getName(attributes["href"]),["list"]={},}
                                		table.insert(list, stat)
						return
					end
					local lvalues = stat["list"]
					if (#lvalues > 0) and (not reference) then   
                                                local val = lvalues[index]
                                                table.insert(val, getName(attributes["href"] ))
					end


					if #lvalues == 0 then
						     local val ={}
						     table.insert(val, getName(attributes["href"]) ) 
		                                     table.insert(lvalues,val)
					end
                         end
                         if #list == 0 then
                                local list= property [property_name]["list_statement"]
                                local stat ={["url"]=  getName(attributes["href"]),["list"]={}}
                                table.insert(list, stat)
                        end
		end
	end
    	end,	
	EndElement = function (parser, name)
	end,
	CharacterData = function (parser, str)
                str = str:gsub('\\','\\\\')
		str = str:gsub('"','\\"')
		str = str:gsub("'","\\'")

		if types=="entity_name"  and not (entity_name) then
			entity_name=str
		end
                if types=="description"  and not (description) then
                        description=str
                end

		if literal then
			next_tag = true
	                if types == "property" then
				local atributes = property [property_name]
				atributes["name"] = str--//2 --name position held
        	        end
                	if types == "value" then
				local list = property [property_name]["list_statement"]
				local stat ={["list"]={}}
				
				if #list > 0 then
						stat = list[statement] --statement = 1
						if stat then
							 local lvalues = stat["list"]--list values
							 if #lvalues > 0 then
	                	                                 local val = lvalues[index]
	                                                	        table.insert(val,str)
								if ((#val >=4) or (#val ==3) ) and (not reference) then
									index  = index + 1
									local newval ={}	
									table.insert(lvalues, newval)
								end
        	                                        end
							if  #lvalues == 0 then
	                                			stat["value"]= str ---value statemrnt
							end	
						end
				end
        	                if #list == 0 then--{
                	                        stat["value"]= str
                        	                table.insert(list, stat)
                                end
			end
		end
		literal = false
	end
        },
	triple={
		wikidata= function(prop)
		 local tab_prop ={}
		 local serialize_entity =""
		 for prop_key, values in pairs(prop) do
			serialize_entity = serialize_entity ..serialize (values,prop_key)..","
			local list_value ={}
			tab_prop ["p:"..prop_key.."/ps:"..prop_key] ={["name"]=values.name, list=list_value} 
	                 for  _,statement in pairs(values.list_statement) do
                                local val ={}
                                if statement.url and statement.url:find("^Q") then
                                        M.size_links = M.size_links + 1
                                end
                                val.url = statement.url
                                val.value = statement.value
                                table.insert(list_value,val)
                                for _,qualificator in pairs(statement.list) do
                                        local q_value ={}
					local lq_value ={}
                                        local key_qualificator_prop = qualificator[1]
                                        local key_qualificator_name = qualificator[2]

					if key_qualificator_prop then
						if not tab_prop ["p:"..prop_key.."/pq:"..key_qualificator_prop] then
							tab_prop ["p:"..prop_key.."/pq:"..key_qualificator_prop] ={["name"]=key_qualificator_name, list=q_value}
						else
							q_value = tab_prop ["p:"..prop_key.."/pq:"..key_qualificator_prop].list
						end

        	                                if qualificator[4] then
                	                               lq_value.url= qualificator[3]
		                                       if lq_value.url:find("^Q") then
			                                        M.size_links = M.size_links + 1
                        			        end

                        	                       lq_value.value= qualificator[4]
                                	        else
                                        	       lq_value.value= qualificator[3]
                                        	end
						table.insert(q_value,lq_value)
					end
                                end

			 end
		end
		return tab_prop,serialize_entity
	end
	},
	dbpedia = {
        StartElement = function( parser, name, attributes )
                if name =="tr" then
                        row = true
                        valueProperty = false
                end
                if name == "td" and row then
                        column = column + 1
                end
                if (column > 0) and name == "a" and not (valueProperty and insertValue) then
                        if not((column%2) ==0) then
                                if attributes["class"]=="uri" then
                                        property_name = attributes["href"]
                                        property[property_name]={["ntype"]=0,["value"]={}}
                                end
                        end
                end
                if (column > 0) and valueProperty then
                        if attributes["property"] then
                                if attributes["xml:lang"] then
                                        if  (attributes["xml:lang"]=="en") then
                                                insertValue = true
                                        else
                                                insertValue = false
                                        end
                                else
                                        insertValue = true
                                end
                        end
                        if attributes["class"]=="uri" then
				local tmp_desc = attributes["href"]
                                tmp_desc = tmp_desc:gsub("'","\\'")
                                tmp_desc = tmp_desc:gsub('"','\\"')

                                if tmp_desc:find("^http") then
					M.size_links = M.size_links + 1
				end
                                table.insert(property[property_name].value ,tmp_desc)
                                insertValue = false
                        end
                end
                if (column > 0) and name == "li"then
                        if (column%2) ==0 then
                                valueProperty = true
                        end
                end

        end,
        EndElement = function (parser, name)
                if name =="tr" then
                        row = false
                end
                if column > 0 then
                        if (column%2) ==0 then
                        else
                        end
                end

        end,
        CharacterData = function (parser, str)
                if (column%2) ==0 then
                         if insertValue then
			        str = str:gsub('\\','\\\\')
		                str = str:gsub('"','\\"')
		                str = str:gsub("'","\\'")
				if str:find("^http") then
					M.size_links = M.size_links + 1
				end
                                table.insert(property[property_name].value, str)
                                insertValue = false
                         end
                else
			if property[property_name] then
	                        property[property_name].ntype = property[property_name].ntype + 1
			end

                end
        end
        },
        dbpedia_n3={
                StartElement = function( parser, name, attributes )
                        temporal_list = {ntype =1, value={}}

                       if attributes["rdf:resource"] then --value
                                M.size_links = M.size_links + 1
                                property_name = getNameRDF(name)
                                if attributes["rdf:resource"] == start_node then
                                        temporal_list.ntype = 4
                                        ifExistReplace(temporal_list, object_value)
                                else
                                        ifExistReplace(temporal_list, attributes["rdf:resource"])
                                end
                        else
                                property_name = getNameRDF(name)
                                property_value = true
                        end

                        if name =="rdf:RDF" then
                                for key, value in pairs(attributes) do
                                        if type(key)=="string" then
                                                name_space[key:gsub("xmlns:","")] =value
                                        end
                                end
                                property_value = false
                        end
                        if name == "rdf:Description" then --object
                                if attributes["rdf:about"] ==start_node then
                                else
                                        object_value = attributes["rdf:about"]
                                end
                                property_value = false
                        end
                end,
                EndElement=function (parser, name)
                end,
                CharacterData = function (parser, str)
                        temporal_list = {ntype =1, value={}}
                        if property_value then
			        str = str:gsub('\\','\\\\')
			        str = str:gsub('"','\\"')
			        str = str:gsub("'","\\'")
                                ifExistReplace(temporal_list, str)
                                property_value = false
                        end
                end

        },
	serializedb={
			dbpedia =serialize
	},
        replace_url= function(url)
                start_node = url:gsub("http://dbpedia.org/page","http://dbpedia.org/resource")
                url =  url:gsub("http://dbpedia.org/page", "http://dbpedia.org/data")
                url = url..".rdf"
                return url
        end

}

function init()
        literal = false
        next_tag = false
        types=""
        property_name =nil
        property ={}
        statement = 0
        value  = 0
        index  = 0
	M.size_links = 0	
	row = false
	column = 0
	valueProperty = false
	insertValue = false

        name_space ={}
        object_value =""
        property_value = false
        temporal_list ={}

end
function setParser(name)
	init()
	local p = lxp.new(M.callbacks[name])
	return p
end

local entiname_wikipedia = nil
function giveStructure(name, url)
        entity_name =nil
        description =nil
	entiname_wikipedia = nil
        local text_html = gethtml(url)
	if not text_html then
		return nil
	end
        local p = setParser(name)
        for line in split_line(text_html)  do  -- iterate lines
            p:parse(line)          -- parses the line
	    if name == "wikidata" and( not entiname_wikipedia) then
		entiname_wikipedia = line:match("https://en.wikipedia.org/wiki/(.*)\" hreflang")
	    end
        end
	if entiname_wikipedia then
		entity_name = url_web.unescape(entiname_wikipedia)
	end
        p:parse()  
        p:close()
end
function M.getStructure(name, url, name_url)
	giveStructure(name, url)
        if  M.size_links == 0 then
                url = M.callbacks.replace_url(name_url)
                giveStructure("dbpedia_n3", url)
        end
        return  property,entity_name,description
end

return M

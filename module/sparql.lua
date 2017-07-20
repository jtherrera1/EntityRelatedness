iii#!/usr/bin/env lua5.2


local cURL = require("cURL")
local lxp= require"lxp"


local csv = require"csv"
local io = require 'io'
local http = require 'socket.http'
local url_s = require 'socket.url'
local socket = require 'socket'
local ltn12 = require 'ltn12'
local redis = require 'redis_config'
local string = require 'string'
local coroutine = require 'coroutine'
local table = require 'table'
local redis_path = require "redis_path"


local M = {
        _COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
	port=nil,
	client=nil,
	start_value=nil,
	sequence =  nil,
	id_query = nil,
	limit = nil,
	conn = nil
}

local key_query = 0
local cols_name =  {}
local list_rows = {}

local response={}
local LIMIT = 0
local local_result ={}
local local_pattern =""
local local_entity_pairs ={}

-- --------------------------------------------------------------------------
-- URL-encode a string (see RFC 2396)
-- --------------------------------------------------------------------------
function M.escape (str)
   str = string.gsub (str, "\n", "\r\n")
   str = string.gsub (str, "([^%w ])",
                function (c) return string.format ("%%%02X", string.byte(c)) end)
   str = string.gsub (str, " ", "+")
   return str
end


function M.query (d)
	
   local graph = d.graph_uri or "http://dbpedia.org"
   local query_string = d.query

   local query = M.escape(query_string)
   local graph_uri = M.escape(graph)

    local host ="http://194.109.129.59"
    if M.client then
	    host = M.client:hget("config_search:"..M.sequence,"endpoint") or "http://194.109.129.59"

    end	

   local endpoint = host.."/sparql?default-graph-uri=" .. graph_uri .. "&query=" .. query .. "&format=csv&timeout=20000&debug=on"
   local sink
   http.request({
   method = "GET",
   url = endpoint,
   headers = {   ["Content-Type"] = "application/x-www-form-urlencoded",   },
              sink = ltn12.sink.table(response),
   }
   )
   local texto = table.concat(response)
   local dados, cols = csv.carrega{
        texto = texto,
        tem_cabecalho =true,

   }

  response={}
	
  return dados,texto
end

function M.stream(d)
   local graph = d.graph_uri  or "http://dbpedia.org"
   local query_string = d.query_string or   [[
                           select distinct ?s ?p ?o 
                           where {?s ?p ?o } 
                     ]]
   local query = M.escape(query_string)
   local graph_uri = M.escape(graph)

   local file = "/sparql?default-graph-uri=" .. graph_uri .. "&query=" .. query .. "&format=rdf/xml&timeout=20000&debug=on"
   local c = assert(socket.connect(host, 80))
   c:send("GET " .. file .. " HTTP/1.0\r\nHost: 194.109.129.59\r\n\r\n")
	
   key_query =d.key
   M.retrieveText(c,d.id)

end

local tcount = 0    -- counts number of bytes read
local tline =''
local theader = true

function test(chunk, err)
              if chunk then
				  M.newretrieveText(chunk,M.id_query)
              else
                        return  nil
              end
	return true, sinkk
end


function getEntitiesRalated(chunk, err)
              if chunk then
                                  M.getEntities(chunk)
              else
                        return  nil
              end
        return true, sinkk
end


function configQuery(d,f)
        local graph = d.graph_uri or "http://dbpedia.org"
        local query_string = d.query_string
        local query = M.escape(query_string)
        local graph_uri = M.escape(graph)

	local host = "http://194.109.129.59"
	if M.sequence then
   		 host = M.client:hget("config_search:"..M.sequence,"endpoint") or host
	end

        local endpoint = host.."/sparql?default-graph-uri=" .. graph_uri .. "&query=" .. query .. "&format=csv&timeout=20000&debug=on"
        http.request({
                method = "GET",
                url = endpoint,
                headers = {   ["Content-Type"] = "application/x-www-form-urlencoded",   },
                sink = ltn12.sink.simplify (f)
           }
        )

end

function M.newstream (d)
        tcount = 0    -- counts number of bytes read
        tline =''
        theader = true

	configQuery(d,test)
	M.getPaths()
end

function M.EnitiesRelated (d, pattern)
        tcount = 0    -- counts number of bytes read
        tline =''
        theader = true
	LIMIT = 0
        local_result ={}
	local_pattern =pattern

	configQuery(d,getEntitiesRalated)
end



function M.getVariables (query)       
	local sub = string.match(query,"{(.-)%FILTER")
	if sub==nil then
		    sub = string.match(query,"{(.-)%}")
	end

	sub= sub:gsub("<","")
	sub= sub:gsub(">","") 
	return sub
end

function M.retrieveText(c,id)
   local count = 0    -- counts number of bytes read

   local line =''
   local header = true
   while true do
      count = count + 1
      local s, status = M.receive(c)
      local linel, text
      if s and (#s > 0) then
         text,linel =  M.getText(s)
        if text~='' then   
           if header then
              local result, dados, cols = M.retrieveHeader(text)
              if not result then
                  if text then   line = text   end
              end
              cols_name = cols
	      if  dados  then  M.writeRedis(dados,M.client, cols_name, id, count, true) end  	
              header = false
           else
              text = line..text
              line=''
              local result,dados = M.getRemainder(text)
              if not result then   line = text   end
              if  dados  then  M.writeRedis(dados,M.client,cols_name, id,count, false)  end 
           end
        end
        if linel ~='' then
            line = line..linel
        end
      end 
      if status == "closed" then 
         break 
      end
    end --while
    c:close()

  end


function M.newretrieveText(c,id)
   tcount = tcount + 1
   local s = c
   local linel, text
   if s and (#s > 0) then
         text,linel =  M.getText(s)
        if text~='' then  
           if theader then
              local result, dados, cols = M.retrieveHeader(text)
              if not result then
                  if text then   tline = text   end
              end
              cols_name = cols
              if  dados  then  M.writeRedis(dados,M.client, cols_name, id, tcount, true) end
              theader = false
           else
              text = tline..text
              tline=''
              local result,dados = M.getRemainder(text)
              if not result then   tline = text   end
              if  dados  then  M.writeRedis(dados,M.client,cols_name, id,tcount, false)  end
           end
        end
        if linel ~='' then
            tline = tline..linel
        end
   end
end 


function M.getEntities(c)
   local s = c
   local linel, text
   if s and (#s > 0) then
         text,linel =  M.getText(s)
        if text~='' then 
           if theader then
              local result, dados = M.retrieveHeader(text)
              if not result then
                  if text then   tline = text   end
              end
              if  dados  then  M.writeRedisEntity(dados) end
              theader = false
           else
              text = tline..text
              tline=''
              local result,dados = M.getRemainder(text)
              if not result then   tline = text   end
              if  dados  then  M.writeRedisEntity(dados)  end
           end
        end
        if linel ~='' then
            tline = tline..linel
        end
   end
end

function M.writeRedisEntity(dados)

    if #dados > 0 then
	       local entity1 = ""
	       local entity2 = ""	
               for i=1, #dados do
                    entity1 = dados[i][1]
		    entity2 = dados[i][2]	
                    if entity1 and entity2 then
	 	            LIMIT = LIMIT + 1
			    if LIMIT < M.limit then
			    	local_result[LIMIT] = {entity1, entity2}
			    end
			    if not local_entity_pairs [entity1.."**"..entity2] then
				     local_entity_pairs [entity1.."**"..entity2] = ""
				    local value =  M.client:zincrby("zentities_related:"..M.sequence,1,entity1.."**"..entity2)
				    value = tonumber(value)
				    if value == 1 then
					    M.client:hset("patterns_related:"..M.sequence, entity1.."**"..entity2, local_pattern)
				    else
					    local patterns =  M.client:hget("patterns_related:"..M.sequence, entity1.."**"..entity2) 
					    if patterns then	
						    patterns = patterns.."**"..local_pattern
						    M.client:hset("patterns_related:"..M.sequence, entity1.."**"..entity2, patterns)
					    end
				    end
			    end
		    end
                end
     end
end



function M.writeRedis(dados, client,column, id,count, col)
    if #dados ==0 and col then
	client:del("variable:"..id)
        client:del("query:"..id)
    end

    if #dados > 0 then
	       for i=1, #dados do
		  local row={}	
        	  for c=1, #column do
			 row[column[c]]=dados[i][c]
	          end
		   list_rows[id..":"..count..":"..i]=row
		end
     end
  end

function M.getRemainder(texto)
   local dados, cols = csv.carrega{
        texto = texto,
        tem_cabecalho =false
   } 
    if dados then
       return true, dados,nil
    else
     return false, nil,nil
    end
  end

function M.retrieveHeader(texto)

   local dados, cols = csv.carrega{
        texto = texto,
        tem_cabecalho =true,
   } 

    if cols then
     return true , dados, cols
    else
     return false, nil, nil
    end

  end

function M.getText(text)
      local  line =  string.match(text, "[^%c]*$")--tiene final de line
      local  stext = string.gsub(text, "[^%c]*$", "",1) --tiene final de linea
      return stext,line
 end

function M.receive (connection)
      connection:settimeout(0)
      local s, status, partial = connection:receive(2^10)
      if status =="timeout" then
         coroutine.yield (connection)
      end
      return s or partial, status
 end

 M.threads = {}    -- list of all live threads

function M.get (d,client)
     cols_name =  {}
     list_rows = {}	
     local variables = M.getVariables(d.query_string)
    if variables == nil then
    end
     client:set("variable:"..d.id, variables)
     client:set("query:"..d.id, d.query_string)

      local co = coroutine.create(function ()
        M.stream(d) --corpo da coorotina
      end)
      table.insert(M.threads, co)
  end


function M.newGet (d,client)
     redis_path.conn = M.conn	
     cols_name =  {}
     list_rows = {}
     M.id_query = d.id
     local variables = M.getVariables(d.query_string)
     client:set("variable:"..d.id, variables)
     client:set("query:"..d.id, d.query_string)
     M.newstream (d)
end

function M.query_entities(d, pattern)
     cols_name =  {}
     list_rows = {}
     local_entity_pairs ={} 	
     M.EnitiesRelated(d,pattern)
     return local_result

end

function M.getPaths()
	redis_path.controlator(M.id_query,M.sequence,M.start_value,M.client, list_rows,cols_name)
end

function M.dispatcher ()

    while true do
	local n = #M.threads
       if n==0 then break end
       local connections = {}
       for i=1, n do	
		local status, res = coroutine.resume(M.threads[i])
	      	if not res then
		        table.remove(M.threads,i)
			break
	       else
		        table.insert(connections, res)
	       end
	end
	if #connections == n then
	     socket.select(connections)
	end
    end
    M.getPaths()
  end

--wikidata
local  function split_line(s)
        local auxLines = function ()
                for line in string.gmatch(s,'[^\r\n]+') do
                      coroutine.yield (line) --pattern, key, size
                end
        end
        return coroutine.wrap(auxLines)
end

local function query_wikidata(d)
        local c = cURL.easy_init()
        local query_string = d
        local query = M.escape(query_string)
        local host ="https://query.wikidata.org/sparql?query=" .. query .. "&format=xml"
        c:setopt_url(host)
        local texto =""
        c:perform{writefunction = function(str)
                texto = texto..str
         end}
        return texto
end

local name_variable =nil
local after_bind = false
local variables ={}
local variable_names ={}
local row_wiki ={}

local   call_query={
                StartElement = function( parser, name, attributes )
                        if name == "variable" then
                                table.insert(variable_names,attributes["name"])
                        end
                        if name_variable then
                                after_bind = true
                        end
                        if name == "binding" then
                                name_variable  = attributes["name"]
                                after_bind = false
                        end

                        if name == "result" then
                                row_wiki ={}
                        end

                end,
                EndElement=function (parser, name)
                        if name== "result" then
                                table.insert(variables,row_wiki)
                        end
                end,
                CharacterData = function (parser, str)
                        if (#str > 0) and after_bind and  name_variable then
                                table.insert(row_wiki,str)
                                name_variable =nil
                                after_bind = false
                        end
                end
        }

function M.giveResult( query )
        name_variable = nil
        after_bind = false
        variables ={}
        variable_names ={}
        row_wiki ={}
        local text_html = query_wikidata(query)
        local p = lxp.new(call_query)

        for line in split_line(text_html)  do
            p:parse(line)  
        end

        p:parse()
        p:close()
        return variables, variable_names
end

function M.generatePairs(query,pattern)
        LIMIT = 0
        local_result ={}
	local_entity_pairs ={}
        local_pattern =pattern

	local result = M.giveResult( query )
	M.writeRedisEntity(result)
	return local_result
end
return M 

#!/usr/bin/env lua5.2

local M = {
		_COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
		_DESCRIPTION = "Módulo Extrair Padroes",
	client = nil,
	parameter= nil,
	porcent=10,
        start_value=nil,
        end_value = nil,
}


local function compute_simples_measure(pattern,size)
  local measure = 1
  local number_instances = 0
  if size >  1 then
     if not  M.client:hexists("measure:"..M.parameter[1], pattern) then
         measure, number_instances = M.getInstanceVariableSimplePattern("pattern:"..M.parameter[1]..":"..pattern)
         M.client:hset("measure:"..M.parameter[1],pattern,measure)
         M.client:hincrby("final_pattern:"..pattern,"instances", number_instances)
     end
  else
      if not  M.client:hexists("measure:"..M.parameter[1], pattern) then
         M.client:hset("measure:"..M.parameter[1],pattern,measure)
         number_instances = M.client:llen("pattern:"..M.parameter[1]..":"..pattern)
         M.client:hincrby("final_pattern:"..pattern,"instances", number_instances)
      end
  end
  M.client:hset("final_pattern:"..pattern, M.start_value..":".. M.end_value,"")
end

--------------------------------------------------------------------------------------
-- recupera padrones de un tamanho definido
-- rehacer para todos los padrones encontrados
-- osea esta funcion deve recurpar "todos" los patrones en algun orden
-- combinacion de pares de padrões
------------------------------------------------------------------------------------------------------
function M.getPatterns(patterns, size_list)
 size_list = tonumber(size_list)
 local start_main =  M.parameter[2] 
 local start_main = tonumber(start_main)
 local end_main =  M.parameter[3]
 local end_main = tonumber(end_main)

 local auxPattern = function ()
      for i=1, #patterns do
          local f = patterns[i] -- implementar "isfrequent"  f-> key do padrão
--          local first = M.getSuffix(f) -- eliminar la sequencia
          local  p1,sizef = patternToTable(f)  -- {[a1]="1,2"
          compute_simples_measure(f,sizef)
          local  temp_start = start_secondary
          start_main = start_main + 1
          if start_main == size_list then
  --            print(start_main)
             break
	  end
  --         print("principal",start_main, end_main)
           for _secondary_patt in getIntervalPatterns(start_main, size_list) do
                for j =1 , #_secondary_patt  do
                   local s = _secondary_patt[j] --isfrequent
  --                 local second = M.getSuffix(s) -- eliminar la sequencia
                   local  p2,sizes = patternToTable(s)
                    compute_simples_measure(s,sizes)
 
                   if sizef > 1 and sizes > 1 then 
                      coroutine.yield ({p1,f,sizef}, {p2,s,sizes}) --pattern, key, size
                   end 
               end
             end 
      
      end 
  end
  return coroutine.wrap(auxPattern)
end
function getIntervalPatterns(start_main, size)
    local length = size
    size  =  size -(start_main-1)

    local interval = math.floor( ( (size+1)*M.porcent)/100)
    local number_intervals = 0
    local starti = 0
    local endi = 0
    starti = start_main
    if interval == 0 then 
       number_intervals =1
       endi = length      
    else
       number_intervals = math.floor( (size+1)/interval)
       endi = start_main+interval
    end

    local auxPattern= function()
          for i = 1, number_intervals  do
              --     starti  1313    1316    12            36
            local patterns= M.client:zrevrange("freq_pattern:"..M.parameter[1], starti , endi) -- start=0, end=-1
            coroutine.yield (patterns)
            starti = endi + 1
            if(starti <= length) then
               endi = (interval+endi) + 1
               if endi >= length then
                 endi = length
               end
            else
		break
            end
          end 
    end
    return coroutine.wrap(auxPattern)
 end

-- combinacion de padrones!
-- (1) verificar si el padrao tiene alguna interseccion de propiedades
  -- comparacion de padrones del mismo tamanho
  -- despues considerar la junta de dos padrones de diferente tamanho
      -- compara inicio (1)
      -- compara el cuerpo (2)
      -- cosiderar la posicion, desconsiderar las primeras posiciones, en el cuerpo
      --      x         x
      --     2  5   3   5
      --      [ corpo ]
      --     x           x
      --      [ corpo ]
      --     5  2 3 5    3
      --            ^ aqui acabaria la comparacion
      -- compara  fin (3)
   -- definir orden de la aresta

function M.getLocalCombination(t1,t2)
   local p1 = t1[1]
   local p2 = t2[1]
   local k1 = t1[2]
   local k2 = t2[2]
   local size1 = t1[3]
   local size2 = t2[3]

   --local index =1
  local localcombination ={}
  for k, tv in pairs(p1) do  --pattern value with a key
       if p2[k]  then  --validar si existe la key en el pattern 2 
          local tp2 = p2[k] --p1=["b1"]={1,2}
          for i,v1 in pairs(tv) do ---combinar valores de p1 con p2
            local t ={}
            for j,v2 in pairs(tp2) do --valores de p2[k]
               -- v1 == 1
               -- entonces v2==1
               -- v1 == #p1
               -- v2 == #p2                                  
               local condition1 = ((v1==1) and (v2==1)) or ((v2==size2) and (v1==size1))
               local condition2 = ((v1==1) or  (v2==1)) or ((v1==size1) or (v2==size2))

               if condition1 then
                  localcombination [v1] = {v2}
               else
                  if not ( condition2 ) then
                     local var = localcombination [v1] --armazenar solo valores 
                     if not var then -- si ya existe solo concatenarlo
                        local t ={}
                        t[v2] = v2  
                        localcombination [v1] = t
                     else
                        var [v2] = v2
                        localcombination [v1] = var
                     end
                  end
               end 
             end
           end         
       end
  end

  ------------------------------
--  p1 = toTable (f)
--  p2 = toTable (s)
  --print(string.gsub(tostring(p1),"table: ","" ),p2)
  --combinacion de tamanho 1 en localcombination
--  local key1 = string.gsub(tostring(p1),"table: ","" )
--  local key2 = string.gsub(tostring(p2),"table: ","" )

--  print(key1,key2) --padron key pattern:sequencia:key1:key2:2

 if #localcombination >0  then
    stringCombination = serialize(localcombination,nil)
--  print(stringCombination)

    local listname = "listD:".. M.parameter[1]..":"..k1 --string.gsub(k1, "pattern", "listD")
    M.client:hset(listname, k2, stringCombination)
-- considerar eliminar este punto, porque la juncion depende de las relaciones ya no de los caminos

--  listname =  "listI:".. M.parameter[1]..":"..k2--string.gsub(k2, "pattern", "listI")
--    M.client:hset(listname,k1, stringCombination)
 end
  
end

function serialize(t, key)
  local serializedValues = {}
  local value, serializedValue
  for k,value in pairs(t) do
    serializedValue = (type(value)=='table' and serialize(value, k)) or value
    table.insert(serializedValues, serializedValue)
  end
  if not key then
    return string.format("{%s }", table.concat(serializedValues, ', ') )
 else
    return string.format("[%s]={ %s }", key,table.concat(serializedValues, ', ') )

 end 
  
end


function M.serialize_graph(t, key)
  local serializedValues = {}
  local value, serializedValue
  for k,value in pairs(t) do
    serializedValue = (type(value)=='table' and M.serialize_graph(value, k)) or string.format("[\"%s\"]=\"%s\"", k,value)
    table.insert(serializedValues, serializedValue)
  end
  if not key then
    return string.format("{%s}", table.concat(serializedValues, ', ') )
 else
    return string.format("[\"%s\"]={%s}", key,table.concat(serializedValues, ', ') )
 end

end
function M.getArrow(arrow)
   local temp_arrow = {}
   for p in arrow:gmatch("%S+") do
       table.insert(temp_arrow,p)
   end
   return temp_arrow
end

-- create graph and add path
function M.getGraph(graph,path)
   if not graph then
        graph = {}
   end
   for i=1, #path do
     local arrow = path[i]  
      arrow = M.getArrow(arrow)
      local temp = {}
      if i == #path then
           graph[arrow[3]] = arrow[3]
      end  
     

      if not graph[arrow[1]] then
           temp[arrow[2]..","..arrow[3]] = 1
           graph[arrow[1]] = temp
      else
          temp  = graph[arrow[1]]
          if type(temp)=='table' then 
           temp [arrow[2]..","..arrow[3]] = 1
          end
      end
     
   end
   return   graph  --serialize(graph,nil)
end

local function orderArrow(descendent)
    local order = {}
    for k, v in pairs (descendent) do
        local insert = true
        for i=1, #order  do
           if k < order[i]  then
              table.insert(order,i, k)
              insert = false
              break
           end
        end
        if #order ==0  then
           table.insert(order,k)
           insert = false
        end
        if insert then
           table.insert(order, k)
        end
    end
    return order
end


local function orderWalk(descendent)
    local order = {}
    for  v, k in pairs (descendent) do
        local insert = true
        for i=1, #order  do
           if k < order[i]  then
              table.insert(order,i, k)
              insert = false
              break
           end
        end
        if #order ==0  then
           table.insert(order,k)
           insert = false
        end
        if insert then
           table.insert(order, k)
        end
    end
    return order
end
local function getArrowNode(arrow)
      local sp = arrow:find(",")
      local predicate = arrow:sub(1,sp-1)
      local next_node = arrow:sub(sp+1)
 return predicate, next_node
end


local function isSameNode(list_node)
    local node = list_node[1]
    for i=2, #list_node do
        if node ~= list_node[i] then
           return false
        end
        node = list_node[i]
    end
    return  true
end


local order_walk = {}
local hash_walk ={}
local hash_node ={}
local function insertWalk(walk, old)
     table.insert(order_walk, walk)
     local hw  = hash_walk[walk]
     if not hw then
        hw ={}
        table.insert(hw ,  old)
        hash_walk[walk] =  hw
      else
        table.insert(hw ,  old)
      end
end
local function getBreadthFirstSearch(graph,old,node, walk, length)
    length =  length - 1

    if graph[node] and not hash_node[node] then --{
       if type(graph [node])=='table' then --{ type
          hash_node[node] =true -- ****ciclos
          local descendent = graph [node]
          local search_node = false
          for arrow, v in pairs (descendent) do--{for
              local predicate, next_node = getArrowNode(arrow)
               if length > 0 then --llego a la longitu
                   search_node = (getBreadthFirstSearch(graph,old, next_node, walk..predicate, length) or search)
               else
                     insertWalk(walk..predicate, old)
                     return true
                 end
          end--}for
          return search_node -- fin de recursividad -- todos los nodos
       else
         insertWalk(walk, old)
          return false --llego al final
       end--}type
   else
       hash_node[node] =true-- **ciclos
       insertWalk(walk, old)
       return false --llego al final
   end --}
end
local function  getNewOrder(graph,list_node,predicate, walk)
   order_walk = {}
   hash_walk ={}
   local orde_new ={}
   local all_walk = {}
   local length = 1
   local search = true

   while search do --{while
        search = false
        for i=1, #list_node do
           search =  getBreadthFirstSearch(graph,list_node[i]..","..tostring(i),list_node[i] , predicate, length) or search
           hash_node = {} -- recorrido por cada busqueda
        end
        all_walk = orderWalk(order_walk)
        if search then --{ if
          search = false
          for i=1, #all_walk do  -- {for
             local hs = hash_walk[all_walk[i]]
             if #hs > 1 then
               order_walk = {}
               hash_walk = {}
               hash_node = {}
               length = length +1
               search = true
               break
             end
          end--} for
        end-- if
   end --} while

   local nodes ={}
    for i=1, #all_walk do
        local temp = all_walk[i]
        if  hash_walk[temp] then
            local key_node = hash_walk[temp]
            for j=1, #key_node do
                local node=  getArrowNode(key_node[j] )
                if not nodes[node] then
                   table.insert(orde_new,node )
                   nodes[node]=node
                end
            end
            hash_walk[temp] = nil
        end
    end
  orde_new = orderWalk(orde_new)
  return orde_new
end

local function orderArrowPattern(graph,descendent)
     local same_arrow = {}
     local order = {}
     local node_index={}
     local same = false
     for arrow, v in pairs (descendent) do--{for
          local insert = true
          local predicate, next_node = getArrowNode(arrow)
          if  not  same_arrow [predicate] then
              same_arrow [predicate]= {}
              table.insert( same_arrow [predicate],next_node)
          else
              table.insert( same_arrow [predicate],next_node)
          end

          for i=1, #order  do
             if predicate < order[i]  then
                table.insert(node_index,i,arrow)
                table.insert(order,i,predicate)
                insert = false
                break
             end
          end
          if #order ==0  then
             table.insert(node_index,arrow)
             table.insert(order,predicate)
             insert = false
          end
          if insert then
             table.insert(node_index,arrow)
             table.insert(order,predicate)
          end
     end --} for
     local final_order = {}
     for i=1,#node_index do
             local arrow = node_index[i]
             local predicate, next_node = getArrowNode(arrow)
             local list_nodes = same_arrow [predicate]
             if list_nodes and #list_nodes > 1 then --{ if
                local new_order ={}
                if not isSameNode(list_nodes) then --{ same node
                    new_order = getNewOrder(graph,list_nodes,predicate)
                     for j=1, #new_order do
                        table.insert(final_order, predicate..","..new_order[j])
                     end
                    i = i+ (#new_order-1)
                else
                    for j=1, #list_nodes do
                       table.insert(final_order, predicate..","..list_nodes[j])
                    end
                   i = i+ (#list_nodes-1)


                 end --} same node
                 same_arrow [predicate] = nil
             else
                   if list_nodes then
                      table.insert(final_order, arrow)
                   end
             end --} if
     end
     return final_order
end
local representation = ""
local code ={}
local index_code =1

local  uniq_node ={}

function setInstancesNode(node_ppt,node)
  if not uniq_node[node_ppt] then
     local temp ={}
     temp[node]=""
     uniq_node[node_ppt] = temp
   else
     local temp =  uniq_node[node_ppt]        
     temp[node]=""
   end
end

local function getCodeNode(node)
  if not code[node] then
       code[node]= index_code
      index_code = index_code +1
  end
  setInstancesNode(code[node],node)

  return code[node]
end

local final_node =""
local function getRepresentationPattern(graph, start)
    if graph[start] then
       if type(graph [start])=='table' then
          local descendent = graph [start]
          local order = orderArrowPattern(graph,descendent)
          graph[start]=nil
          for i=1, #order do
             local arrow = order[i]
             local predicate, next_node = getArrowNode(arrow)
             local st_node = getCodeNode(start)
             local nx_node = getCodeNode(next_node)
            if M.end_value == next_node then
                final_node = nx_node
             end

             representation = representation ..st_node..","..predicate..","..nx_node..","
             getRepresentationPattern(graph,  next_node)
             order = orderArrowPattern(graph,descendent) --generar nuevo orden
          end
       end
    end
    graph[start]=nil
    return
end

local function getRepresentationGraph(graph, start)
    if graph[start] then
       if type(graph [start])=='table' then
          local descendent = graph [start]
          local order = orderArrow(descendent)
          graph[start]=nil
          for i=1, #order do
             local arrow = order[i]
             local predicate, next_node = getArrowNode(arrow)
             representation = representation ..start..","..predicate..","..next_node..","
             getRepresentationGraph(graph,  next_node)
          end
       end
    end
    graph[start]=nil
   return
end

function M.getMainRepresentationGraph(graph)
      representation = ""
      code ={}
      index_code =1
      getRepresentationGraph(graph, M.start_value)
      return representation
end

function M.getMainRepresentationPattern(graph)
      final_node=""
      representation = ""
      code ={}
      uniq_node = {}
      index_code =1
      getRepresentationPattern(graph,  M.start_value)
      return representation .."@"..final_node
end


function M.toTable (pattern)
  local t ={}
  for arrow in string.gmatch(pattern, "%a+%d+[@]*") do
     table.insert(t,arrow)
  end
  return t
end

function M.toTableSpace (pattern)
  local t ={}
  for arrow in string.gmatch(pattern, "%S+") do
     table.insert(t,arrow)
  end
  return t
end


function M.toProperties (pattern)
  local t ={}
  local index  = 1
  for arrow in string.gmatch(pattern, "%S+") do
	if (index % 2 ) ==0 then
	     table.insert(t,arrow)
	end
	index  = index +1
  end
  return t
end



function patternToTable(pattern)
  local t ={}
  local index =1
  for arrow in string.gmatch(pattern, "%a+%d+[@]*") do
     if not t[arrow] then
        local s ={}
        s[index] = index       
        t[arrow]= s
     else
        local  s= t[arrow]
        s[index] = index
        t[arrow] = s
     end 
     index = index +1   
  end
  return t, index -1
end

function M.getPositionIntersection(key_pattern)
  local i,_ = string.find(key_pattern,"}:")
  local intersection =  string.sub(key_pattern,1,i)
  local table_result = loadstring("return "..intersection)()
  return table_result
end

function M.getIntersection(key_pattern)
  local i,_ = string.find(key_pattern,"}:")
  local intersection =  string.sub(key_pattern,1,i)
  return intersection 
end



function M.getPropertiesPosition(representation)
  local array = {}
  for arrow in string.gmatch(representation, "%a+%d+[@]*")do 
      table.insert(array,arrow) 
  end
  return array
end
function  M.getclone (t)
  local temp = {}
  if not t then
     return temp
  end
	
  for i=1, #t do
     table.insert(temp, t[i])
  end
 return temp
end

function M.getPatternStrings(pattern)
    local list ={}
    for pp  in string.gmatch(pattern,"[^,]+") do
        list[#list+1] = pp
    end
    return list
end

local function getMonocount(variables)
  local count ={}
  for i,v in pairs (variables) do
    for k,_ in pairs (v) do
       if (k ~=  M.end_value) and (k ~=  M.start_value) then
         if not   count[i] then
           count[i] = 1
         else
           count[i] = count[i] + 1
         end
     end
    end
  end
  
  count = orderWalk(count)
  return count[1]
  
end

local function compute_measure( key_pattern)
  uniq_node ={}
  local instances = M.client:lrange(key_pattern, 0, -1)

  for _ ,inst in pairs (instances) do
       local table_path = M.client:lrange("path_triple:"..M.parameter[1]..":"..inst, 0, -1)
           for i=2, #table_path do
              local arrow= M.getArrow(table_path[i-1])
              local object = arrow[3]
              if  not uniq_node [i-1] then
                 local temp = {}
                 temp [object] =""
                 uniq_node [i-1] = temp
               else
                 local temp = uniq_node [i-1]
                 temp[object] = ""
              end
           end
  end
  return #instances
end

local function getPattern(path_triple)
  local pattern = ""
  for i =1, #path_triple do
      local arrow = path_triple[i]
      arrow = arrow:sub(arrow:find( "%s[%a]*[%d]*[@]*%s")) 
      arrow = arrow:gsub("^%s*(.-)%s*$", "%1")
      pattern = pattern .." "..arrow
  end
  pattern = pattern:gsub("^%s*(.-)%s*$", "%1")
  return pattern
end
function M.getMeasureComplexPattern(instances, rep_pp,measure)
   local measure_old = M.client:hget("measure:"..M.parameter[1],rep_pp)
   if measure_old then
       measure_old = loadstring("return "..measure_old)()
       for m1, var_pair in pairs (measure) do
           local ixstr= tostring(m1)   
           local var = measure_old[ixstr]
           for x, _ in pairs(var) do
	       local temp = measure[m1]
               temp [x]=""
           end
       end 
   else
      for index =1, #instances do
          local instance = instances [index]
          local path_triple = M.client:lrange("path_triple:"..M.parameter[1]..":"..instance, 0, -1)
          local pattern = getPattern(path_triple)
          local var_pair= M.client:hget("measure:"..M.parameter[1],pattern)

          var_pair = loadstring("return "..var_pair)()
          for insx = 1, #path_triple - 1 do
              local arrow =  M.getArrow(path_triple[insx])
              for m1, v1 in pairs (measure) do   
	         if v1[arrow[3]] then
                     local ixstr= tostring(insx)
                     for x, _ in pairs(var_pair[ixstr]) do
	                 local tmp= measure[m1]
                         tmp[x]=""
                     end 
                 end		
              end
          end
      end
   end

   return M.serialize_graph(measure,nil), measure

end
function M.comparation_measure(pattern,measure_pair_pattern)
    local var_pair= M.client:hget("measure:"..M.parameter[1],pattern)
    var_pair = loadstring("return "..var_pair)()
    local value_sub =  getMonocount(var_pair)
    local value_super=  getMonocount(measure_pair_pattern)
    if value_sub and value_super and ( value_sub <= value_super) then
       return true	
    end
    return false
end
function M.getInstanceVariablePattern()
     return uniq_node
end

function M.getInstanceVariableSimplePattern(key_pattern)
     local number_instances = compute_measure(key_pattern)
     return M.serialize_graph(uniq_node,nil), number_instances
end

function M.init()
   local start_node = M.client:get("START_NODE:"..M.parameter[1])
   local end_node = M.client:get("END_NODE:"..M.parameter[1])
   local start_value = M.client:get(start_node)
   local end_value = M.client:get(end_node)
   M.start_value=start_value
   M.end_value=end_value
end

return M


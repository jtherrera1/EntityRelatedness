#!/usr/bin/env lua5.2

local M = {
        _COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
        _DESCRIPTION = "Módulo SPARQL",
}



local endpointURI = "http://dbpedia-live.openlinksw.com/sparql"
local defaultGraphURI = "http://dbpedia.org"
local contentType = "application/sparql-results+json";
local prefixes = {
		rdf ="http://www.w3.org/1999/02/22-rdf-syntax-ns#",
		skos ="http://www.w3.org/2004/02/skos/core#",
		}
		
	
		
--[[
 * Return a set of queries to find relations between two objects.
 * 
 * @param object1 First object.
 * @param object2 Second object.
 * @param maxDistance The maximum distance up to which we want to search.
 * @param limit The maximum number of results per SPARQL query (=LIMIT).
 * @param ignoredObjects Objects which should not be part of the returned connections between the first and second object.
 * @param ignoredProperties Properties which should not be part of the returned connections between the first and second object.
 * @param avoidCycles Integer value which indicates whether we want to suppress cycles, 
 * 			0 = no cycle avoidance
 * 			1 = no intermediate object can be object1 or object2
 *  		2 = like 1 + an object can not occur more than once in a connection.
 * @return A two dimensional array of the form $array[$distance][$queries].
 --]]

function M.getQueries(object1, object2, maxDistance, limit, ignoredObjects, ignoredProperties, avoidCycles)
   local queries = {}

   local options = { object1 = object1, 
            object2 = object2,
            limit = limit,
            ignoredObjects = ignoredObjects,
            ignoredProperties = ignoredProperties, 
            avoidCycles = avoidCycles,
           }
		
   for distance=1, maxDistance do
	-- get direct connection in both directions
        queries[distance]={}

	table.insert(queries[distance],  M.direct(object1, object2, distance, options))
        --************************ 
	table.insert(queries[distance] , M.direct(object2, object1, distance, options))
	--*************************
		
	--[[
	 * generates all possibilities for the distances
	 * 
	 * current
	 * distance 	a 	b
	 * 2			1	1
	 * 3			2	1
	 * 				1	2
	 * 4			3	1
	 * 				1	3
	 * 				2	2
	 * --]]
			
  	 for a=1, distance do 
	    for b=1, distance  do
		if a+b == distance then
			table.insert(queries[distance],M.connectedViaAMiddleObject(object1, object2,a, b, true,  options))
			table.insert(queries[distance],M.connectedViaAMiddleObject(object1, object2,a, b, false,  options))
			--echo $a.$b."\n"
		end
	   end 
         end
   end

 return queries
end



--[[
	 * Return a set of queries to find relations between two objects, 
	 * which are connected via a middle objects.
	 * $dist1 and $dist2 give the distance between the first and second object to the middle
	 * they have ti be greater that 1
	 * 
	 * Patterns:
	 * if $toObject is true then:
	 * PATTERN								DIST1	DIST2
	 * first-->?middle<--second 					  	1		1
	 * first-->?of1-->?middle<--second					2		1
	 * first-->?middle<--?os1<--second 					1		2
	 * first-->?of1-->middle<--?os1<--second				2		2
	 * first-->?of1-->?of2-->middle<--second				3		1
	 * 
	 * if $toObject is false then (reverse arrows)
	 * first<--?middle-->second 
	 * 
	 * the naming of the variables is "pf" and "of" because predicate from "f"irst object
	 * and "ps" and "os" from "s"econd object
	 * 
	 * @param first First object.
	 * @param second Second object.
	 * @param dist1 Distance of first object from middle
	 * @param dist2 Distance of second object from middle
	 * @param toObject Boolean reverses the direction of arrows.
	 * @param options All options like ignoredProperties, etc. are passed via this array (needed for filters)
	 * @return the SPARQL Query as a String
--]]
function M.connectedViaAMiddleObject(first, second, dist1, dist2, toObject, options)
	local properties ={}
	local vars = {}
	vars["pred"] ={}
	vars["obj"] = {}
        table.insert(vars["obj"], "?middle")
	
	local fs = "f"
	local tmpdist = dist1
	local twice = 0
	local coreQuery = ""
	local object = first
			
	-- to keep the code compact I used a loop
	-- subfunctions were not appropiate since information for filters is collected
	-- basically the first loop generates $first-pf1->of1-pf2->middle
	-- while the second generates $second -ps1->os1-pf2->middle
	while twice < 2  do
	   if tmpdist == 1 then
		coreQuery = coreQuery .. M.toPattern(M.uri(object), "?p"..fs.."1", "?middle", toObject)
		 table.insert(vars.pred,  "?p"..fs.."1")
	    else 
		coreQuery = coreQuery ..M.toPattern(M.uri(object), "?p"..fs.."1", "?o"..fs.."1", toObject)
                table.insert(vars.pred,  "?p"..fs.."1")
			
		for x=1, tmpdist-1 do
			local s = "?o"..fs..""..x;
			local p = '?p'..fs..""..(x+1) 
			table.insert(vars.obj,  s)
			table.insert(vars.pred, p)

			if (x+1)==tmpdist then
				coreQuery = coreQuery..M.toPattern(s , p , "?middle", toObject)
			else
				coreQuery = coreQuery..M.toPattern(s , p , "?o"..fs..""..(x+1), toObject)
                        end
                 end
            end --if
	    twice= twice+1
	    fs = "s"
	    tmpdist = dist2
	    object = second
      end --while  

      return  M.completeQuery(coreQuery, options, vars)
end 

--[[
 * Returns a query for getting a direct connection from $object1 to $object2.
--]]
function M.direct(object1, object2, distance, options) 
   local vars = {};
   vars["obj"] ={};
   vars["pred"] = {};
   if distance == 1 then
      retval =  M.uri(object1) .." ?pf1 ".. M.uri(object2)
      table.insert(vars["pred"],"?pf1")
      return M.completeQuery(retval,  options, vars)
			
    else 
      local query =M.uri(object1) .." ?pf1 ?of1 "..".\n"
      table.insert(vars["pred"], "?pf1")
      table.insert(vars['obj'], "?of1")
      for i = 1, distance-2  do	
      query=  query .."?of"..i.." ?pf"..(i+1).." ?of"..(i+1)..".\n"
      table.insert(vars["pred"], "?pf"..(i+1))
      table.insert(vars["obj"],  "?of"..(i+1))
      end
      query = query.."?of"..(distance-1).." ?pf"..distance.." "..M.uri(object2)
      table.insert(vars["pred"], "?pf"..distance)
      table.insert( vars["obj"], "?of"..(distance-1))
      return M.completeQuery(query, options, vars)
   end
	
end

--[[
 * Helper function to reverse the order 
 * --]]
function M.toPattern(s, p, o, toObject)
	if(toObject) then
		return s.." "..p.." "..o.." . \n"
	else 
		return o.." "..p.." "..s.." . \n"
	end
end

--[[
 * Takes the core of a SPARQL query and completes it (e.g. adds prefixes).
 * 
--]]
function M.completeQuery(coreQuery, options, vars) 
   local _completeQuery = ""

   for k,v in pairs(prefixes) do
	_completeQuery=_completeQuery .. "PREFIX "..k..": <"..v..">\n"
   end
   _completeQuery = _completeQuery.. "SELECT * WHERE {".."\n"
   _completeQuery = _completeQuery.. coreQuery.."\n"
   _completeQuery = _completeQuery.. M.generateFilter(options, vars).."\n"
   _completeQuery = _completeQuery.."} "
   local limit =""
   if options.limit ~=nil then	
    limit = "LIMIT "..options.limit
   end

    _completeQuery = _completeQuery .. limit

   --print(_completeQuery)
   return _completeQuery
end

--[[
 * assembles the filter according to the options given and the variables used
 * @param vars 
 * array(1) {
	["pred"]=>
		array(1) {
 		[0]=>string(4) "?pf1"
 		}
 	["obj"]=>
 		array(1) {
    		[0]=>string(4) "?of1"
  						}
	}
 *
--]]
function M.generateFilter(options, vars)
    --var_dump($vars)
    --	//die;
    --rint(#vars.pred)
    local filterterms = {}
    for k, v in pairs(vars.pred) do
	-- ignore properties
	if options.ignoredProperties ~=nil and #options.ignoredProperties>0 then
		for k1, v1 in pairs(options.ignoredProperties) do
			table.insert(filterterms,  v.." != "..M.uri(v1).." ")
	 	end
	end
		
    end

    for k,obj in pairs(vars.obj) do 
	-- ignore literals
	table.insert(filterterms,  "!isLiteral("..obj..")")
		-- ignore objects
	if options.ignoredObjects ~= nil and #options.ignoredObjects>0 then
		for k1, ignored in pairs(options.ignoredObjects) do
			 table.insert(filterterms,  obj.." != "..M.uri(ignored).." ")
		end
	end
			
       if options.avoidCycles ~= nil then 
	-- object variables should not be the same as object1 or object2
		if  options.avoidCycles > 0  then
			table.insert(filterterms,  obj.." != "..M.uri(options.object1).." ")
			table.insert(filterterms, obj.." != "..M.uri(options.object2).." ")
		end
	-- object variables should not be the same as any other objectvariables
		if options.avoidCycles  > 1 then
			for k, otherObj in pairs(vars.obj) do
				if obj ~= otherObj then
				table.insert(filterterms, obj.." != "..otherObj.." ")
				end
			
			end	
        	end
			
			
       end
  end
   
  if #filterterms > 0 then
  return "FILTER "..M.expandTerms(filterterms, "&&")..". "
  else
    return "  "
  end


end

--[[
	 * puts bracket around the (filterterms) and concatenates them with &&
	 * 
--]]
function M.expandTerms (terms, operator)--operator = "&&")
	local result=""
        local term=""
        
	for x=1, #terms  do
		result= result .."("..terms[x]..")"
                term = (x+1 == (#terms +1)) and "" or " "..operator.. " "
                result = result .. term
                result = result .. "\n"
	end
	return "("..result..")"
end

--[[
 * Takes a URI and formats it according to the prefix map.
 * This basically is a fire and forget function, punch in 
 * full uris, prefixed uris or anything and it will be fine
 * 
 * 1. if uri can be prefixed, prefixes it and returns
 * 2. checks whether uri is already prefixed and returns
 * 3. else it puts brackets around the <uri>
--]]
function M.uri(uri_value)
		
   for  k, v in pairs(prefixes) do
        local pattern = "^"..v
 
	if uri_value:find( v )~= nil then
		uri_value = uri_value:gsub(v, k..":");
 		return uri_value;
	end
   end
		
   for  k, v in pairs(prefixes) do
        if uri_value:find( k..":" )~= nil  then
                return uri_value;
        end
   end

				
  return "<"..uri_value..">"
			

end 

return M

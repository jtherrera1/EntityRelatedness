# Entity Relatedness Project

The Entity Relatedness Project (ERP) is an iniciative to find relations and paths between two entities in Knowlegde Databases.


## Dependencies

* Lua 5.2
* Lua Rocks 2.4.1
* lpeg 1.0.0-1
* lua-curl 0.3.5-1
* luaexpat 1.3.0-1
* luafilesystem 1.6.3-2
* luasocket 3.0rc1-2
* luasql-mysql 2.3.0-1 
* redis-lua 2.0.4-1
* uuid 0.2-1 
* wsapi 1.6.1-1
* GNU parallel 20161122
* Redis Server 3.2.6

# Architecture Configuration

## Ontology Class

At the data pre-processing layer, ERP builds an index (using Redis) over the DBpedia and Wikidata class hierarchy to help identify the immediate classes of an entity. We follow the approach published at [1]. The Onotlogy of DBpedia is available at [2] and the Wikidata Taxanomy is found at the link [3]. In a next version we available this data as a service.  

## Processing Data

The data extraction is executed throug HTTP requests. We used luasockets and luaexpat for this task. The generated jobs are executed in paralllel with GNU parallel [4]. The URIs indentified in an Entity Document is sored and encoded in a Local Redis Server.

## Finding Path

   
# References

[1] Herrera, J., Casanova, M.A., Nunes, B.P., Lopes, G.R., and Leme, L.A. DBpedia Profiler Tool: Profiling the Connectivity of Entity Pairs in DBpedia. Proc. 5th Workshop on Intelligent Exploration of Semantic Data (October 2016).
<br/>[2] DBpedia Ontology Ontology.http://downloads.dbpedia.org/2014/dbpedia_2014.owl.bz2
<br/>[3] Wikidata Taxonomy: https://tools.wmflabs.org/wikidata-exports/rdf/exports/20160801/wikidata-taxonomy.nt.gz
<br/>[4] GNU Parallel: https://www.gnu.org/software/parallel/



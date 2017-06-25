#!/bin/bash

export LUA_PATH="$LUA_PATH;?.lua;/EntityRelatedness/module/?.lua;/usr/local/share/lua/5.2/?.lua"

SEQ=$1
DATABASE=$2


redis-cli -a xxxxx -r 1 set "START_NODE:$SEQ" $3
redis-cli -a xxxxx -r 1 set "END_NODE:$SEQ" $4

lua configure_process.lua $SEQ $DATABASE | parallel -j0 lua process.lua 

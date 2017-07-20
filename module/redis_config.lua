#!/usr/bin/env lua5.2
local redis = require 'redis'

local M = {
        _COPYRIGHT = "Copyright (C) 2017 PUC-Rio",
        _DESCRIPTION = "Módulo Redis",
}

local params = {
    host = '127.0.0.1',
    port = 6379,
    auth = 'xxxxxxx'
 }

function M.getRedis(port)
 	if port then
		params.port = port
	end
	local client = redis.connect(params)
	client:auth(params.auth)
	return client
end

return M


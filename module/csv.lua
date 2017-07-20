#!/usr/bin/env lua5.2

------------------------------------------------------------------------------
-- Módulo de leitura de arquivo no formato CSV (comma separated values).
--
-- @release $Id: csv.lua,v 1.21 2013/08/16 00:57:46 tomas Exp $
------------------------------------------------------------------------------

local assert, error, select, tostring, type = assert, error, select, tostring, type
local fopen = require"io".open
local lpeg = require"lpeg"
local gsub = require"string".gsub
local tconcat = require"table".concat

local M = {
	_COPYRIGHT = "Copyright (C) 2009-2017 PUC-Rio",
	_DESCRIPTION = "Módulo de leitura de arquivo no formato CSV",
	_VERSION = "Sintra CSV "..("$Id: csv.lua,v 1.21 2013/08/16 00:57:46 tomas Exp $"):match(",v (%S+)"),
}

------------------------------------------------------------------------------
-- @param d Tabela com os seguintes campos:
-- @param d.arquivo String com o nome do arquivo.
-- @param d.texto String com o conteúdo a ser interpretado.
-- @param d.aspas String com o caracter usado para delimitar strings (default=").
-- @param d.separador String com o caracter usado para separar campos (default=;).
-- @param d.ignorar_cabecalho Flag indicando se a primeira linha do arquivo deve
--		ser ignorada (default=false).
-- @param d.tem_cabecalho Flag indicado se a primeira linha do arquivo contém os
--		nomes das colunas (default=true).
-- @param d.colunas Tabela (array) com os nomes das colunas (default = nil).
-- @param d.saida Função callback que recebe (a cada vez) uma tabela duplamente
--		indexada (números e nomes de colunas) com o conteúdo de uma linha.
-- @param d.so_nomes Flag indicando se a tabela passada para a callback deve conter
--		apenas chaves indicadas na primeira linha (isso só vale quando
--		o cabeçalho não for ignorado) (default=false).
-- @return Tabela com o conteúdo do arquivo organizado em vetores de vetores.

function M.carrega(d)
	assert (d, "Falta indicar o nome do arquivo de dados!")
	if type(d) == "string" then
		d = { arquivo = d }
	end
	assert (d.arquivo or d.texto, "Falta indicar o nome do arquivo de dados (d.arquivo) ou passar o texto a ser interpretado (d.texto)!")
	-- Trata os parâmetros, atribuindo valores default --
	local tem_cabecalho = d.tem_cabecalho -- default == true
	if tem_cabecalho == nil then
		tem_cabecalho = not d.ignorar_cabecalho
	end
	local so_nomes = d.so_nomes
	if so_nomes == nil then
		so_nomes = false
	elseif so_nomes == true then
		assert (d.colunas or tem_cabecalho, "Se so_nomes==true então (tem_cabecalho==true OU ignorar_cabecalho==true OU colunas)")
	end
	local aspas = d.aspas or '"'
	local separador = d.separador or ','

	-- Função de tratamento de erro --
	local function erro_na_linha (s, i)
		--local msg = s:sub(i, s:find('\n',i+1))
		--error((d.prefixo_erro or '').."Erro na linha [["..msg.."]]\n"..i, 2)
              return nil
	end

	-- Monta os padrões para captura --
	local Caracter = lpeg.P(1)
	local Caracter_exceto_aspas = Caracter - aspas
	local Duas_aspas = lpeg.P(aspas..aspas)
	local Aspas_dentro_aspas = Duas_aspas / aspas -- substitui duas aspas por aspas
	local Campo = aspas * lpeg.Cs((Caracter_exceto_aspas + Aspas_dentro_aspas)^0) * aspas
	            + lpeg.C((1 - lpeg.S(separador..'\n'..aspas))^0)

	local Fim_linha = lpeg.P'\n'
	local Linha = (Campo * (separador * Campo)^0 * Fim_linha)

	local checa_num_colunas -- função que verifica a quantidade de colunas de cada linha
	local guarda_colunas -- função que trata o cabeçalho, definida adiante
	local Cabecalho = Linha / function (...)
			return guarda_colunas (checa_num_colunas (...))
		end

	local monta_linha -- função que monta a linha, definida adiante
	local Registro = Linha / function (...)
			return monta_linha (checa_num_colunas (...))
		end
	local Fim_arquivo = lpeg.P(-1)
	local Arquivo -- Padrão que processa um arquivo CSV
	if not tem_cabecalho then
		-- Agrupa todas as linhas em um vetor
		Arquivo = (lpeg.Ct(Registro^1) + erro_na_linha) * (Fim_arquivo + erro_na_linha)
	else
		-- Trata a primeira linha de forma especial (cabeçalho)
		-- Agrupa as outras linhas em um vetor
		Arquivo = (Cabecalho + erro_na_linha) * (lpeg.Ct(Registro^0) + erro_na_linha) * (Fim_arquivo + erro_na_linha)
	end

	-- Ações semânticas --
        local numberfilas =0
	do
		local total
		local i = 0
		checa_num_colunas = function (...) -- definindo função declarada acima
			i = i+1
			local n = select('#', ...)
                   
			if not total then total = n end
		        --assert (total == n, (d.prefixo_erro or '').."A linha "..i.." possui "..n.." colunas, mas a primeira possui "..total)
                        if total~=n then
                          return nil
                        end
                    
			return ...
		end
	end

	local colunas, nc -- Nomes das colunas (e quantidade de colunas)
	if d.colunas then
		colunas = d.colunas
		nc = #colunas
	else
		guarda_colunas = function (...) -- definindo função declarada acima
			nc = select('#', ...)
			colunas = { checa_num_colunas (...) }

			-- checa se há colunas repetidas
			for i = 1, nc-1 do
				for j = i+1, nc do
					assert (colunas[i] ~= colunas[j], (d.prefixo_erro or '').."As colunas "..i.." e "..j.." tem o mesmo nome [["..colunas[i].."]] !")
				end
			end
		end
	end

	-- Cria a função que monta a linha com os dados obtidos --
	if not tem_cabecalho and not d.colunas then
		-- só índices numéricos
		monta_linha = function (...) -- definindo função declarada acima
                        if ... ~= nil then
                           numberfilas = numberfilas + 1
                        end
			return { ... }
		end
	else -- existem nomes de colunas
               
		if so_nomes then
			-- só índices alfanuméricos => ignorar_cabecalho == false AND so_nomes == true
			monta_linha = function (...) -- definindo função declarada acima
				local linha = {}
                                --if #colunas ==0 then
                                --    return nil
                                --end

				for i = 1, nc do
					linha[colunas[i]] = select (i, ...)
				end
				return linha
			end
		else
			-- duplamente indexada => ignorar_cabecalho == false (default)
			monta_linha = function (...) -- definindo função declarada acima
                       
				local linha = { ... }
                                numberfilas = numberfilas + 1

                                if ... == nil then
                                    return nil
                                end
                                   
				for i = 1, nc do
					linha[colunas[i]] = linha[i]
				end
				return linha
			end
		end
	end

	if d.saida then
		local original = monta_linha
		monta_linha = function (...)
			local linha = original (...)
			return d.saida (linha) or linha
		end
	end

	local conteudo
	if d.arquivo then
		-- Lê o conteúdo do arquivo --
		local fh = assert(fopen(d.arquivo))
		conteudo = fh:read"*a"
		fh:close()
	elseif d.texto then
		conteudo = d.texto
	end

	-- padroniza quebras de linha e garante que há uma no final do arquivo
	conteudo = conteudo:gsub('\r\n', '\n')
	if conteudo:sub(-1) ~= '\n' then
		conteudo = conteudo..'\n'
	end

	-- Processa o arquivo --
	local dados, err = Arquivo:match(conteudo)
       
        if dados then
            if #dados ~= numberfilas then
               dados = nil
            end
        end

	return dados, dados and colunas or err
end

------------------------------------------------------------------------------

function M.codifica_valor (val, aspas)
	aspas = aspas or '"'
	return aspas..(gsub (val, aspas, aspas..aspas))..aspas
end

------------------------------------------------------------------------------

function M.codifica_linha (a, d)
	local aspas = (d and d.aspas) or '"'
	local sep = (d and d.separador) or ';'
	local res = {}
	if d and d.cabecalho then
		for i = 1, #d.cabecalho do
			local col = d.cabecalho[i]
			res[i] = M.codifica_valor (a[col], aspas)
		end
	else
		for i = 1, #a do
			res[i] = M.codifica_valor (a[i], aspas)
		end
	end
	return tconcat (res, sep)
end

------------------------------------------------------------------------------

function M.codifica_sequencia (linhas, d)
	local res = {}
	for i = 1, #linhas do
		res[i] = M.codifica_linha (linhas[i], d)
	end
	return tconcat (res, '\n')
end

------------------------------------------------------------------------------
return M

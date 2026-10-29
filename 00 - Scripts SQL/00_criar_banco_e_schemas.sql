-- =====================================================================
-- Trabalho Final - Tema 3: Multas Ambientais no Brasil (Ibama)
-- 00_criar_banco_e_schemas.sql
--
-- Cria o banco de dados (se ainda não existir) e os schemas da
-- arquitetura medalhão: sch_bronze, sch_silver e sch_gold (vazios).
--
-- SGBD: PostgreSQL — executar com o psql:
--   psql -h <host> -p <porta> -U <usuario> -d postgres -f 00_criar_banco_e_schemas.sql
--
-- Windows (PowerShell), quando o psql não está no PATH:
--   1) Descobrir onde está o psql.exe:
--      Get-ChildItem "C:\Program Files" -Filter psql.exe -Recurse -ErrorAction SilentlyContinue | Select-Object FullName
--   2) Rodar com o caminho completo (o & é obrigatório antes de caminho
--      entre aspas; troque 16 pela versão instalada):
--      & "C:\Program Files\PostgreSQL\16\bin\psql.exe" -h localhost -p 5432 -U postgres -d postgres -f 00_criar_banco_e_schemas.sql
--   O psql pede a senha do usuário postgres (definida na instalação).
--
-- Para usar outro nome de banco, acrescente:  -v db_name=outro_nome
--
-- O script é idempotente: pode rodar várias vezes sem dar erro.
-- =====================================================================

\set ON_ERROR_STOP on

-- Este arquivo está salvo em UTF-8. No Windows o psql assume a codificação
-- do console (WIN1252), o que estragaria a acentuação dos comentários.
\encoding UTF8

-- Nome padrão do banco (sobrescrito por -v db_name=... na linha de comando)
\if :{?db_name}
\else
    \set db_name dw_multas_ambientais
\endif


-- 1. BANCO DE DADOS ---------------------------------------------------
-- O PostgreSQL não aceita CREATE DATABASE IF NOT EXISTS nem permite
-- CREATE DATABASE dentro de um bloco DO. Por isso a consulta abaixo só
-- gera o comando quando o banco não existe, e o \gexec o executa.
-- UTF8 garante a acentuação dos dados do Ibama (nomes, municípios etc.).
SELECT format(
           'CREATE DATABASE %I WITH ENCODING ''UTF8'' TEMPLATE template0',
           :'db_name'
       )
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_database
    WHERE datname = :'db_name'
)\gexec

-- Passa a trabalhar dentro do banco do projeto
\connect :db_name
\encoding UTF8

COMMENT ON DATABASE :"db_name" IS
    'Trabalho Final - Tema 3: Multas Ambientais no Brasil (dados abertos do Ibama)';


-- 2. SCHEMAS DA ARQUITETURA MEDALHÃO ----------------------------------
CREATE SCHEMA IF NOT EXISTS sch_bronze;
CREATE SCHEMA IF NOT EXISTS sch_silver;
CREATE SCHEMA IF NOT EXISTS sch_gold;

COMMENT ON SCHEMA sch_bronze IS
    'Camada bronze: dados brutos do Ibama, carregados como vieram da fonte';
COMMENT ON SCHEMA sch_silver IS
    'Camada silver: dados limpos, tipados e padronizados';
COMMENT ON SCHEMA sch_gold IS
    'Camada gold: modelo dimensional (esquema estrela) pronto para análise';


-- 3. CONFERÊNCIA ------------------------------------------------------
SELECT n.nspname                                AS schema,
       obj_description(n.oid, 'pg_namespace')   AS descricao
FROM pg_namespace n
WHERE n.nspname IN ('sch_bronze', 'sch_silver', 'sch_gold')
ORDER BY CASE n.nspname
             WHEN 'sch_bronze' THEN 1
             WHEN 'sch_silver' THEN 2
             WHEN 'sch_gold'   THEN 3
         END;
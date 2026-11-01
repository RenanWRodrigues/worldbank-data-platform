#!/usr/bin/env bash
# Cria os usuários somente leitura com senhas do ambiente — nenhuma senha fica em SQL versionado.
# Roda antes do 01_init.sql (ordem alfabética), só na primeira inicialização (volume vazio).
# Sem `set -u` e sem `exit`: o entrypoint do Postgres pode fazer `source` deste arquivo.
set -eo pipefail

if [ -z "${POWERBI_PASSWORD:-}" ] || [ -z "${API_RO_PASSWORD:-}" ]; then
  echo "00_users.sh: POWERBI_PASSWORD e API_RO_PASSWORD são obrigatórias" >&2
  false
fi

# :'var' faz o psql citar o valor como literal SQL (sem risco com aspas na senha)
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  -v powerbi_pw="$POWERBI_PASSWORD" -v api_ro_pw="$API_RO_PASSWORD" <<'SQL'
CREATE ROLE powerbi LOGIN PASSWORD :'powerbi_pw';
CREATE ROLE api_ro LOGIN PASSWORD :'api_ro_pw';
-- Defesa extra: mesmo com GRANT errado no futuro, as sessões desses usuários não escrevem
ALTER ROLE powerbi SET default_transaction_read_only = on;
ALTER ROLE api_ro SET default_transaction_read_only = on;
SQL

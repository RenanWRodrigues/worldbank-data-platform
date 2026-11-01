-- =====================================================================================================
-- World Bank Data Platform — validação dos KPIs do Power BI (Fase 14)
-- Uma consulta por KPI de powerbi/medidas.dax (mesmo código Knn). Somente SELECT.
-- Resultados esperados de projetos (K13–K21b, K28) conferidos em 01/11/2026, depois da ADR 0009 e da correção do
-- grande setor da Projects API.
-- Rodar:  make psql   (ou: docker compose exec -T postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < powerbi/validacao_kpis.sql)
--
-- Regras espelhadas do DAX:
--   * dw.vw_country_year já exclui agregados (is_aggregate), inclusive FCS (agregado só do Kaggle, marcado
--     no silver por KAGGLE_ONLY_AGGREGATES).
--   * Ano de referência = último ano em que os 4 indicadores têm ≥ 180 países (K00) → 2024.
--   * % entre países: média ponderada pela população, só linhas com os dois valores.
--   * Projetos: período 2000–2017 é o único com histórico completo (Kaggle); ver ressalvas.
--
-- Resultados colados abaixo de cada consulta: execução de 29/10/2026 (dw carregado em 29/10/2026 21:03 UTC).
-- As consultas do bloco "Ao vivo" dependem de now(): o resultado muda a cada execução.
-- =====================================================================================================


-- K00 · Ano de referência e último ano com cobertura ≥ 180 países por indicador -----------------------
WITH cov AS (
    SELECT year,
           count(population)             AS pop,
           count(gdp_usd)                AS gdp,
           count(rural_population_pct)   AS rural,
           count(electricity_access_pct) AS elec
    FROM dw.vw_country_year
    GROUP BY year
)
SELECT max(year) FILTER (WHERE least(pop, gdp, rural, elec) >= 180) AS ano_referencia,
       max(year) FILTER (WHERE pop   >= 180) AS ultimo_ano_pop,
       max(year) FILTER (WHERE gdp   >= 180) AS ultimo_ano_pib,
       max(year) FILTER (WHERE rural >= 180) AS ultimo_ano_rural,
       max(year) FILTER (WHERE elec  >= 180) AS ultimo_ano_eletricidade
FROM cov;
--  ano_referencia | ultimo_ano_pop | ultimo_ano_pib | ultimo_ano_rural | ultimo_ano_eletricidade
-- ----------------+----------------+----------------+------------------+-------------------------
--            2024 |           2025 |           2025 |             2025 |                    2024
-- (PIB 2025 cobre só 186 países — faltam p.ex. Emirados Árabes (US$ 552 bi em 2024), Líbano, Afeganistão;
--  eletricidade não tem 2025. Por isso o ano comum é 2024.)


-- =====================================================================================================
-- Visão Global (mundo, 2024)
-- =====================================================================================================

-- K01 · População ------------------------------------------------------------------------------------
SELECT sum(population) / 1e9 AS populacao_bi, count(population) AS paises
FROM dw.vw_country_year
WHERE year = 2024;
--  populacao_bi | paises
-- --------------+--------
--   8.117484624 |    217

-- K02 · População — crescimento a.a. em 10 anos (2014 → 2024) ----------------------------------------
SELECT (power(sum(population) FILTER (WHERE year = 2024)
              / sum(population) FILTER (WHERE year = 2014), 1.0 / 10) - 1) * 100 AS crescimento_aa_pct
FROM dw.vw_country_year
WHERE year IN (2014, 2024);
--  crescimento_aa_pct
-- --------------------
--  1.0254792660742362

-- K03 · PIB (US$ correntes) e variação vs ano anterior -----------------------------------------------
SELECT sum(gdp_usd) FILTER (WHERE year = 2024) / 1e12 AS pib_tri_2024,
       sum(gdp_usd) FILTER (WHERE year = 2023) / 1e12 AS pib_tri_2023,
       (sum(gdp_usd) FILTER (WHERE year = 2024) / sum(gdp_usd) FILTER (WHERE year = 2023) - 1) * 100 AS var_pct,
       count(gdp_usd) FILTER (WHERE year = 2024) AS paises_2024
FROM dw.vw_country_year
WHERE year IN (2023, 2024);
--     pib_tri_2024    |    pib_tri_2023    |      var_pct      | paises_2024
-- --------------------+--------------------+-------------------+-------------
--  110.62075037259542 | 106.37457514885348 | 3.991720030655932 |         200

-- K04 · PIB per capita ponderado (Σ PIB / Σ população, países com os dois valores) ------------------
SELECT year, sum(gdp_usd) / sum(population) AS pib_per_capita_usd
FROM dw.vw_country_year
WHERE year IN (2023, 2024)
  AND gdp_usd IS NOT NULL AND population IS NOT NULL
GROUP BY year ORDER BY year;
--  year | pib_per_capita_usd
-- ------+--------------------
--  2023 | 13424.855148292136
--  2024 | 13830.256366412013      → var. +3,0%

-- K05 · Acesso à eletricidade (ponderado) · K06 Pessoas sem eletricidade · K08 Países com acesso universal
SELECT year,
       sum(population * electricity_access_pct) / sum(population)  AS eletricidade_pct,
       sum(population * (100 - electricity_access_pct) / 100) / 1e6 AS sem_eletricidade_mi,
       count(*) FILTER (WHERE electricity_access_pct >= 99.5)       AS paises_acesso_universal,
       count(*)                                                     AS paises
FROM dw.vw_country_year
WHERE year IN (2014, 2024)
  AND population IS NOT NULL AND electricity_access_pct IS NOT NULL
GROUP BY year ORDER BY year;
--  year | eletricidade_pct  | sem_eletricidade_mi | paises_acesso_universal | paises
-- ------+-------------------+---------------------+-------------------------+--------
--  2014 | 86.20194894966686 |  1011.4091077972739 |                     108 |    216
--  2024 | 91.92648438536804 |   655.2338927159996 |                     132 |    215
--  → +5,7 p.p. em 10 anos; −356 mi de pessoas sem eletricidade em 10 anos

-- K07 · População rural (% ponderado) -----------------------------------------------------------------
SELECT year, sum(population * rural_population_pct) / sum(population) AS rural_pct
FROM dw.vw_country_year
WHERE year IN (2014, 2024)
  AND population IS NOT NULL AND rural_population_pct IS NOT NULL
GROUP BY year ORDER BY year;
--  year |     rural_pct
-- ------+-------------------
--  2014 | 46.17355246288777
--  2024 | 42.49595739659921      → −3,7 p.p. em 10 anos

-- K09 · Top 10 países por PIB (2024) -----------------------------------------------------------------
SELECT country_name, gdp_usd / 1e12 AS pib_tri
FROM dw.vw_country_year
WHERE year = 2024 AND gdp_usd IS NOT NULL
ORDER BY gdp_usd DESC LIMIT 10;
--     country_name    |     pib_tri
-- --------------------+------------------
--  United States      |        29.298013
--  China              |  18.729668435848
--  Germany            | 4.68559257780469
--  Japan              | 4.19000818835857
--  India              | 3.76081347050086
--  United Kingdom     | 3.69553951353415
--  France             | 3.16044262246508
--  Italy              | 2.38343556245812
--  Canada             | 2.27007618968346
--  Russian Federation | 2.18646226881308

-- K10 · PIB per capita ponderado por região (2024) ----------------------------------------------------
SELECT region, sum(gdp_usd) / sum(population) / 1e3 AS pib_per_capita_mil_usd
FROM dw.vw_country_year
WHERE year = 2024 AND gdp_usd IS NOT NULL AND population IS NOT NULL
GROUP BY region ORDER BY 2 DESC;
--                       region                       | pib_per_capita_mil_usd
-- ---------------------------------------------------+------------------------
--  North America                                     |        82.808120496409
--  Europe & Central Asia                             |      31.61489027598115
--  East Asia & Pacific                               |     13.414944413040203
--  Latin America & Caribbean                         |     10.623944727218687
--  Middle East, North Africa, Afghanistan & Pakistan |      6.783375799665117
--  South Asia                                        |     2.6018220952936555
--  Sub-Saharan Africa                                |      1.552538334375404


-- =====================================================================================================
-- Energia e Desenvolvimento
-- =====================================================================================================

-- K11 · Acesso à eletricidade por região, 2000 → 2024 (ponderado) -----------------------------------
SELECT region,
       sum(population * electricity_access_pct) FILTER (WHERE year = 2000)
         / sum(population) FILTER (WHERE year = 2000) AS eletricidade_2000,
       sum(population * electricity_access_pct) FILTER (WHERE year = 2024)
         / sum(population) FILTER (WHERE year = 2024) AS eletricidade_2024
FROM dw.vw_country_year
WHERE year IN (2000, 2024)
  AND population IS NOT NULL AND electricity_access_pct IS NOT NULL
GROUP BY region ORDER BY 3 DESC;
--                       region                       | eletricidade_2000  | eletricidade_2024
-- ---------------------------------------------------+--------------------+-------------------
--  North America                                     |                100 |               100
--  Europe & Central Asia                             |  99.10405342998405 | 99.99885747137296
--  South Asia                                        |  56.76642467640919 | 99.82464249020511
--  East Asia & Pacific                               |  91.12339499749527 | 98.55979482380343
--  Latin America & Caribbean                         |  91.73608642775663 |  98.0459111591331
--  Middle East, North Africa, Afghanistan & Pakistan |  82.56991390428878 | 96.77368847222864
--  Sub-Saharan Africa                                | 25.330378132521556 | 55.11035425590005

-- K12 · Onde estão as pessoas sem eletricidade (top 8, 2024) -----------------------------------------
SELECT country_name, population * (100 - electricity_access_pct) / 100 / 1e6 AS sem_eletricidade_mi
FROM dw.vw_country_year
WHERE year = 2024
  AND population IS NOT NULL AND electricity_access_pct IS NOT NULL
ORDER BY 2 DESC LIMIT 8;
--    country_name   | sem_eletricidade_mi
-- ------------------+---------------------
--  Nigeria          |         87.25480425
--  Congo, Dem. Rep. |        84.689105375
--  Ethiopia         |        57.313938878
--  Tanzania         |        32.634634732
--  Uganda           |        22.356746124
--  Mozambique       |        21.852644346
--  Niger            |  21.274508244000003
--  Madagascar       |  18.379849699999998


-- =====================================================================================================
-- Projetos do Banco Mundial (fluxo; total_amount_usd = IBRD + IDA; doações à parte)
-- =====================================================================================================

-- K13 · Cartões: projetos, valor comprometido, valor médio (só total > 0), doações, ativos, encerrados ---
SELECT periodo,
       count(*)                                                                         AS projetos,
       round(sum(total_amount_usd) / 1e9, 1)                                            AS valor_bi,
       round(sum(total_amount_usd) / nullif(count(*) FILTER (WHERE total_amount_usd > 0), 0) / 1e6, 1)
                                                                                        AS medio_mi,
       round(sum(grant_amount_usd) / 1e9, 1)                                            AS doacoes_bi,
       count(*) FILTER (WHERE status = 'Active')                                        AS ativos,
       round(100.0 * count(*) FILTER (WHERE status = 'Active') / count(*), 1)           AS ativos_pct,
       round(100.0 * count(*) FILTER (WHERE status = 'Closed') / count(*), 1)           AS encerrados_pct
FROM (SELECT *, 'todos os anos' AS periodo FROM dw.fact_project
      UNION ALL SELECT *, '2000-2017' FROM dw.fact_project WHERE approval_year BETWEEN 2000 AND 2017
      UNION ALL SELECT *, '2024'      FROM dw.fact_project WHERE approval_year = 2024) t
GROUP BY periodo ORDER BY periodo;
--     periodo    | projetos | valor_bi | medio_mi | doacoes_bi | ativos | ativos_pct | encerrados_pct
-- ---------------+----------+----------+----------+------------+--------+------------+----------------
--  2000-2017     |     8430 |    589.1 |    106.8 |       28.5 |   1600 |       19.0 |           81.0
--  2024          |      344 |     63.6 |    209.8 |        0.2 |    344 |      100.0 |            0.0
--  todos os anos |    10569 |    956.7 |    126.7 |       67.4 |   3659 |       34.6 |           65.4

-- K14 · Valor comprometido por ano de aprovação --------------------------------------------------------
SELECT approval_year, count(*) AS projetos, round(sum(total_amount_usd) / 1e9, 2) AS valor_bi
FROM dw.fact_project
WHERE approval_year >= 2000
GROUP BY approval_year ORDER BY approval_year;
--  approval_year | projetos | valor_bi
-- ---------------+----------+----------
--           2000 |      315 |    17.19
--           2001 |      337 |    18.91
--           2002 |      310 |    18.26
--           2003 |      353 |    18.15
--           2004 |      433 |    24.08
--           2005 |      415 |    22.07
--           2006 |      442 |    21.16
--           2007 |      532 |    24.08
--           2008 |      549 |    33.97
--           2009 |      561 |    53.95   ← pico (resposta à crise de 2008–09)
--           2010 |      649 |    50.75
--           2011 |      572 |    38.03
--           2012 |      465 |    38.59
--           2013 |      481 |    32.18
--           2014 |      574 |    48.46
--           2015 |      462 |    45.88
--           2016 |      440 |    37.91
--           2017 |      540 |    45.51
--           2018 |      317 |    39.57   ← Kaggle até jun/2018 + ativos da API
--           2019 |      199 |    26.88   ← 2019–2024: só projetos AINDA ATIVOS (API live); encerrados faltam
--           2020 |      303 |    43.26
--           2021 |      272 |    48.10
--           2022 |      345 |    78.93
--           2023 |      323 |    67.04
--           2024 |      344 |    63.56
-- (antes de 2000: 36 projetos ainda ativos, US$ 0,24 bi; nenhum aprovado em 2025–2026 no dw)

-- K15 · Valor por região do Banco (fact_project.region_name), 2000–2017 -------------------------------
SELECT region_name,
       round(100 * sum(total_amount_usd) / sum(sum(total_amount_usd)) OVER (), 1) AS pct_valor,
       round(sum(total_amount_usd) / 1e9, 1)                                     AS valor_bi
FROM dw.fact_project
WHERE approval_year BETWEEN 2000 AND 2017
GROUP BY region_name ORDER BY 2 DESC;
--          region_name          | pct_valor | valor_bi
-- ------------------------------+-----------+----------
--  Latin America and Caribbean  |      20.3 |    119.7
--  Africa                       |      19.8 |    116.4
--  South Asia                   |      18.6 |    109.3
--  Europe and Central Asia      |      16.5 |     97.1
--  East Asia and Pacific        |      15.4 |     91.0
--  Middle East and North Africa |       6.9 |     40.4
--  Eastern and Southern Africa  |       1.7 |     10.2
--  Western and Central Africa   |       0.9 |      5.0
--  Other                        |       0.0 |      0.0
-- ("Eastern and Southern Africa" e "Western and Central Africa" são a divisão da África usada pela API desde
--  2020; aparecem também em 2000–2017 porque o estado da API vence o do replay — ADR 0009)

-- K16 · Valor por setor principal, 2000–2017 ------------------------------------------------------------
SELECT major_sector, count(*) AS projetos, round(sum(total_amount_usd) / 1e9, 1) AS valor_bi
FROM dw.fact_project
WHERE approval_year BETWEEN 2000 AND 2017
GROUP BY major_sector ORDER BY 3 DESC LIMIT 8;
--               major_sector              | projetos | valor_bi
-- ----------------------------------------+----------+----------
--  Public Administration                  |     1768 |    134.5
--  Transportation                         |      802 |     96.8
--  Energy and Extractives                 |      964 |     75.1
--  Water, Sanitation and Waste Management |      793 |     54.2
--  Social Protection                      |      679 |     43.4
--  Agriculture, Fishing and Forestry      |     1151 |     41.9
--  Education                              |      680 |     41.7
--  Financial Sector                       |      458 |     39.8

-- K17 · Instrumento de financiamento (% dos projetos), 2000–2017 ---------------------------------------
SELECT lending_instrument, round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct_projetos
FROM dw.fact_project
WHERE approval_year BETWEEN 2000 AND 2017
GROUP BY lending_instrument ORDER BY 2 DESC LIMIT 6;
--       lending_instrument      | pct_projetos
-- ------------------------------+--------------
--  Investment Project Financing |         30.5
--  Specific Investment Loan     |         29.7
--  Technical Assistance Loan    |         12.1
--  Development Policy Lending   |          9.3
--  Emergency Recovery Loan      |          5.2
--  Adaptable Program Loan       |          4.7

-- K18 · Países com maior valor comprometido, 2000–2017 ----------------------------------------------
SELECT c.country_name, count(*) AS projetos, round(sum(p.total_amount_usd) / 1e9, 1) AS valor_bi
FROM dw.fact_project p
JOIN dw.dim_country c USING (country_code)
WHERE p.approval_year BETWEEN 2000 AND 2017
GROUP BY c.country_name ORDER BY 3 DESC LIMIT 6;
--  country_name | projetos | valor_bi
-- --------------+----------+----------
--  India        |      288 |     57.1
--  Brazil       |      227 |     32.8
--  China        |      292 |     27.0
--  Indonesia    |      230 |     26.1
--  Turkiye      |       73 |     24.8
--  Mexico       |      107 |     23.7

-- K18b · Projetos sem país (regionais/multipaís; aparecem como "(Em branco)" em visuais por país) ------
SELECT count(*) AS projetos_sem_pais, round(sum(total_amount_usd) / 1e9, 1) AS valor_bi
FROM dw.fact_project
WHERE country_code IS NULL;
--  projetos_sem_pais | valor_bi
-- -------------------+----------
--                656 |     37.6

-- K19 · Cartão "Valor comprometido BM" da Visão Global: aprovados no ano de referência (2024) -------
SELECT count(*) AS projetos, round(sum(total_amount_usd) / 1e9, 2) AS valor_bi
FROM dw.fact_project
WHERE approval_year = 2024;
--  projetos | valor_bi
-- ----------+----------
--       344 |    63.56   (só projetos ativos — ver ressalva de 2019–2024)


-- =====================================================================================================
-- Perfil do País — Brasil (BRA)
-- =====================================================================================================

-- K20 · Indicadores do Brasil (cartões no ano de referência 2024; 2014 e 2023 para as variações) --------
SELECT year, population / 1e6 AS pop_mi, gdp_usd / 1e12 AS pib_tri, gdp_per_capita_usd,
       electricity_access_pct, rural_population_pct
FROM dw.vw_country_year
WHERE country_code = 'BRA' AND year IN (2000, 2014, 2023, 2024, 2025)
ORDER BY year;
--  year |   pop_mi   |      pib_tri       | gdp_per_capita_usd | electricity_access_pct | rural_population_pct
-- ------+------------+--------------------+--------------------+------------------------+----------------------
--  2000 | 174.018282 |  0.655448231983527 | 3766.5481146603147 |                   94.4 |     18.8515634576194
--  2014 | 200.085127 | 2.4560437271988502 | 12274.993968936282 |                   99.7 |     14.5668632101832
--  2023 | 211.140729 |   2.19113176568463 | 10377.589279255684 |                   99.8 |     12.3840289764237
--  2024 | 211.998573 |   2.18582161068931 | 10310.548697369346 |                   99.8 |     12.1041450466854
--  2025 | 212.812405 | 2.2799200924921297 | 10713.285686951049 |                        |     11.8227003512102
--  → 2024: pop. 212,0 mi (+0,58% a.a. em 10 anos); PIB US$ 2,19 tri (−0,2% vs 2023); PIB per capita
--    US$ 10,3 mil (−0,6%); eletricidade 99,8% (+0,1 p.p. em 10 anos; +5,4 p.p. desde 2000); rural 12,1%

-- K20b · Posição do Brasil no ranking de PIB (2024) ----------------------------------------------------
SELECT posicao
FROM (SELECT country_code, rank() OVER (ORDER BY gdp_usd DESC) AS posicao
      FROM dw.vw_country_year
      WHERE year = 2024 AND gdp_usd IS NOT NULL) r
WHERE country_code = 'BRA';
--  posicao
-- ---------
--       11

-- K21 · Projetos do Banco Mundial no Brasil ---------------------------------------------------------------
SELECT 'todos os anos' AS periodo, count(*) AS projetos, round(sum(total_amount_usd) / 1e9, 2) AS valor_bi,
       count(*) FILTER (WHERE status = 'Active') AS ativos
FROM dw.fact_project WHERE country_code = 'BRA'
UNION ALL
SELECT '2000-2017', count(*), round(sum(total_amount_usd) / 1e9, 2), count(*) FILTER (WHERE status = 'Active')
FROM dw.fact_project WHERE country_code = 'BRA' AND approval_year BETWEEN 2000 AND 2017
UNION ALL
SELECT '2024', count(*), round(sum(total_amount_usd) / 1e9, 2), count(*) FILTER (WHERE status = 'Active')
FROM dw.fact_project WHERE country_code = 'BRA' AND approval_year = 2024;
--     periodo    | projetos | valor_bi | ativos
-- ---------------+----------+----------+--------
--  todos os anos |      263 |    37.97 |     65
--  2000-2017     |      227 |    32.80 |     29
--  2024          |       10 |     1.62 |     10

-- K21b · Projetos no Brasil por setor (todos os anos) ------------------------------------------------------
SELECT major_sector, count(*) AS projetos, round(sum(total_amount_usd) / 1e9, 2) AS valor_bi
FROM dw.fact_project
WHERE country_code = 'BRA'
GROUP BY major_sector ORDER BY 3 DESC LIMIT 6;
--               major_sector              | projetos | valor_bi
-- ----------------------------------------+----------+----------
--  Public Administration                  |       55 |    13.77
--  Transportation                         |       36 |     7.01
--  Water, Sanitation and Waste Management |       29 |     3.47
--  Social Protection                      |       17 |     3.18
--                                         |       11 |     1.92   ← sem setor (444 projetos no total)
--  Health                                 |       13 |     1.69


-- =====================================================================================================
-- Ao vivo (rt, DirectQuery) — janelas pela chegada ao rt (consumed_at / created_at), em UTC
-- K22–K27 são uma foto de 29/10/2026, durante o replay do Kaggle: mudam a cada execução (não são referência fixa)
-- =====================================================================================================

-- K22 · Eventos na última hora, hora anterior, 24 h, aprovações 24 h · K25 último evento ------------------
SELECT now()                                                                         AS consultado_em,
       count(*) FILTER (WHERE consumed_at >= now() - interval '1 hour')              AS eventos_ultima_hora,
       count(*) FILTER (WHERE consumed_at >= now() - interval '2 hours'
                          AND consumed_at <  now() - interval '1 hour')              AS eventos_hora_anterior,
       count(*) FILTER (WHERE consumed_at >= now() - interval '24 hours')            AS eventos_24h,
       count(*) FILTER (WHERE consumed_at >= now() - interval '24 hours'
                          AND event_type = 'PROJECT_APPROVED')                       AS aprovacoes_24h,
       max(consumed_at)                                                              AS ultimo_evento,
       floor(extract(epoch FROM now() - max(consumed_at)) / 60)                      AS minutos_desde_ultimo
FROM rt.project_events;
--          consultado_em         | eventos_ultima_hora | eventos_hora_anterior | eventos_24h | aprovacoes_24h |         ultimo_evento         | minutos_desde_ultimo
-- -------------------------------+---------------------+-----------------------+-------------+----------------+-------------------------------+----------------------
--  2026-10-29 21:32:20.468812+00 |                   0 |                     0 |       15879 |           8686 | 2026-10-29 19:07:58.413915+00 |                  144
-- (o producer está em modo live sem mudanças na API; as 24 h contêm o replay do Kaggle das 19:00–19:08 UTC.
--  Rode `make replay` para a página "mexer".)

-- K23 · Valor aprovado (24 h): compromisso atual dos projetos com aprovação recebida nas últimas 24 h ------
SELECT round(sum(l.total_commitment_usd) / 1e9, 2)                               AS valor_aprovado_24h_bi,
       count(*)                                                                  AS projetos,
       round(sum(l.total_commitment_usd) / nullif(count(*), 0) / 1e6, 1)         AS medio_mi
FROM rt.projects_latest l
WHERE l.project_id IN (SELECT project_id
                       FROM rt.project_events
                       WHERE event_type = 'PROJECT_APPROVED'
                         AND consumed_at >= now() - interval '24 hours');
--  valor_aprovado_24h_bi | projetos | medio_mi
-- -----------------------+----------+----------
--                 613.07 |     8686 |     70.6

-- K24 · Alertas (24 h) ------------------------------------------------------------------------------------
SELECT count(*) FILTER (WHERE created_at >= now() - interval '24 hours') AS alertas_24h,
       count(*)                                                         AS alertas_total
FROM rt.alerts;
--  alertas_24h | alertas_total
-- -------------+---------------
--          219 |           387

-- K26 · Eventos por tipo e fonte (24 h) ------------------------------------------------------------------
SELECT event_type, source, count(*) AS eventos
FROM rt.project_events
WHERE consumed_at >= now() - interval '24 hours'
GROUP BY event_type, source ORDER BY event_type, source;
--     event_type    |    source     | eventos
-- ------------------+---------------+---------
--  PROJECT_APPROVED | kaggle-replay |    8686
--  PROJECT_CLOSED   | kaggle-replay |    7193

-- K27 · Últimos projetos (tabela da página Ao vivo) --------------------------------------------------------
SELECT left(project_name, 50) AS projeto, country_name, last_event_type,
       round(total_commitment_usd / 1e6, 1) AS usd_mi, updated_at
FROM rt.projects_latest
ORDER BY updated_at DESC, last_produced_at DESC
LIMIT 6;
--                       projeto                       |     country_name     | last_event_type | usd_mi |          updated_at
-- ----------------------------------------------------+----------------------+-----------------+--------+-------------------------------
--  PPIAF: GLOBAL: Water Dialogues on Delivery of Wate | World                | PROJECT_CLOSED  |    0.0 | 2026-10-29 20:52:14.049807+00
--  IRF Road Safety Training                           | World                | PROJECT_CLOSED  |    0.0 | 2026-10-29 20:52:14.049807+00
--  GVEP International - Grant TF058079                | World                | PROJECT_CLOSED  |    0.0 | 2026-10-29 20:51:43.674035+00
--  Joint proposal Special Project to support the Rein | Republic of Angola   | PROJECT_CLOSED  |    0.0 | 2026-10-29 20:50:57.556015+00
--  GRANT FOR INTEGRATION OF INTERNALLY DISPLACED PEOP | Republic of Colombia | PROJECT_CLOSED  |    0.0 | 2026-10-29 20:50:57.556015+00
--  Statistical Capacity Building for Poverty Reductio | Southern Africa      | PROJECT_CLOSED  |    0.0 | 2026-10-29 20:50:50.093122+00
-- (updated_at 20:50–20:52 é POSTERIOR ao último consumed_at de rt.project_events (19:07): um segundo replay
--  reaplicou projetos cujo APPROVED e CLOSED têm o mesmo event_time. Corrigido pela ADR 0008: reentregas não
--  disputam mais o estado.)

-- K28 · Consistência rt × dw (depois do micro-lote os totais devem coincidir) -----------------------------
SELECT (SELECT count(*) FROM rt.projects_latest)                          AS rt_projetos,
       (SELECT count(*) FROM rt.projects_latest WHERE status = 'Active')  AS rt_ativos,
       (SELECT count(*) FROM dw.fact_project)                             AS dw_projetos,
       (SELECT count(*) FROM dw.fact_project WHERE status = 'Active')     AS dw_ativos;
--  rt_projetos | rt_ativos | dw_projetos | dw_ativos
-- -------------+-----------+-------------+-----------
--        10569 |      3659 |       10569 |      3659


-- =====================================================================================================
-- Checagem: FCS (agregado só do Kaggle) não pode aparecer como país
-- =====================================================================================================
SELECT country_code, is_aggregate,
       (SELECT count(*) FROM dw.vw_country_year WHERE country_code = 'FCS') AS linhas_na_view
FROM dw.dim_country WHERE country_code = 'FCS';
--  country_code | is_aggregate | linhas_na_view
-- --------------+--------------+----------------
--  FCS          | t            |              0

# Guia do relatório Power BI — World Bank Data Platform

Artefatos desta pasta (texto, versionados):

| arquivo | conteúdo |
|---|---|
| `medidas.dax` | todas as medidas (seções 0–4), com formato sugerido e código `Knn` do KPI |
| `validacao_kpis.sql` | uma consulta por KPI (`Knn`), com o resultado real colado embaixo |
| `tema_worldbank.json` | tema de cores e tipografia |
| `GUIA_POWERBI.md` | este guia |
| `relatorio/WorldBank.pbip` | o relatório salvo como projeto (`WorldBank.Report/` + `WorldBank.SemanticModel/`) |
| `wordlbank.svg` | arte do fundo da página inicial (Home), sem o ícone "Ao vivo" (seção 12); o relatório usa uma cópia em `WorldBank.Report/StaticResources/` |

O relatório em si é montado no **Power BI Desktop** e salvo como **`.pbip`** (nunca `.pbix`). O layout de
cada página está descrito na seção 7; os números reais dos cartões estão em `validacao_kpis.sql`.

---

## 1. Perguntas de negócio e KPIs

Ano de referência dos cartões: **2024** (regra na seção 6.1). Valores de 29/10/2026.

| # | pergunta | KPIs (medidas) | página · visual | valor real (mundo / Brasil) |
|---|---|---|---|---|
| 1 | Qual o tamanho da população e da economia mundial e quanto cresceram? | `População` (K01), `População — cresc. a.a. em 10 anos` (K02), `PIB (US$)` e `PIB — var. % vs ano anterior` (K03) | Visão Global · cartões + linhas de PIB e população | 8,12 bi de pessoas (+1,03% a.a. 2014→2024); PIB US$ 110,6 tri (+4,0% vs 2023) |
| 2 | Quão desigual é a renda entre regiões e países? | `PIB per capita (US$)` ponderado (K04), por região (K10), top 10 PIB (K09) | Visão Global · cartão, barras por região, top 10 | US$ 13,8 mil (+3,0%); América do Norte 82,8 mil × África Subsaariana 1,6 mil (53×) |
| 3 | Quanto avançou o acesso à eletricidade e quantas pessoas ainda estão sem? | `Acesso à eletricidade (%)` e `— var. p.p. em 10 anos` (K05), `Pessoas sem eletricidade` e `— var. em 10 anos` (K06), `Países com acesso universal` (K08) | Energia · cartões | 91,9% (+5,7 p.p. em 10 anos); 655 mi sem eletricidade (−356 mi); 132 países ≥ 99,5% |
| 4 | Onde estão as pessoas sem eletricidade e quais regiões ficaram para trás? | K11 (região 2000 → 2024), K12 (top 8 países) | Energia · halteres por região + barras top 8 | África Subsaariana 25,3% → 55,1%; Nigéria 87 mi e RD Congo 85 mi sem eletricidade |
| 5 | Urbanização anda junto com acesso à energia? | `População rural (%)` e `— var. p.p. em 10 anos` (K07), dispersão rural × eletricidade (bolha = população) | Energia · cartão + dispersão | 42,5% rural (−3,7 p.p. em 10 anos) |
| 6 | Quanto o Banco Mundial comprometeu, quando, e quanto segue ativo? | `Projetos aprovados`, `Valor comprometido (US$)`, `Valor médio por projeto (US$)`, `Projetos ativos`, `Projetos encerrados — % do total` (K13), série por ano (K14) | Projetos · cartões + colunas por ano | 2000–2017: 8.430 projetos, US$ 589 bi, média US$ 107 mi, 81,0% encerrados; pico em 2009 (US$ 54 bi) |
| 7 | Para onde vai o financiamento (região, setor, instrumento, países)? | `% do valor comprometido` (K15), valor por setor (K16), `% dos projetos` por instrumento (K17), top países (K18) | Projetos · barras e tabela | América Latina e Caribe 20,3%, África 19,8% (+ 2,6% nas subdivisões da API); Administração pública US$ 134,5 bi; Índia US$ 57 bi, Brasil US$ 32,8 bi (2º) |
| 8 | Como está o Brasil e o que acontece agora no portfólio do Banco? | Perfil do País (K20–K21) e Ao vivo (K22–K28) | Perfil do País (drill-through) · Ao vivo | Brasil 2024: 212,0 mi hab., PIB US$ 2,19 tri (11º), US$ 10,3 mil per capita, 99,8% eletricidade, 12,1% rural; 263 projetos, US$ 38,0 bi. Ao vivo: ver K22–K28 (dependem do momento) |

---

## 2. Conexão

1. Suba a stack (`make up` ou `make up-stream`) e confirme o Postgres em `localhost:5432` (ou na porta de
   `POSTGRES_HOST_PORT` do seu `.env`).
2. Power BI Desktop → **Obter dados → Banco de dados PostgreSQL**.
   * Servidor: `localhost:5432` (ou `localhost:<POSTGRES_HOST_PORT>`). O `relatorio/WorldBank.pbip` versionado
     aponta para `127.0.0.1:5433` (máquina com outro Postgres na 5432); com a porta padrão, troque em
     **Transformar dados → Configurações da fonte de dados → Alterar origem**.
   * Banco de dados: o valor de `POSTGRES_DB` do seu `.env` (no `.env.example` o padrão é `worldbank`)
3. Credenciais → **Banco de dados**: usuário `powerbi`, senha = `POWERBI_PASSWORD` do seu `.env`
   (preencha você mesmo; não versione nem cole a senha em lugar nenhum).
   O usuário `powerbi` é **somente leitura** nos schemas `dw` e `rt` (não enxerga `ops`).
4. Se aparecer aviso de criptografia (SSL), aceite a conexão sem criptografia — é local.

## 3. Modelo composto (Importar + DirectQuery)

Faça **duas** conexões (duas vezes "Obter dados"), uma por modo:

| modo | tabelas do navegador | renomear no Power Query para |
|---|---|---|
| **Importar** | `dw.dim_country`, `dw.dim_year`, `dw.dim_indicator`, `dw.fact_indicator`, `dw.fact_project`, `dw.agg_country_year`, `dw.load_audit` | `dim_country`, `dim_year`, `dim_indicator`, `fact_indicator`, `fact_project`, `agg_country_year`, `load_audit` |
| **DirectQuery** | `rt.project_events`, `rt.projects_latest`, `rt.alerts` | `rt_project_events`, `rt_projects_latest`, `rt_alerts` |

* `dw.vw_country_year` **não** precisa ser importada: `agg_country_year` + `dim_country` já cobrem tudo
  (a view é usada só em `validacao_kpis.sql`).
* Tipos: `year`/`approval_year` → Número inteiro; colunas `NUMERIC` (valores em US$) → Número decimal fixo
  ou decimal; `timestamptz` → Data/Hora (ver seção 9 sobre fuso).
* Não aplique transformações nas tabelas DirectQuery além de renomear (quebraria a dobragem de consulta).
* **Conexões simultâneas: 3** (Arquivo → Opções → Arquivo atual → DirectQuery → "Máximo de conexões por fonte de
  dados"; no modelo, `dataSourceDefaultMaxConnections`). Com o padrão 10, a página Ao vivo dispara ~20 consultas
  em paralelo e, com o Docker em 4 GB, o Postgres (`mem_limit` 512m) caiu em 30/10/2026 e todos os visuais da
  página deram erro durante a recuperação. Para usar o relatório numa máquina de 8 GB, suba a stack com
  `make up-stream` (sem Airflow) ou pare o Airflow: `docker compose stop airflow-apiserver airflow-scheduler
  airflow-dag-processor airflow-triggerer`.
* `fact_project.country_code` tem nulos (656 projetos regionais/multipaís) — mantenha.
* Atualização: `dw` muda na carga semanal e nos micro-lotes (a cada 30 min); clique **Atualizar** para
  reimportar. As tabelas `rt` são consultadas a cada interação/atualização de página.

## 4. Relacionamentos

Todos **1 → \*** (muitos para um), **direção única** (dimensão filtra fato), ativos:

| de (lado 1) | para (lado \*) | observação |
|---|---|---|
| `dim_country[country_code]` | `agg_country_year[country_code]` | |
| `dim_country[country_code]` | `fact_indicator[country_code]` | |
| `dim_country[country_code]` | `fact_project[country_code]` | FK opcional (nulos ficam no membro "(Em branco)") |
| `dim_year[year]` | `agg_country_year[year]` | |
| `dim_year[year]` | `fact_indicator[year]` | |
| `dim_year[year]` | `fact_project[approval_year]` | fluxo: projeto pertence ao ano de aprovação |
| `dim_indicator[indicator_code]` | `fact_indicator[indicator_code]` | |

* `load_audit` fica solta (só alimenta `Última carga do DW`).
* **As tabelas `rt_*` ficam sem relacionamento**, entre si e com o `dw`. Entre si, o conector PostgreSQL não
  dobra a junção por `project_id` (texto) em DirectQuery — o modelo compara texto sem diferenciar maiúsculas e
  o Postgres não traduz esse comparador — e todo visual que atravessa a relação falha com "A avaliação local de
  Table.Join ou Table.NestedJoin com comparadores de igualdade de chave não tem suporte" (visto em 30/10/2026).
  Onde é preciso levar o filtro de um evento ao projeto (`Valor aprovado (24h)`), a medida usa `TREATAS` com os
  `project_id` (vira `WHERE … IN`). Por isso a página Ao vivo não tem fatia de região. Com o `dw`, seriam
  relacionamentos limitados entre ilhas. Não use filtro bidirecional no modelo.
* Marque `dim_year` como tabela de datas **não** é necessário (o grão é ano inteiro, sem inteligência de tempo).

## 5. Medidas e tema

* Crie uma tabela vazia `_Medidas` (Página Inicial → Inserir dados → OK) e crie nela as medidas de
  `medidas.dax`. Forma mais rápida: **Exibição TMDL** (ou DAX query view / Tabular Editor) colando as
  definições; o texto antes de `=` é o nome da medida.
* Aplique o formato indicado no comentário de cada medida. Os percentuais de indicadores
  (`Acesso à eletricidade (%)`, `População rural (%)`) já vêm em 0–100: formate como número `0,0` com o
  sufixo "%" no rótulo, **não** com o formato "%" (que multiplicaria por 100). Medidas de participação
  (`% do valor comprometido`, `Projetos ativos — % do total`…) estão em 0–1 e usam o formato `%`.
* Tema: **Exibir → Temas → Procurar temas** → `powerbi/tema_worldbank.json` (azul `#1F4E79`, fundo da página
  `#F4F6F9`, cartões brancos sem borda, títulos Segoe UI Semibold 14, valores de cartão 28).

## 6. Regras das medidas

### 6.1 Ano de referência ("último ano")

* Com **um** ano no contexto (fatia de ano, eixo de gráfico, ponto de linha), a medida usa esse ano.
* Com vários anos ou nenhum filtro, usa o **último ano do contexto em que os 4 indicadores têm ≥ 180 países
  não agregados com valor** (`[Cobertura mínima de países]`, mesmo limiar do `coverage_check` e do
  `data_quality`). Hoje isso dá **2024**: 2025 não tem eletricidade e o PIB 2025 só cobre 186 países
  (faltam, entre outros, Emirados Árabes Unidos — US$ 552 bi em 2024 —, Líbano e Afeganistão).
* A cobertura ignora filtros de país/região (é sempre do universo inteiro), então a fatia "Região" não muda
  o ano escolhido.
* Se o usuário escolher 2025 numa fatia de ano único, os cartões mostram 2025 como está (eletricidade em
  branco, PIB com 186 países) — é o comportamento esperado.

### 6.2 Estoque × fluxo, agregados, ponderação

* Estoque (população, PIB, %): valor de um ano; nunca somar anos.
* Fluxo (projetos e valores): soma no período de aprovação.
* Indicadores: `KEEPFILTERS(dim_country[is_aggregate] = FALSE())` (FCS, agregado só do Kaggle, já vem como agregado).
* % entre países: média ponderada pela população, só nas linhas com os dois valores;
  PIB per capita = Σ PIB / Σ população dos países com os dois valores.
* Projetos: **sem** filtro de `dim_country` (os 656 regionais/multipaís têm `country_code` nulo).
  `Valor comprometido` = `total_amount_usd` (IBRD + IDA); doações estão em `Doações (US$)`.
  `Valor médio por projeto` divide só pelos projetos com compromisso > 0 (≈ 29% são só doação/AT, com total 0).

## 7. Layout das páginas

Tela 16:9 (1280 × 720). Em todas: faixa de cabeçalho azul (`#1F4E79`) com o título
"Indicadores de Desenvolvimento · World Bank", subtítulo com `[Rótulo do contexto]` e, à direita,
"Atualizado em" + `[Última carga do DW]`; abaixo, faixa de fatias; depois 4–5 cartões; depois 2–4 visuais.

### 7.1 Visão Global
* **Fatias:** Ano (`dim_year[year]`, lista suspensa do mais recente ao mais antigo, **sem** "Seleção única" —
  com ela o Desktop obriga um item marcado e seleciona 1960 ao abrir; deixe sem seleção para usar o ano de
  referência), Região (`dim_country[region]`), Indicador de tendência (opcional: parâmetro de campo
  com `PIB (US$)` / `População`).
* **Cartões** (cartão novo, com linha de referência/ sparkline por `dim_year[year]`):
  `População` + `População — cresc. a.a. em 10 anos`; `PIB (US$)` + `PIB — var. % vs ano anterior`;
  `PIB per capita (US$)` + `PIB per capita — var. % vs ano anterior`; `Acesso à eletricidade (%)` +
  `Acesso à eletricidade — var. p.p. em 10 anos`; `Valor comprometido no ano de referência (US$)` com
  subtítulo `Projetos aprovados no ano de referência`.
* **Linhas (áreas):** `PIB (US$)` e `População` por `dim_year[year]` (filtro do visual: ano ≥ 1990 e
  ≤ ano de referência; a fatia de ano não deve filtrar estes dois — **Editar interações → Nenhum**).
* **Barras:** Top 10 `dim_country[country_name]` por `PIB (US$)` (filtro Top N = 10 pela própria medida).
* **Barras:** `PIB per capita (US$)` por `dim_country[region]`.
* Botão/ação de **drill-through** para "Perfil do País" a partir das barras do top 10.

### 7.2 Energia e Desenvolvimento
* **Fatias:** Ano, Região (como na Visão Global).
* **Cartões:** `Acesso à eletricidade (%)` ("média ponderada pela população"); `Pessoas sem eletricidade` +
  `Pessoas sem eletricidade — var. em 10 anos`; `População rural (%)` + `População rural — var. p.p. em 10 anos`;
  `Países com acesso universal` ("≥ 99,5% da população").
* **Dispersão:** detalhes `dim_country[country_name]`, X `População rural (%)`, Y `Acesso à eletricidade (%)`,
  tamanho `População`, legenda opcional `dim_country[region]`; rótulos de categoria ligados.
* **Halteres por região:** gráfico de linhas e colunas empilhadas ou de pontos com
  `Acesso à eletricidade em 2000 (%)` e `Acesso à eletricidade (%)` por `dim_country[region]`
  (marcadores sem linha; cinza para 2000, azul para o ano de referência).
* **Barras:** top 8 `dim_country[country_name]` por `Pessoas sem eletricidade`.

### 7.3 Projetos do Banco Mundial
* **Fatias:** Ano (intervalo `dim_year[year]`; **padrão sugerido 2000–2017**, único período com histórico
  completo — seção 10), Região (`fact_project[region_name]`, região do Banco, inclui projetos regionais),
  Status (`fact_project[status]`), Instrumento (`fact_project[lending_instrument]`).
* **Cartões:** `Projetos aprovados`; `Valor comprometido (US$)` (sparkline por ano); `Valor médio por projeto (US$)`;
  `Projetos ativos` + `Projetos ativos — % do total`; `Projetos encerrados — % do total`.
* **Colunas:** `Valor comprometido (US$)` por `dim_year[year]` (destaque 2008–2010 com formatação condicional).
* **Barras:** `% do valor comprometido` por `fact_project[region_name]`.
* **Barras:** `Valor comprometido (US$)` por `fact_project[major_sector]` (top 7).
* **Barras:** `% dos projetos` por `fact_project[lending_instrument]` (top 4; agrupe o resto em "Outros"
  com um grupo).
* **Tabela:** `dim_country[country_name]`, `Projetos aprovados`, `Valor comprometido (US$)` (barra de dados),
  top 6 por valor; filtro "country_name não está em branco" para esconder os regionais.

### 7.4 Perfil do País — drill-through
* Página de **detalhamento**: em "Detalhamento" (painel Visualizações) arraste `dim_country[country_name]`;
  mantenha "Manter todos os filtros" ligado. O botão "Voltar" é criado automaticamente (estilize como
  "← Voltar para Visão Global").
* Para abrir direto no Brasil durante a construção: filtro de página `country_name = Brazil`
  (remova depois).
* **Fatias:** Ano (intervalo, padrão 1990–ano de referência). Mostre País e Região em cartões de texto
  (`SELECTEDVALUE(dim_country[country_name])`, `dim_country[region]`).
* **Cartões:** `População`, `PIB (US$)` + var., `PIB per capita (US$)` + var., `Acesso à eletricidade (%)` +
  var., `Valor comprometido (US$)` com subtítulo `Projetos aprovados` (soma no intervalo) e
  `Posição no ranking de PIB`.
* **Área:** `PIB (US$)` por ano. **Linhas:** `Acesso à eletricidade (%)` e `População rural (%)` por ano.
* **Colunas:** `Valor comprometido (US$)` por `dim_year[year]`.
* **Tabela:** `fact_project[major_sector]`, `Projetos aprovados`, `Valor comprometido (US$)`.

### 7.5 Ao vivo — DirectQuery (só local)
Página **oculta** e sem botão na Home: só funciona no Desktop com a stack de pé (ver seção 12). No Desktop, abra
pela aba "Ao vivo" (páginas ocultas aparecem no modo de edição).
* **Fatias:** Fonte (`rt_project_events[source]`) e Tipo de evento (`rt_project_events[event_type]`). Sem fatia
  de região: ela está em `rt_projects_latest` e não filtraria os eventos (tabelas `rt_*` sem relacionamento,
  seção 4). À direita, cartão de texto com `[Status do stream]`.
* **Cartões:** `Eventos (última hora)` + `Eventos — var. % vs hora anterior`; `Aprovações (24h)`;
  `Valor aprovado (24h) (US$)` + `Valor médio aprovado (24h) (US$)`; `Alertas (24h)` ("aprovações ≥ US$ 500 mi");
  `Minutos desde o último evento` (**substitui "Lag dos consumidores"**: o usuário `powerbi` não lê
  `ops.kafka_lag`; o lag está em `GET /v1/pipeline/status` da API).
* **Colunas "Eventos por minuto":** eixo X = grupo de `rt_project_events[consumed_at]` com compartimento de
  1 minuto (botão direito → Novo grupo → Compartimento), Y = contagem de `event_id`; filtro do visual
  **Data relativa/Tempo relativo: nos últimos 60 minutos**. Se o Desktop não permitir o compartimento em
  DirectQuery, use gráfico de linhas com `consumed_at` contínuo.
* **Barras "Eventos por tipo":** `Eventos (24h)` por `rt_project_events[event_type]`. A medida fica **sem**
  `+ 0`: com ela, o visual falhava ao filtrar Tipo de evento ou Fonte (o DirectQuery gera junção por texto, que o
  conector PostgreSQL não dobra — o mesmo problema da seção 4). Sem eventos nas 24 h, o gráfico fica vazio.
* **Tabela "Últimos projetos":** `rt_projects_latest[project_name]`, `country_name`, `last_event_type`
  (ícone por formatação condicional), `total_commitment_usd`, `updated_at`; classificar por `updated_at`
  decrescente; filtro Top N = 10 por `MAX(updated_at)`.
* **Lista "Alertas":** tabela/cartão de várias linhas com `rt_alerts[message]` e `created_at`, ordenada por
  `created_at` decrescente.
* **Atualização automática da página:** selecione a página → Formatar página → **Atualização de página** →
  Ligado → Tipo **Intervalo fixo**, **30 segundos**. (No serviço, capacidade compartilhada/Pro tem intervalo
  mínimo de 30 min; Premium/Fabric permite segundos e o tipo **Detecção de alterações** com a medida
  `Último seq`.) Só funciona porque as tabelas `rt` estão em DirectQuery.
* Para ver a página "mexer": `make replay` (ou deixe `make live` rodando — no modo live só há eventos quando
  a Projects API muda).

## 8. Validar os cartões contra o SQL

1. `make psql` (ou o comando no topo de `validacao_kpis.sql`) e rode o arquivo inteiro ou o bloco do KPI.
2. Compare cada cartão com a linha do código `Knn` correspondente (o código está no comentário da medida):
   * Visão Global sem fatias de ano/região → K01–K05, K19 (ano 2024).
   * Energia → K05–K08, K11, K12. Projetos com Ano = 2000–2017 e demais fatias "Todas" → K13–K18.
   * Perfil do País no Brasil → K20, K20b (ano de referência) e K21 (todos os anos ou 2000–2017,
     conforme o intervalo da fatia).
   * Ao vivo → K22–K28, rodando o SQL **no mesmo minuto** (as janelas usam `now()`).
3. Tolerância: só arredondamento de exibição. Diferença maior costuma ser: fatia esquecida, formato "%"
   aplicado num valor 0–100, ou relacionamento com direção errada.
4. Se mudar uma medida, atualize a consulta e o resultado em `validacao_kpis.sql` na mesma alteração.

## 9. Fuso horário (página Ao vivo)

O Postgres grava `timestamptz` em UTC, mas o conector PostgreSQL do Desktop **converte para o horário local**
(confirmado em 30/10/2026: `max(consumed_at)` = 19:07 UTC aparece como 16:07 no Power BI, em UTC−3). Por isso as
medidas da seção 4 de `medidas.dax` comparam com `NOW()`, não com `UTCNOW()` — com `UTCNOW()` as janelas de
"última hora"/"24 h" ficavam 3 h deslocadas e `Minutos desde o último evento` somava 180 min. Ao validar contra
K22–K28, lembre que o SQL mostra UTC e o relatório, horário local. O filtro de tempo relativo dos visuais também
usa o relógio local, então funciona sem ajuste. Vale para o **Desktop**: publicado no serviço, `NOW()` passa a
ser UTC e as medidas voltariam a precisar de `UTCNOW()`.

## 10. Ressalvas dos dados (encontradas na validação)

* **FCS ("Fragile and conflict affected situations")** só existe no Kaggle (sem metadados da API). Até a Fase 15
  entrava como país (511 mi de pessoas em 2017, dados só até 2017) e derrubava a população "mundial" em 0,43 bi de
  2017 para 2018; agora o silver o marca como agregado (`KAGGLE_ONLY_AGGREGATES`) e o filtro `is_aggregate` basta.
* **Cobertura dos anos recentes:** 2025 tem população e % rural (217 países), PIB em 186 países e nenhuma
  eletricidade; 2024 é o ano de referência. Séries com a fatia em 2025 mostram um PIB mundial subestimado.
* **Histórico de projetos incompleto fora de 2000–2017:** o Kaggle cobre aprovações de jan/2000 a jun/2018; a
  Projects API (modo live) traz só projetos **ativos**. Logo, 2018 é parcial, 2019–2024 só têm projetos ainda
  ativos (100% "Active" em 2024), antes de 2000 há só 36 projetos ainda ativos e **não há aprovações de 2025–2026**
  no dw (a última é 20/12/2024). Comparações entre anos fora de 2000–2017 não medem o volume real.
* **Status do replay é inferido:** `PROJECT_CLOSED` é gerado quando `closing_date ≤ hoje`, sem saber de
  prorrogações reais. Desde a ADR 0009, o estado vindo da Projects API vence o do replay: projetos prorrogados que a
  API ainda vê como Active ficam Active (em 01/11/2026, 283 voltaram a Active). Sobram ~23 Active sem data de
  aprovação na API, que não geram evento (runbook, "Exceções conhecidas da auditoria").
* **Valores 0:** 3.021 projetos têm `total_amount_usd = 0` (2.858 deles só com doação); o valor médio os exclui.
* **Regiões do Banco mudaram:** `fact_project[region_name]` tem "Africa" (Kaggle) e "Eastern and Southern
  Africa"/"Western and Central Africa" (API, divisão de 2020). Como o estado da API vence o do replay (ADR 0009), os
  nomes novos aparecem também em projetos antigos ainda acompanhados pela API (aprovados desde 1987); "Other" agrupa 96 projetos com US$ 0. Região do Banco
  (`fact_project`) ≠ região WDI (`dim_country[region]`).
* **Projetos sem país:** 656 projetos (US$ 37,6 bi) regionais/multipaís têm `country_code` nulo; aparecem em
  "(Em branco)" nos visuais por país e somem quando a fatia de região WDI (`dim_country`) é usada.
* **444 projetos sem setor** (`major_sector` nulo).
* **Valores altos legítimos:** PIB per capita de Mônaco e Liechtenstein acima de US$ 200 mil é real.
* **rt.projects_latest.updated_at pode não refletir evento novo:** em 29/10, 6 projetos ficaram com `updated_at`
  20:50–20:52 UTC, depois do último `consumed_at` de `rt.project_events` (19:07). Um segundo replay reenviou
  eventos com o mesmo `event_id` e `produced_at` novo; `project_events` os ignorou, mas `projects_latest` os
  reaplicou porque, nesses projetos, `PROJECT_APPROVED` e `PROJECT_CLOSED` têm o mesmo `event_time` e o
  desempate é por `produced_at` (o APPROVED reenviado pode até regredir o status para Active até o CLOSED
  chegar). Estado final correto, mas a tabela "Últimos projetos" os mostra como recentes. **Corrigido pela
  ADR 0008:** só eventos gravados pela primeira vez em `rt.project_events` disputam o estado, então reentregas não
  reaplicam nem mexem no `updated_at`.
* **"Ao vivo" depende do momento:** em 29/10 21:32 UTC havia 0 eventos na última hora (modo live sem mudanças) e
  15.879 nas 24 h (todos do replay do Kaggle); `Valor aprovado (24h)` usa o compromisso **atual** do projeto
  em `rt_projects_latest` (o valor do evento só existe no `payload` jsonb).
* **Lag do Kafka** não está acessível ao usuário `powerbi` (schema `ops`); dar acesso exigiria migração e
  mudança no contrato de permissões.

## 11. Salvar e versionar

* **Arquivo → Salvar como → Projeto do Power BI (`.pbip`)** em `powerbi/relatorio/` (`powerbi/relatorio/WorldBank.pbip`). Se a
  opção não aparecer: Opções → Recursos de visualização → "Salvar projetos do Power BI Desktop (.pbip)".
* Versione as pastas `*.Report/` e `*.SemanticModel/`; o `.gitignore` já ignora `*.pbix`, `**/.pbi/localSettings.json`
  e `**/.pbi/cache.abf` (dados importados) em qualquer subpasta, e o pre-commit bloqueia `.pbix`.
* Credenciais **não** ficam no `.pbip` (ficam no cache do Desktop); quem abrir o projeto informa a própria senha.

## 12. Publicar no Power BI Service

* **Link público** ("Publicar na Web", sem login): https://app.powerbi.com/view?r=eyJrIjoiZGUzYmYwZWUtNjI4OS00MGM0LWI4MjEtYjIzNmNiM2ExNTgwIiwidCI6Ijk5MjYwYzhkLWRlMGMtNGQ4OS1iZjAxLWEyMTJkOTgzNmVkOSJ9 <!-- gitleaks:allow -->
  Qualquer pessoa com o link vê o relatório, então só publique dados públicos (é o caso: dados abertos do
  Banco Mundial). Depois de republicar, o link pode levar até cerca de 1 hora para mostrar a versão nova (cache
  do "Publicar na Web").
* **Ao vivo não funciona publicada.** As tabelas `rt_*` são DirectQuery para `127.0.0.1:5433`; no serviço quem
  consulta é um servidor da Microsoft, para o qual `127.0.0.1` é ele mesmo. Todo visual da página falha (visto em
  31/10/2026). Por isso a página fica **oculta** (`"visibility": "HiddenInViewMode"` em
  `pages/ao_vivo/page.json`) e a Home não tem ícone nem botão para ela. Não basta ocultar: botão de navegação abre
  página oculta.
* As 4 páginas importadas funcionam: são uma **foto** do `dw` no momento da publicação. Atualização agendada no
  serviço também falharia (mesma fonte local); para atualizar, clique em Atualizar no Desktop e publique de novo.
* No serviço, a fonte do modelo semântico aparece com erro de credenciais/gateway; é esperado e não afeta as
  páginas publicadas. Não configure atualização agendada.
* Alternativas descartadas (opção mais simples para portfólio): gateway de dados local em modo padrão (o pessoal
  não faz DirectQuery; exige o PC ligado com o Docker, sem SSL, e `UTCNOW()` nas medidas, porque `NOW()` no
  serviço é UTC — seção 9); Postgres na nuvem (muda a arquitetura local do projeto).

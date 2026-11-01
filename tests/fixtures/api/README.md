Respostas de exemplo das APIs do World Bank (formato v2), usadas nos testes do
`ingestion/worldbank_client.py` com uma sessão HTTP falsa. Valores ilustrativos;
a estrutura segue a documentação das APIs. Se a primeira extração real revelar
diferenças, atualize estes arquivos e os testes juntos.

`projects_page1.json` (P123456) segue o formato real dos setores da Projects API conferido em 01/11/2026:
sem `mjsector1Name`; `sector1` é o subsetor principal e `sector`/`mjsector` são listas paralelas de `{"Name"}`.

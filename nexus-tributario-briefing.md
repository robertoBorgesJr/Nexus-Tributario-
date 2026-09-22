# Nexus Tributário — briefing de continuidade


## O que é o projeto
Pipeline de dados fiscais que consolida três fontes brasileiras — **NF-e**
(nota fiscal eletrônica), **SPED Fiscal** (apuração de ICMS/IPI) e **EFD
Contribuições** (apuração de PIS/COFINS) — em um modelo analítico único,
para gerar insights sobre arrecadação de impostos e conciliação entre o que
foi emitido (NF-e) e o que foi declarado (SPED/EFD).

Nome do projeto: **Nexus Tributário** .

## Arquitetura decidida
- **Medallion architecture** (bronze / silver / gold)
- **Ingestão**: dados sintéticos de NF-e (XML, schema oficial modelo 55),
  SPED Fiscal e EFD Contribuições (texto pipe-delimited, registros `0000`,
  `C100`/`C170`, `E100`/`E110` para SPED; blocos `A`/`C`/`M` para EFD) — não
  há certificado digital disponível, então os dados são gerados
  sinteticamente com Faker/Jinja2 seguindo os layouts oficiais
- **Transformação**: dbt sobre Databricks, camadas staging → marts
- **Modelo dimensional**:
  - `fact_nfe_itens` (grão: item de nota)
  - `fact_apuracao_icms` (grão: período x UF, do SPED Fiscal)
  - `fact_apuracao_pis_cofins` (grão: período x empresa, da EFD)
  - `dim_empresa`, `dim_produto`, `dim_cfop`, `dim_periodo`, `dim_uf`
- **Insights-chave**: arrecadação por imposto/UF/período, carga tributária
  efetiva por CFOP, e **conciliação fiscal** (ICMS nas notas NF-e vs. ICMS
  apurado no SPED — divergências viram alerta)


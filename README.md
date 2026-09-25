# Nexus Tributário

Pipeline de dados fiscais que consolida três obrigações acessórias brasileiras — **NF-e**, **SPED Fiscal** e **EFD Contribuições** — em um modelo analítico único para análise de arrecadação de impostos e conciliação fiscal.

---

## Visão geral

O projeto implementa uma **Medallion Architecture** (bronze / silver / gold) sobre Azure Databricks com Unity Catalog. Os dados são gerados sinteticamente (não há certificado digital disponível), seguindo os layouts oficiais de cada obrigação, e processados por um pipeline dbt que entrega um modelo dimensional star schema.

**Fontes de dados:**

| Fonte | Registros | Impostos |
|---|---|---|
| NF-e modelo 55 | XML schema oficial | ICMS, IPI, PIS, COFINS |
| SPED Fiscal (EFD ICMS/IPI) | C100, C170, E110 | ICMS/IPI |
| EFD Contribuições | C100, C170, M200, M600 | PIS/COFINS |

**Dados sintéticos de referência:** 3 empresas emitentes (SP, RJ, MG), 4 clientes (SP, PR, RS, AM), 8 produtos de informática/eletrônicos, 6 meses de competência (Jul–Dez 2025), 30 NF-e por empresa por mês.

---

## Arquitetura

```
Geradores Python                          Dados locais / ADLS Gen2
(Faker + Jinja2)  ──► XMLs / TXTs ──►   ──────────────────────────
                                                    │
                                        ┌───────────▼───────────┐
                                        │     BRONZE (Delta)     │
                                        │  nfe_cabecalho         │
                                        │  nfe_itens             │
                                        │  sped_fiscal_c100/170  │
                                        │  sped_fiscal_e110      │
                                        │  efd_contrib_c100/170  │
                                        │  efd_contrib_m200/m600 │
                                        └───────────┬────────────┘
                                                    │  dbt (views)
                                        ┌───────────▼────────────┐
                                        │     SILVER (views)      │
                                        │  stg_nfe__itens         │
                                        │  stg_sped_fiscal__      │
                                        │    apuracao / itens     │
                                        │  stg_efd_contrib__      │
                                        │    apuracao / itens     │
                                        └───────────┬─────────────┘
                                                    │  dbt (tables)
                                        ┌───────────▼──────────────┐
                                        │      GOLD (tables)        │
                                        │  fact_nfe_itens           │
                                        │  fact_apuracao_icms       │
                                        │  fact_apuracao_pis_cofins │
                                        │  fact_conciliacao_produto │
                                        │  dim_empresa / produto /  │
                                        │    cfop / periodo / uf    │
                                        │  alert_conciliacao_icms   │
                                        └───────────────────────────┘
```

---

## Estrutura do repositório

```
nexus-tributario/
├── generators/                  # Geração de dados sintéticos
│   ├── config.py                # Empresas, produtos, clientes, alíquotas
│   ├── nfe_generator.py         # XMLs NF-e modelo 55
│   ├── sped_fiscal_generator.py # Arquivos SPED Fiscal (pipe-delimited)
│   ├── efd_contribuicoes_generator.py
│   └── run_all.py               # Executa todos os geradores
│
├── ingestao/                    # Ingestão para a camada bronze
│   ├── ingest_to_bronze.py      # PySpark: local (Parquet) ou Databricks (Delta)
│   └── upload_to_adls.py        # Envia arquivos brutos para ADLS Gen2
│
├── dbt/                         # Transformação (silver + gold)
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/             # Camada silver (views)
│   │   │   ├── _sources.yml
│   │   │   ├── stg_nfe__itens.sql
│   │   │   ├── stg_sped_fiscal__apuracao.sql
│   │   │   ├── stg_sped_fiscal__itens.sql       # C170 por produto × CFOP
│   │   │   ├── stg_efd_contrib__apuracao.sql
│   │   │   └── stg_efd_contrib__itens.sql        # C170 por produto
│   │   └── marts/               # Camada gold (tables)
│   │       ├── dimensions/      # dim_empresa, dim_produto, dim_cfop, dim_periodo, dim_uf
│   │       ├── facts/           # fact_nfe_itens, fact_apuracao_icms,
│   │       │                    # fact_apuracao_pis_cofins, fact_conciliacao_produto
│   │       └── alerts/          # alert_conciliacao_icms
│   └── packages.yml             # dbt-utils
│
├── infra/                       # Terraform (Azure + Databricks)
│   └── README.md
│
├── data/                        # Dados locais para desenvolvimento
│   └── raw/{nfe,sped_fiscal,efd_contribuicoes}/
│
├── requirements.txt             # Dependências principais
├── requirements-local.txt       # Para desenvolvimento local (PySpark)
├── requirements-databricks.txt  # Para produção (databricks-connect)
└── .env                         # Variáveis de ambiente (não versionado)
```

---

## Modelo dimensional (Gold)

### Fatos

| Modelo | Grão | Principais métricas |
|---|---|---|
| `fact_nfe_itens` | Item de NF-e | `v_prod`, `v_icms`, `v_pis`, `v_cofins`, `carga_tributaria_total_pct` |
| `fact_apuracao_icms` | Empresa × Período | `vl_icms_recolher`, `divergencia_icms`, `divergencia_pct` |
| `fact_apuracao_pis_cofins` | Empresa × Período | `vl_pis_devido`, `vl_cofins_devido`, `carga_pis_pct`, `carga_cofins_pct` |
| `fact_conciliacao_produto` | Empresa × Período × Produto | `div_icms`, `div_pis`, `div_cofins`, `*_pct`, `status_declaracao` |

### Dimensões

| Dimensão | Chave natural | Atributos notáveis |
|---|---|---|
| `dim_empresa` | `cnpj` | `uf`, `regime_tributario` (Simples / Lucro Real / Lucro Presumido) |
| `dim_produto` | `cod_produto` | `ncm`, `descricao`, `ucom` |
| `dim_cfop` | `cfop` | `natureza_operacao`, `tipo_operacao` (entrada/saída), `gera_credito_icms` |
| `dim_periodo` | `competencia` (yyyyMM) | `ano`, `mes`, `trimestre`, `nome_mes` |
| `dim_uf` | `uf` | `nome_estado`, `regiao` |

### Alerta

| Modelo | Filtro | Severidades |
|---|---|---|
| `alert_conciliacao_icms` | `divergencia_icms > R$50` ou `divergencia_pct > 0,5%` | BAIXO / MEDIO / ALTO / CRITICO |

---

## Qualidade de dados (Silver)

Todos os modelos staging aplicam as seguintes regras antes de entregar dados para a gold:

| Regra | Implementação |
|---|---|
| String vazia convertida a NULL | `try_cast(nullif(campo, '') as tipo)` |
| Valor numérico negativo bloqueado | `greatest(try_cast(...), 0.00)` |
| `v_prod` zero vira NULL (divisão segura) | `nullif(greatest(...), 0.00)` — denominador nos cálculos de carga |
| Chave NF-e com comprimento incorreto descartada | `WHERE length(chv_nfe) = 44` |
| Registros sem chave primária descartados | `WHERE chv_nfe / cnpj_empresa / cod_item IS NOT NULL` |
| Data de emissão inválida descartada | `WHERE dt_emissao IS NOT NULL` (pós `to_date`) |
| Documentos cancelados/denegados excluídos | `WHERE cod_sit = '00'` |
| Itens sem movimentação excluídos (SPED C170) | `WHERE ind_mov = '0'` |

Os modelos gold não precisam de guards adicionais — recebem dados já limpos e tipos já validados da silver.

---

## Pré-requisitos

- Python 3.11+
- Java 11+ (exigido pelo PySpark em modo local)
- dbt-databricks >= 1.8
- Databricks workspace com Unity Catalog (produção) **ou** PySpark local (desenvolvimento)

---

## Início rápido — desenvolvimento local

### 1. Ambiente Python

```powershell
python -m venv .venv-local
.venv-local\Scripts\Activate.ps1
pip install -r requirements-local.txt
```

### 2. Variáveis de ambiente

Crie um arquivo `.env` na raiz com:

```env
# Databricks (opcional em modo local)
DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
DATABRICKS_TOKEN=<pat>

# ADLS Gen2 (opcional em modo local)
AZURE_STORAGE_ACCOUNT=<conta>
AZURE_STORAGE_KEY=<chave>
```

### 3. Gerar dados sintéticos

```powershell
python generators/run_all.py
```

Saída em `data/raw/{nfe,sped_fiscal,efd_contribuicoes}/`.

### 4. Ingerir para bronze (local)

```powershell
python ingestao/ingest_to_bronze.py --modo local
```

Grava Parquet em `data/bronze/`.

### 5. Executar transformações dbt

Antes de cada sessão, carregue o token e aponte o dbt para o `profiles.yml` do projeto:

```powershell
# Extrai o token de ~/.databrickscfg para a sessão atual
$cfg = "$env:USERPROFILE\.databrickscfg"
$env:DATABRICKS_TOKEN = ((Get-Content $cfg | Select-String '^\s*token\s*=\s*(.+)').Matches[0].Groups[1].Value.Trim())

# Aponta o dbt para o profiles.yml do projeto (dentro de dbt/)
$env:DBT_PROFILES_DIR = "."
```

Depois rode normalmente:

```powershell
cd dbt
dbt deps          # instala dbt-utils
dbt debug         # confirma conexão antes de rodar
dbt run           # silver + gold
dbt test          # testes de qualidade
```

---

## Início rápido — produção (Databricks)

### 1. Provisionar infraestrutura

```powershell
cd infra
cp terraform.tfvars.example terraform.tfvars
# edite terraform.tfvars com assinatura, workspace e metastore
terraform init
terraform apply nexus.tfplan
```

Cria: ADLS Gen2, Access Connector, Unity Catalog com schemas `bronze`, `silver`, `gold`, `controle`.

### 2. Enviar dados brutos para ADLS

```powershell
python ingestao/upload_to_adls.py
```

### 3. Ingerir para bronze (Databricks)

```powershell
python ingestao/ingest_to_bronze.py --modo databricks
```

Usa Auto Loader com checkpoints incrementais.

### 4. Executar dbt no Databricks

Antes de cada sessão, carregue o token e aponte o dbt para o `profiles.yml` do projeto:

```powershell
# Extrai o token de ~/.databrickscfg para a sessão atual
$cfg = "$env:USERPROFILE\.databrickscfg"
$env:DATABRICKS_TOKEN = ((Get-Content $cfg | Select-String '^\s*token\s*=\s*(.+)').Matches[0].Groups[1].Value.Trim())

# Aponta o dbt para o profiles.yml do projeto (dentro de dbt/)
$env:DBT_PROFILES_DIR = "."
```

Depois execute:

```powershell
cd dbt
dbt deps
dbt debug              # confirma conexão
dbt run --target prod
dbt test --target prod
```

---

## Principais insights analíticos

- **Carga tributária efetiva por CFOP** — `fact_nfe_itens` agrupado por `cfop` e `competencia`
- **Arrecadação por UF e período** — `fact_apuracao_icms` com `dim_uf` e `dim_periodo`
- **Conciliação fiscal ICMS** — `alert_conciliacao_icms` aponta divergências entre o ICMS declarado no SPED e o ICMS das NF-e emitidas
- **Carga PIS/COFINS sobre receita bruta** — `fact_apuracao_pis_cofins.carga_pis_pct` e `carga_cofins_pct`
- **Drill-down por produto** — `fact_conciliacao_produto` mostra qual produto gerou divergência entre NF-e e SPED/EFD, com `status_declaracao` identificando produtos emitidos sem declaração correspondente

---

## Dependências principais

| Pacote | Versão mínima | Uso |
|---|---|---|
| `pyspark` | 3.5 | Ingestão local |
| `dbt-databricks` | 1.8 | Transformação |
| `Faker` | 26.0 | Geração de dados |
| `Jinja2` | 3.1 | Templates XML/texto |
| `azure-storage-blob` | 12.20 | Upload para ADLS |
| `python-dotenv` | 1.0 | Configuração via `.env` |

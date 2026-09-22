"""
Ingestão dos arquivos brutos para a camada bronze.

Modos de execução
─────────────────
--local   PySpark local (sem Databricks). Lê data/raw/, grava em data/bronze/ como Parquet.
          Use para desenvolvimento e validação de schema.

(padrão) Databricks Runtime no cluster. Lê do ADLS via Auto Loader (cloudFiles),
          grava em Unity Catalog como Delta. Requer cluster com Databricks Runtime ≥ 13.

Uso:
    python ingestao/ingest_to_bronze.py --local [--source nfe|sped|efd|all]
    python ingestao/ingest_to_bronze.py          [--source nfe|sped|efd|all]
"""
import argparse
import os
import sys
from pathlib import Path

# Alinha driver e worker ao pyspark instalado no venv, ignorando SPARK_HOME do sistema.
# Necessário quando há uma instalação separada de Spark (ex: C:\spark) com versão diferente.
import pyspark as _pyspark
os.environ["SPARK_HOME"]            = os.path.dirname(_pyspark.__file__)
os.environ["PYSPARK_PYTHON"]        = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, input_file_name, regexp_extract, split, udf,
)
from pyspark.sql.types import (
    StringType, StructField, StructType, ArrayType,
)

# ── Configuração ──────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = os.getenv("ADLS_STORAGE_ACCOUNT", "stnexustributarioprod")
FILESYSTEM      = os.getenv("ADLS_FILESYSTEM",       "nexus-data")
CATALOG         = os.getenv("DATABRICKS_CATALOG",    "nexus_tributario_prod")

ADLS_BASE = f"abfss://{FILESYSTEM}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
ADLS_LANDING = f"{ADLS_BASE}/landing"

LOCAL_RAW    = Path(__file__).parent.parent / "data" / "raw"
LOCAL_BRONZE = Path(__file__).parent.parent / "data" / "bronze"

# ── SparkSession ──────────────────────────────────────────────────────────────

def get_spark(local: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("nexus-tributario-ingestao-bronze")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
        .config("spark.python.worker.faulthandler.enabled", "true")
    )
    if local:
        builder = (
            builder
            .master("local[2]")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
        )
    return builder.getOrCreate()


# ── UDFs para parse de NF-e XML ───────────────────────────────────────────────

NFE_CABECALHO_SCHEMA = StructType([
    StructField("chv_nfe",           StringType()),
    StructField("cnpj_emitente",     StringType()),
    StructField("uf_emitente",       StringType()),
    StructField("cnpj_destinatario", StringType()),
    StructField("uf_destinatario",   StringType()),
    StructField("dt_emissao",        StringType()),
    StructField("serie",             StringType()),
    StructField("num_nf",            StringType()),
    StructField("crt",               StringType()),
])

NFE_ITEM_SCHEMA = StructType([
    StructField("chv_nfe",      StringType()),
    StructField("num_item",     StringType()),
    StructField("cod_produto",  StringType()),
    StructField("ncm",          StringType()),
    StructField("cfop",         StringType()),
    StructField("ucom",         StringType()),
    StructField("qtd",          StringType()),
    StructField("v_unit",       StringType()),
    StructField("v_prod",       StringType()),
    StructField("cst_icms",     StringType()),
    StructField("v_bc_icms",    StringType()),
    StructField("aliq_icms",    StringType()),
    StructField("v_icms",       StringType()),
    StructField("v_ipi",        StringType()),
    StructField("cst_pis",      StringType()),
    StructField("v_pis",        StringType()),
    StructField("cst_cofins",   StringType()),
    StructField("v_cofins",     StringType()),
    StructField("v_nf",         StringType()),
])


@udf(returnType=NFE_CABECALHO_SCHEMA)
def parse_nfe_cabecalho(xml_text: str):
    import xml.etree.ElementTree as ET
    NS = "http://www.portalfiscal.inf.br/nfe"
    def t(name): return f"{{{NS}}}{name}"
    def tx(el, *path):
        node = el
        for p in path:
            if node is None: return ""
            node = node.find(t(p))
        return (node.text or "").strip() if node is not None else ""
    try:
        root = ET.fromstring(xml_text)
        nfe  = root.find(t("NFe"))
        inf  = nfe.find(t("infNFe"))
        ide  = inf.find(t("ide"))
        emit = inf.find(t("emit"))
        dest = inf.find(t("dest"))
        chave = inf.get("Id", "").replace("NFe", "")
        return (
            chave,
            tx(emit, "CNPJ"),
            tx(emit, "enderEmit", "UF"),
            tx(dest, "CNPJ"),
            tx(dest, "enderDest", "UF"),
            (tx(ide, "dhEmi") or "")[:10].replace("-", ""),
            tx(ide, "serie"),
            tx(ide, "nNF"),
            tx(emit, "CRT"),
        )
    except Exception:
        return None


@udf(returnType=ArrayType(NFE_ITEM_SCHEMA))
def parse_nfe_itens(xml_text: str):
    import xml.etree.ElementTree as ET
    NS = "http://www.portalfiscal.inf.br/nfe"
    def t(name): return f"{{{NS}}}{name}"
    def tx(el, *path):
        node = el
        for p in path:
            if node is None: return ""
            node = node.find(t(p))
        return (node.text or "").strip() if node is not None else ""
    try:
        root = ET.fromstring(xml_text)
        nfe  = root.find(t("NFe"))
        inf  = nfe.find(t("infNFe"))
        chave = inf.get("Id", "").replace("NFe", "")
        itens = []
        for det in inf.findall(t("det")):
            num  = det.get("nItem", "")
            prod = det.find(t("prod"))
            imp  = det.find(t("imposto"))
            icms_node = imp.find(t("ICMS")) if imp is not None else None
            icms_tipo = icms_node[0] if icms_node is not None and len(icms_node) else None
            ipi_node  = imp.find(t("IPI"))    if imp is not None else None
            ipi_trib  = ipi_node.find(t("IPITrib")) if ipi_node is not None else None
            pis_node  = imp.find(t("PIS"))    if imp is not None else None
            pis_t     = pis_node[0]           if pis_node is not None and len(pis_node) else None
            cof_node  = imp.find(t("COFINS")) if imp is not None else None
            cof_t     = cof_node[0]           if cof_node is not None and len(cof_node) else None
            total    = inf.find(t("total"))
            icms_tot = total.find(t("ICMSTot")) if total is not None else None
            itens.append((
                chave, num,
                tx(prod, "cProd"), tx(prod, "NCM"),  tx(prod, "CFOP"),
                tx(prod, "uCom"),  tx(prod, "qCom"),  tx(prod, "vUnCom"),
                tx(prod, "vProd"),
                tx(icms_tipo, "CST")   if icms_tipo is not None else "",
                tx(icms_tipo, "vBC")   if icms_tipo is not None else "",
                tx(icms_tipo, "pICMS") if icms_tipo is not None else "",
                tx(icms_tipo, "vICMS") if icms_tipo is not None else "",
                tx(ipi_trib,  "vIPI")  if ipi_trib  is not None else "0.00",
                tx(pis_t,  "CST")      if pis_t  is not None else "",
                tx(pis_t,  "vPIS")     if pis_t  is not None else "0.00",
                tx(cof_t,  "CST")      if cof_t  is not None else "",
                tx(cof_t,  "vCOFINS")  if cof_t  is not None else "0.00",
                tx(icms_tot, "vNF")    if icms_tot is not None else "",
            ))
        return itens
    except Exception:
        return []


# ── Ingestão NF-e ─────────────────────────────────────────────────────────────

def _parse_nfe_python(xml_path: Path):
    """Parseia um XML NF-e com Python puro. Retorna (cabecalho_dict, [itens_dict])."""
    import xml.etree.ElementTree as ET
    NS = "http://www.portalfiscal.inf.br/nfe"
    def t(name): return f"{{{NS}}}{name}"
    def tx(el, *path):
        node = el
        for p in path:
            if node is None: return ""
            node = node.find(t(p))
        return (node.text or "").strip() if node is not None else ""

    try:
        root = ET.parse(xml_path).getroot()
        nfe  = root.find(t("NFe"))
        inf  = nfe.find(t("infNFe"))
        ide  = inf.find(t("ide"))
        emit = inf.find(t("emit"))
        dest = inf.find(t("dest"))
        chave = inf.get("Id", "").replace("NFe", "")

        cab = {
            "arquivo":           str(xml_path),
            "chv_nfe":           chave,
            "cnpj_emitente":     tx(emit, "CNPJ"),
            "uf_emitente":       tx(emit, "enderEmit", "UF"),
            "cnpj_destinatario": tx(dest, "CNPJ"),
            "uf_destinatario":   tx(dest, "enderDest", "UF"),
            "dt_emissao":        (tx(ide, "dhEmi") or "")[:10].replace("-", ""),
            "serie":             tx(ide, "serie"),
            "num_nf":            tx(ide, "nNF"),
            "crt":               tx(emit, "CRT"),
        }

        itens = []
        for det in inf.findall(t("det")):
            prod     = det.find(t("prod"))
            imp      = det.find(t("imposto"))
            icms_n   = imp.find(t("ICMS"))    if imp is not None else None
            icms_t   = icms_n[0]              if icms_n is not None and len(icms_n) else None
            ipi_n    = imp.find(t("IPI"))     if imp is not None else None
            ipi_t    = ipi_n.find(t("IPITrib")) if ipi_n is not None else None
            pis_n    = imp.find(t("PIS"))     if imp is not None else None
            pis_t    = pis_n[0]               if pis_n is not None and len(pis_n) else None
            cof_n    = imp.find(t("COFINS"))  if imp is not None else None
            cof_t    = cof_n[0]               if cof_n is not None and len(cof_n) else None
            total    = inf.find(t("total"))
            icms_tot = total.find(t("ICMSTot")) if total is not None else None
            itens.append({
                "chv_nfe":     chave,
                "num_item":    det.get("nItem", ""),
                "cod_produto": tx(prod, "cProd"),
                "ncm":         tx(prod, "NCM"),
                "cfop":        tx(prod, "CFOP"),
                "ucom":        tx(prod, "uCom"),
                "qtd":         tx(prod, "qCom"),
                "v_unit":      tx(prod, "vUnCom"),
                "v_prod":      tx(prod, "vProd"),
                "cst_icms":    tx(icms_t, "CST")   if icms_t  is not None else "",
                "v_bc_icms":   tx(icms_t, "vBC")   if icms_t  is not None else "",
                "aliq_icms":   tx(icms_t, "pICMS") if icms_t  is not None else "",
                "v_icms":      tx(icms_t, "vICMS") if icms_t  is not None else "",
                "v_ipi":       tx(ipi_t,  "vIPI")  if ipi_t   is not None else "0.00",
                "cst_pis":     tx(pis_t,  "CST")   if pis_t   is not None else "",
                "v_pis":       tx(pis_t,  "vPIS")  if pis_t   is not None else "0.00",
                "cst_cofins":  tx(cof_t,  "CST")   if cof_t   is not None else "",
                "v_cofins":    tx(cof_t,  "vCOFINS") if cof_t is not None else "0.00",
                "v_nf":        tx(icms_tot, "vNF") if icms_tot is not None else "",
            })
        return cab, itens
    except Exception:
        return None, []


def ingerir_nfe_local(spark: SparkSession) -> None:
    """Parseia XMLs NF-e com Python puro (sem UDF) e grava Parquet no bronze local."""
    nfe_dir = LOCAL_RAW / "nfe"
    print(f"[NF-e] Lendo XMLs de {nfe_dir}")

    cabs, itens_flat = [], []
    for xml_path in sorted(nfe_dir.rglob("*.xml")):
        cab, itens = _parse_nfe_python(xml_path)
        if cab:
            cabs.append(cab)
            itens_flat.extend(itens)

    print(f"[NF-e] Parseados: {len(cabs)} cabecalhos, {len(itens_flat)} itens")

    df_cab   = spark.createDataFrame(cabs)
    df_itens = spark.createDataFrame(itens_flat)

    dest_cab   = str(LOCAL_BRONZE / "nfe_cabecalho")
    dest_itens = str(LOCAL_BRONZE / "nfe_itens")
    df_cab.write.mode("overwrite").parquet(dest_cab)
    df_itens.write.mode("overwrite").parquet(dest_itens)
    print(f"[NF-e] nfe_cabecalho -> {dest_cab}  ({len(cabs)} registros)")
    print(f"[NF-e] nfe_itens     -> {dest_itens} ({len(itens_flat)} registros)")


def ingerir_nfe_databricks(spark: SparkSession) -> None:
    nfe_path = f"{ADLS_LANDING}/nfe"
    print(f"[NF-e] Lendo XMLs de {nfe_path}")

    df_raw = (
        spark.read
        .text(nfe_path, wholetext=True, recursiveFileLookup=True)
        .withColumn("arquivo", input_file_name())
    )

    df_cab = (
        df_raw
        .withColumn("cab", parse_nfe_cabecalho(col("value")))
        .select(
            col("arquivo"),
            col("cab.chv_nfe"),        col("cab.cnpj_emitente"),
            col("cab.uf_emitente"),    col("cab.cnpj_destinatario"),
            col("cab.uf_destinatario"),col("cab.dt_emissao"),
            col("cab.serie"),          col("cab.num_nf"),
            col("cab.crt"),
        )
        .filter(col("chv_nfe").isNotNull())
    )

    df_itens = (
        df_raw
        .withColumn("itens", parse_nfe_itens(col("value")))
        .withColumn("item", explode(col("itens")))
        .select(
            col("item.chv_nfe"),    col("item.num_item"),
            col("item.cod_produto"),col("item.ncm"),
            col("item.cfop"),       col("item.ucom"),
            col("item.qtd"),        col("item.v_unit"),
            col("item.v_prod"),     col("item.cst_icms"),
            col("item.v_bc_icms"),  col("item.aliq_icms"),
            col("item.v_icms"),     col("item.v_ipi"),
            col("item.cst_pis"),    col("item.v_pis"),
            col("item.cst_cofins"), col("item.v_cofins"),
            col("item.v_nf"),
        )
        .filter(col("chv_nfe").isNotNull())
    )

    (
        df_cab.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.nfe_cabecalho")
    )
    (
        df_itens.write.format("delta").mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.bronze.nfe_itens")
    )
    print(f"[NF-e] {CATALOG}.bronze.nfe_cabecalho e nfe_itens gravados")


# ── Ingestão SPED Fiscal ──────────────────────────────────────────────────────

SPED_REGISTROS = {
    "C100": ["ind_oper", "ind_emit", "cod_part", "cod_mod", "cod_sit",
             "ser", "num_doc", "chv_nfe", "dt_doc", "dt_e_s", "vl_doc",
             "ind_pgto", "vl_desc", "vl_abat_nt", "vl_merc", "ind_frt",
             "vl_frt", "vl_seg", "vl_out_da", "vl_bc_icms", "vl_icms",
             "vl_bc_icms_st", "vl_icms_st", "vl_ipi", "vl_pis", "vl_cofins",
             "vl_pis_st", "vl_cofins_st"],
    "C170": ["num_item", "cod_item", "descr_compl", "qtd", "unid",
             "vl_item", "vl_desc", "ind_mov", "cst_icms", "cfop",
             "cod_nat", "vl_bc_icms", "aliq_icms", "vl_icms",
             "vl_bc_icms_st", "aliq_st", "vl_icms_st", "ind_apur",
             "cst_ipi", "cod_enq", "vl_bc_ipi", "aliq_ipi", "vl_ipi",
             "cst_pis", "vl_bc_pis", "aliq_pis", "quant_bc_pis", "vl_pis",
             "cst_cofins", "vl_bc_cofins", "aliq_cofins", "quant_bc_cofins",
             "vl_cofins", "cod_cta", "vl_abat_nt"],
    "E110": ["vl_tot_debitos", "vl_aj_debitos", "vl_tot_aj_debitos",
             "vl_estornos_cred", "vl_tot_creditos", "vl_aj_creditos",
             "vl_tot_aj_creditos", "vl_estornos_deb", "vl_sld_credor_ant",
             "vl_sld_apurado", "vl_tot_ded", "vl_icms_recolher",
             "vl_sld_credor_transp", "deb_esp"],
}


def _ingerir_pipe_delimited(spark, path: str, registros: dict, destino_base):
    df_raw = (
        spark.read.text(path, recursiveFileLookup=True)
        .withColumn("arquivo",      input_file_name())
        .withColumn("cnpj_empresa", regexp_extract(col("arquivo"), r"[/\\](\d{14})[/\\]", 1))
        .withColumn("competencia",  regexp_extract(col("arquivo"), r"_(\d{6})\.txt", 1))
        .withColumn("campos",       split(col("value"), "\\|"))
        .withColumn("registro",     col("campos")[1])
    )

    resultados = {}
    for reg, colunas in registros.items():
        df_reg = df_raw.filter(col("registro") == reg)
        for i, nome in enumerate(colunas, start=2):
            df_reg = df_reg.withColumn(nome, col("campos")[i])
        df_reg = df_reg.drop("value", "campos", "registro")
        resultados[reg] = df_reg

        dest = str(destino_base / f"sped_fiscal_{reg.lower()}")
        df_reg.write.mode("overwrite").parquet(dest)
        print(f"  {reg} -> {dest} ({df_reg.count()} registros)")

    return resultados


def ingerir_sped_local(spark: SparkSession) -> None:
    path = str(LOCAL_RAW / "sped_fiscal")
    print(f"[SPED] Lendo arquivos de {path}")
    _ingerir_pipe_delimited(spark, path, SPED_REGISTROS, LOCAL_BRONZE)


def ingerir_sped_databricks(spark: SparkSession) -> None:
    raw_path = f"{ADLS_LANDING}/sped_fiscal"
    df_raw = (
        spark.read.text(raw_path, recursiveFileLookup=True)
        .withColumn("arquivo",      input_file_name())
        .withColumn("cnpj_empresa", regexp_extract(col("arquivo"), r"/(\d{14})/", 1))
        .withColumn("competencia",  regexp_extract(col("arquivo"), r"_(\d{6})\.txt", 1))
        .withColumn("campos",       split(col("value"), "\\|"))
        .withColumn("registro",     col("campos")[1])
    )
    for reg, colunas in SPED_REGISTROS.items():
        df_reg = df_raw.filter(col("registro") == reg)
        for i, nome in enumerate(colunas, start=2):
            df_reg = df_reg.withColumn(nome, col("campos")[i])
        (
            df_reg.drop("value", "campos", "registro")
            .write.format("delta").mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(f"{CATALOG}.bronze.sped_fiscal_{reg.lower()}")
        )
        print(f"[SPED] {CATALOG}.bronze.sped_fiscal_{reg.lower()} gravado")


# ── Ingestão EFD Contribuições ────────────────────────────────────────────────

EFD_REGISTROS = {
    "C100": ["ind_oper", "ind_emit", "cod_part", "cod_mod", "cod_sit",
             "ser", "num_doc", "chv_nfe", "dt_doc", "dt_e_s", "vl_doc",
             "vl_desc", "vl_bc_pis", "vl_pis", "vl_bc_cofins", "vl_cofins",
             "nat_bc_cred", "ind_orig_cred", "ind_util_cred_pres",
             "vl_ret_pis", "vl_ret_cofins", "ind_nat_frt"],
    "C170": ["num_item", "cod_item", "descr_compl", "vl_item", "vl_desc",
             "nat_bc_cred", "ind_orig_cred",
             "cst_pis", "vl_bc_pis", "aliq_pis", "vl_pis",
             "cst_cofins", "vl_bc_cofins", "aliq_cofins", "vl_cofins", "cod_cta"],
    "M200": ["vl_tot_cont_nc_per", "vl_tot_cred_desc", "vl_tot_cred_desc_ant",
             "vl_tot_cont_nc_dev", "vl_ret", "vl_out_ded",
             "vl_cont_nrec", "vl_cont_cump"],
    "M600": ["vl_tot_cont_nc_per", "vl_tot_cred_desc", "vl_tot_cred_desc_ant",
             "vl_tot_cont_nc_dev", "vl_ret", "vl_out_ded",
             "vl_cont_nrec", "vl_cont_cump"],
}


def ingerir_efd_local(spark: SparkSession) -> None:
    path = str(LOCAL_RAW / "efd_contribuicoes")
    print(f"[EFD]  Lendo arquivos de {path}")

    df_raw = (
        spark.read.text(path, recursiveFileLookup=True)
        .withColumn("arquivo",      input_file_name())
        .withColumn("cnpj_empresa", regexp_extract(col("arquivo"), r"[/\\](\d{14})[/\\]", 1))
        .withColumn("competencia",  regexp_extract(col("arquivo"), r"_(\d{6})\.txt", 1))
        .withColumn("campos",       split(col("value"), "\\|"))
        .withColumn("registro",     col("campos")[1])
    )

    for reg, colunas in EFD_REGISTROS.items():
        df_reg = df_raw.filter(col("registro") == reg)
        for i, nome in enumerate(colunas, start=2):
            df_reg = df_reg.withColumn(nome, col("campos")[i])
        df_reg = df_reg.drop("value", "campos", "registro")
        dest = str(LOCAL_BRONZE / f"efd_contrib_{reg.lower()}")
        df_reg.write.mode("overwrite").parquet(dest)
        print(f"  {reg} -> {dest} ({df_reg.count()} registros)")


def ingerir_efd_databricks(spark: SparkSession) -> None:
    raw_path = f"{ADLS_LANDING}/efd_contribuicoes"
    df_raw = (
        spark.read.text(raw_path, recursiveFileLookup=True)
        .withColumn("arquivo",      input_file_name())
        .withColumn("cnpj_empresa", regexp_extract(col("arquivo"), r"/(\d{14})/", 1))
        .withColumn("competencia",  regexp_extract(col("arquivo"), r"_(\d{6})\.txt", 1))
        .withColumn("campos",       split(col("value"), "\\|"))
        .withColumn("registro",     col("campos")[1])
    )
    for reg, colunas in EFD_REGISTROS.items():
        df_reg = df_raw.filter(col("registro") == reg)
        for i, nome in enumerate(colunas, start=2):
            df_reg = df_reg.withColumn(nome, col("campos")[i])
        (
            df_reg.drop("value", "campos", "registro")
            .write.format("delta").mode("overwrite")
            .option("mergeSchema", "true")
            .saveAsTable(f"{CATALOG}.bronze.efd_contrib_{reg.lower()}")
        )
        print(f"[EFD]  {CATALOG}.bronze.efd_contrib_{reg.lower()} gravado")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["nfe", "sped", "efd", "all"], default="all")
    parser.add_argument("--local",  action="store_true",
                        help="Executa com PySpark local (sem Databricks). "
                             "Lê data/raw/ e grava em data/bronze/ como Parquet.")
    args = parser.parse_args()

    spark = get_spark(local=args.local)

    if args.local:
        LOCAL_BRONZE.mkdir(parents=True, exist_ok=True)
        print(f"Modo   : LOCAL  (PySpark {spark.version})")
        print(f"Entrada: {LOCAL_RAW}")
        print(f"Saída  : {LOCAL_BRONZE}\n")
        dispatch = {
            "nfe":  ingerir_nfe_local,
            "sped": ingerir_sped_local,
            "efd":  ingerir_efd_local,
        }
    else:
        print(f"Modo   : DATABRICKS  (catalog={CATALOG})")
        print(f"Entrada: {ADLS_LANDING}\n")
        dispatch = {
            "nfe":  ingerir_nfe_databricks,
            "sped": ingerir_sped_databricks,
            "efd":  ingerir_efd_databricks,
        }

    fontes = ["nfe", "sped", "efd"] if args.source == "all" else [args.source]
    for fonte in fontes:
        dispatch[fonte](spark)

    print("\nIngestão concluída.")


if __name__ == "__main__":
    main()

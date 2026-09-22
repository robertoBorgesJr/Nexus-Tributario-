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
import xml.etree.ElementTree as ET
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, lit, regexp_extract, split, trim, udf,
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

NFE_NS = "http://www.portalfiscal.inf.br/nfe"


# ── SparkSession ──────────────────────────────────────────────────────────────

def get_spark(local: bool) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName("nexus-tributario-ingestao-bronze")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    if local:
        builder = builder.master("local[*]")
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


def _tag(name: str) -> str:
    return f"{{{NFE_NS}}}{name}"


def _txt(el, *path) -> str:
    node = el
    for tag in path:
        if node is None:
            return ""
        node = node.find(_tag(tag))
    return (node.text or "").strip() if node is not None else ""


@udf(returnType=NFE_CABECALHO_SCHEMA)
def parse_nfe_cabecalho(xml_text: str):
    try:
        root = ET.fromstring(xml_text)
        nfe  = root.find(_tag("NFe"))
        inf  = nfe.find(_tag("infNFe"))
        ide  = inf.find(_tag("ide"))
        emit = inf.find(_tag("emit"))
        dest = inf.find(_tag("dest"))
        chave = inf.get("Id", "").replace("NFe", "")
        return (
            chave,
            _txt(emit, "CNPJ"),
            _txt(emit, "enderEmit", "UF"),
            _txt(dest, "CNPJ"),
            _txt(dest, "enderDest", "UF"),
            (_txt(ide, "dhEmi") or "")[:10].replace("-", ""),
            _txt(ide, "serie"),
            _txt(ide, "nNF"),
            _txt(emit, "CRT"),
        )
    except Exception:
        return None


@udf(returnType=ArrayType(NFE_ITEM_SCHEMA))
def parse_nfe_itens(xml_text: str):
    try:
        root = ET.fromstring(xml_text)
        nfe  = root.find(_tag("NFe"))
        inf  = nfe.find(_tag("infNFe"))
        chave = inf.get("Id", "").replace("NFe", "")
        itens = []
        for det in inf.findall(_tag("det")):
            num = det.get("nItem", "")
            prod  = det.find(_tag("prod"))
            imp   = det.find(_tag("imposto"))
            icms_node = imp.find(_tag("ICMS")) if imp is not None else None
            icms_tipo = (
                icms_node[0] if icms_node is not None and len(icms_node) else None
            )
            ipi_node   = imp.find(_tag("IPI"))        if imp else None
            ipi_trib   = ipi_node.find(_tag("IPITrib")) if ipi_node is not None else None
            pis_node   = imp.find(_tag("PIS"))         if imp else None
            pis_t      = pis_node[0]                   if pis_node is not None and len(pis_node) else None
            cofins_node = imp.find(_tag("COFINS"))     if imp else None
            cofins_t    = cofins_node[0]               if cofins_node is not None and len(cofins_node) else None
            total = inf.find(_tag("total"))
            icms_tot = total.find(_tag("ICMSTot")) if total is not None else None
            itens.append((
                chave, num,
                _txt(prod, "cProd"), _txt(prod, "NCM"), _txt(prod, "CFOP"),
                _txt(prod, "uCom"), _txt(prod, "qCom"), _txt(prod, "vUnCom"),
                _txt(prod, "vProd"),
                _txt(icms_tipo, "CST") if icms_tipo is not None else "",
                _txt(icms_tipo, "vBC") if icms_tipo is not None else "",
                _txt(icms_tipo, "pICMS") if icms_tipo is not None else "",
                _txt(icms_tipo, "vICMS") if icms_tipo is not None else "",
                _txt(ipi_trib, "vIPI") if ipi_trib is not None else "0.00",
                _txt(pis_t, "CST")  if pis_t is not None else "",
                _txt(pis_t, "vPIS") if pis_t is not None else "0.00",
                _txt(cofins_t, "CST")     if cofins_t is not None else "",
                _txt(cofins_t, "vCOFINS") if cofins_t is not None else "0.00",
                _txt(icms_tot, "vNF") if icms_tot is not None else "",
            ))
        return itens
    except Exception:
        return []


# ── Ingestão NF-e ─────────────────────────────────────────────────────────────

def ingerir_nfe_local(spark: SparkSession) -> None:
    path = str(LOCAL_RAW / "nfe")
    print(f"[NF-e] Lendo XMLs de {path}")

    df_raw = (
        spark.read
        .option("wholetext", "true")
        .text(path, recursiveFileLookup=True)
        .withColumn("arquivo", input_file_name())
    )

    from pyspark.sql.functions import explode

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

    dest_cab   = str(LOCAL_BRONZE / "nfe_cabecalho")
    dest_itens = str(LOCAL_BRONZE / "nfe_itens")
    df_cab.write.mode("overwrite").parquet(dest_cab)
    df_itens.write.mode("overwrite").parquet(dest_itens)
    print(f"[NF-e] nfe_cabecalho -> {dest_cab}  ({df_cab.count()} registros)")
    print(f"[NF-e] nfe_itens     -> {dest_itens} ({df_itens.count()} registros)")


def ingerir_nfe_databricks(spark: SparkSession) -> None:
    nfe_path   = f"{ADLS_LANDING}/nfe"
    checkpoint = f"{ADLS_BASE}/checkpoints/nfe_autoloader"
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "xml")
        .option("rowTag", "nfeProc")
        .option("cloudFiles.schemaLocation", checkpoint)
        .load(nfe_path)
    )
    print(f"[NF-e] Auto Loader configurado: {nfe_path} -> {CATALOG}.bronze.nfe_cabecalho")


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

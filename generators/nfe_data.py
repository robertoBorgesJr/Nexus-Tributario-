"""
Núcleo de geração de dados sintéticos de NF-e.

Produz uma lista de dicts com todos os campos necessários para gravar
em XML (NF-e), SPED Fiscal e EFD Contribuições. Todos os geradores
importam daqui para garantir consistência entre as três fontes.
"""
import random
from datetime import date

from config import (
    CLIENTES,
    EMPRESAS,
    NF_POR_MES,
    PERIODOS,
    PRODUTOS,
    SEED_ALEATORIA,
    ALIQ_ICMS_INTRA,
    ALIQ_ICMS_INTER,
    PIS_ALIQ,
    COFINS_ALIQ,
)

rng = random.Random(SEED_ALEATORIA)


def _dv_modulo11(chave: str) -> str:
    pesos = [2, 3, 4, 5, 6, 7, 8, 9]
    soma = sum(int(d) * pesos[i % 8] for i, d in enumerate(reversed(chave)))
    resto = soma % 11
    return "0" if resto in (0, 1) else str(11 - resto)


def _chave_acesso(empresa: dict, periodo: date, nnf: int) -> str:
    cuf = str(empresa["cuf"]).zfill(2)
    aamm = periodo.strftime("%y%m")
    cnpj = empresa["cnpj"]
    mod = "55"
    ser = "001"
    nnf_str = str(nnf).zfill(9)
    temis = "1"
    cnf = str(rng.randint(10000000, 99999999))
    base = cuf + aamm + cnpj + mod + ser + nnf_str + temis + cnf
    return base + _dv_modulo11(base)


def _aliq_icms(uf_origem: str, uf_destino: str) -> float:
    if uf_origem == uf_destino:
        return ALIQ_ICMS_INTRA.get(uf_origem, 12.0)
    return ALIQ_ICMS_INTER.get((uf_origem, uf_destino), 12.0)


def _gerar_nf(empresa: dict, cliente: dict, produto: dict, periodo: date, nnf: int) -> dict:
    qtd = rng.randint(1, 20)
    v_unit = round(produto["valor_unitario"] * rng.uniform(0.9, 1.1), 2)
    v_prod = round(qtd * v_unit, 2)

    # ICMS
    aliq_icms = _aliq_icms(empresa["uf"], cliente["uf"])
    v_icms = round(v_prod * aliq_icms / 100, 2)

    # IPI (apenas industria / Lucro Real)
    aliq_ipi = produto["ipi_aliq"] if empresa["regime"] == "LUCRO_REAL" else 0.0
    v_ipi = round(v_prod * aliq_ipi / 100, 2)

    # PIS / COFINS (Simples Nacional recolhe no DAS — alíquota zero na nota)
    if empresa["regime"] == "SIMPLES_NACIONAL":
        aliq_pis, aliq_cofins = 0.0, 0.0
        cst_piscofins = "07"
    else:
        aliq_pis, aliq_cofins = PIS_ALIQ, COFINS_ALIQ
        cst_piscofins = produto["cst_pis_cofins_saida"]

    v_pis = round(v_prod * aliq_pis / 100, 2)
    v_cofins = round(v_prod * aliq_cofins / 100, 2)
    v_nf = round(v_prod + v_ipi, 2)

    dias_mes = 28 if periodo.month == 2 else 30
    dia = rng.randint(1, dias_mes)
    hora = rng.randint(8, 17)
    minuto = rng.randint(0, 59)
    dh_emi = periodo.replace(day=dia)

    cfop = "5101" if empresa["uf"] == cliente["uf"] else "6101"
    id_dest = 1 if empresa["uf"] == cliente["uf"] else 2
    chave = _chave_acesso(empresa, periodo, nnf)

    return {
        "chave": chave,
        "empresa": empresa,
        "cliente": cliente,
        "produto": produto,
        "nnf": nnf,
        "serie": 1,
        "dh_emi": f"{dh_emi.strftime('%Y-%m-%d')}T{hora:02d}:{minuto:02d}:00-03:00",
        "dt_doc": dh_emi.strftime("%Y%m%d"),
        "periodo": periodo,
        "id_dest": id_dest,
        "cfop": cfop,
        "qtd": qtd,
        "v_unit": v_unit,
        "v_prod": v_prod,
        "cst_icms": produto["cst_icms"],
        "v_bc_icms": v_prod,
        "aliq_icms": aliq_icms,
        "v_icms": v_icms,
        "aliq_ipi": aliq_ipi,
        "v_ipi": v_ipi,
        "cst_pis": cst_piscofins,
        "v_bc_pis": v_prod,
        "aliq_pis": aliq_pis,
        "v_pis": v_pis,
        "cst_cofins": cst_piscofins,
        "v_bc_cofins": v_prod,
        "aliq_cofins": aliq_cofins,
        "v_cofins": v_cofins,
        "v_nf": v_nf,
    }


def gerar_notas(empresa: dict, periodo: date) -> list[dict]:
    return [
        _gerar_nf(empresa, rng.choice(CLIENTES), rng.choice(PRODUTOS), periodo, nnf)
        for nnf in range(1, NF_POR_MES + 1)
    ]


def gerar_todas_notas() -> dict:
    """Retorna {(cnpj, periodo): [nfs]} para todos os períodos e empresas."""
    resultado = {}
    for empresa in EMPRESAS:
        for periodo in PERIODOS:
            resultado[(empresa["cnpj"], periodo)] = gerar_notas(empresa, periodo)
    return resultado

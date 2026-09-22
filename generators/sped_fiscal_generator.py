"""
Gerador de SPED Fiscal (EFD ICMS/IPI) — leiaute 17.

Gera um arquivo .txt por empresa por mês com os registros:
  0000, 0001, 0150, 0190, 0200, 0990
  C001, C100, C170, C190, C990
  E001, E100, E110, E990
  9001, 9900, 9990, 9999

Os valores de ICMS são intencionalmente divergentes em ~10% das notas
para exercitar o modelo de conciliação NF-e vs SPED.
"""
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import EMPRESAS, PERIODOS
from nfe_data import gerar_notas

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw" / "sped_fiscal"

# Semente separada para introduzir divergências controladass
rng_div = random.Random(99)


def _fmt(v) -> str:
    """Formata valor numérico para o padrão SPED (2 decimais, sem separador de milhar)."""
    return f"{v:.2f}"


def _pipe(*campos) -> str:
    return "|" + "|".join(str(c) for c in campos) + "|"


def _divergencia_icms(v_icms: float) -> float:
    """Introduz divergência de ±5% em ~10% das notas para simular erros de apuração."""
    if rng_div.random() < 0.10:
        fator = rng_div.uniform(0.95, 1.05)
        return round(v_icms * fator, 2)
    return v_icms


def _registros_participantes(nfs: list[dict]) -> list[str]:
    vistos = {}
    for nf in nfs:
        c = nf["cliente"]
        if c["cnpj"] not in vistos:
            vistos[c["cnpj"]] = c
    linhas = []
    for i, (cnpj, c) in enumerate(vistos.items(), start=1):
        linhas.append(_pipe("0150", str(i).zfill(3), "1", "1058", cnpj, "",
                            c["ie"], c["cmun"], "", c["xnome"]))
    return linhas, {cnpj: str(i).zfill(3) for i, cnpj in enumerate(vistos.keys(), start=1)}


def _registros_produtos(nfs: list[dict]) -> list[str]:
    vistos = {}
    for nf in nfs:
        p = nf["produto"]
        if p["cprod"] not in vistos:
            vistos[p["cprod"]] = p
    linhas = []
    for p in vistos.values():
        linhas.append(_pipe("0190", p["ucom"], p["ucom"]))
        linhas.append(_pipe("0200", p["cprod"], p["xprod"], p["cean"],
                            "", p["ucom"], "04", p["ncm"], "", "", "", "", ""))
    return linhas


def gerar_sped(empresa: dict, periodo, nfs: list[dict]) -> str:
    dt_ini = periodo.strftime("%d%m%Y")
    dt_fin_dia = 28 if periodo.month == 2 else 30
    dt_fin = periodo.replace(day=dt_fin_dia).strftime("%d%m%Y")

    participantes_linhas, cod_part_map = _registros_participantes(nfs)
    produtos_linhas = _registros_produtos(nfs)

    linhas_0 = [
        _pipe("0000", "017", "LEIAUTE 17", dt_ini, dt_fin,
              empresa["razao_social"], empresa["cnpj"], "",
              empresa["uf"], empresa["ie"], empresa["cmun"], "1", "1"),
        _pipe("0001", "1"),
    ]
    linhas_0 += participantes_linhas
    linhas_0 += produtos_linhas
    linhas_0.append(_pipe("0990", str(len(linhas_0) + 1)))

    # ── Bloco C (documentos fiscais) ─────────────────────────────────────────
    linhas_c = [_pipe("C001", "1")]
    agrupamento_c190: dict[tuple, dict] = {}

    for nf in nfs:
        cod_part = cod_part_map.get(nf["cliente"]["cnpj"], "001")
        v_icms_sped = _divergencia_icms(nf["v_icms"])

        linhas_c.append(_pipe(
            "C100", "1", "0", cod_part, "55", "00",
            str(nf["serie"]), str(nf["nnf"]), nf["chave"],
            nf["dt_doc"], nf["dt_doc"],
            _fmt(nf["v_nf"]), "0", "", "",
            _fmt(nf["v_prod"]), "9", "0.00", "0.00", "0.00",
            _fmt(nf["v_bc_icms"]), _fmt(v_icms_sped),
            "0.00", "0.00",
            _fmt(nf["v_ipi"]),
            _fmt(nf["v_pis"]), _fmt(nf["v_cofins"]),
            "0.00", "0.00",
        ))
        linhas_c.append(_pipe(
            "C170", "1", nf["produto"]["cprod"], "",
            _fmt(nf["qtd"]), nf["produto"]["ucom"],
            _fmt(nf["v_prod"]), "0.00", "0",
            nf["cst_icms"], nf["cfop"], "",
            _fmt(nf["v_bc_icms"]), _fmt(nf["aliq_icms"]), _fmt(v_icms_sped),
            "0.00", "0.00", "0.00", "T",
            "50" if nf["aliq_ipi"] > 0 else "99",
            "999",
            _fmt(nf["v_prod"]), _fmt(nf["aliq_ipi"]), _fmt(nf["v_ipi"]),
            nf["cst_pis"], _fmt(nf["v_bc_pis"]), _fmt(nf["aliq_pis"]),
            "0", _fmt(nf["v_pis"]),
            nf["cst_cofins"], _fmt(nf["v_bc_cofins"]), _fmt(nf["aliq_cofins"]),
            "0", _fmt(nf["v_cofins"]), "", "0.00",
        ))

        # Agrega C190 por (CST, CFOP, alíquota)
        chave_c190 = (nf["cst_icms"], nf["cfop"], nf["aliq_icms"])
        if chave_c190 not in agrupamento_c190:
            agrupamento_c190[chave_c190] = {"v_opr": 0, "v_bc": 0, "v_icms": 0}
        agrupamento_c190[chave_c190]["v_opr"] += nf["v_prod"]
        agrupamento_c190[chave_c190]["v_bc"] += nf["v_bc_icms"]
        agrupamento_c190[chave_c190]["v_icms"] += v_icms_sped

    for (cst, cfop, aliq), vals in agrupamento_c190.items():
        linhas_c.append(_pipe(
            "C190", cst, cfop, _fmt(aliq),
            _fmt(vals["v_opr"]), _fmt(vals["v_bc"]), _fmt(vals["v_icms"]),
            "0.00", "0.00", "0.00", "",
        ))

    linhas_c.append(_pipe("C990", str(len(linhas_c) + 1)))

    # ── Bloco E (apuração ICMS) ───────────────────────────────────────────────
    v_tot_debitos = sum(
        _divergencia_icms(nf["v_icms"]) for nf in nfs if nf["v_icms"] > 0
    )
    linhas_e = [
        _pipe("E001", "1"),
        _pipe("E100", dt_ini, dt_fin),
        _pipe("E110",
              _fmt(v_tot_debitos), "0.00", _fmt(v_tot_debitos),
              "0.00", "0.00", "0.00", "0.00", "0.00",
              "0.00", _fmt(v_tot_debitos), "0.00",
              _fmt(v_tot_debitos), "0.00", "0.00"),
        _pipe("E990", "4"),
    ]

    # ── Bloco 9 (totalizadores) ───────────────────────────────────────────────
    contagens = {
        "0000": 1, "0001": 1, "0150": len(cod_part_map),
        "0190": len(set(nf["produto"]["ucom"] for nf in nfs)),
        "0200": len(set(nf["produto"]["cprod"] for nf in nfs)),
        "0990": 1,
        "C001": 1, "C100": len(nfs), "C170": len(nfs),
        "C190": len(agrupamento_c190), "C990": 1,
        "E001": 1, "E100": 1, "E110": 1, "E990": 1,
        "9001": 1, "9990": 1, "9999": 1,
    }
    linhas_9 = [_pipe("9001", "1")]
    for reg, qtd in sorted(contagens.items()):
        linhas_9.append(_pipe("9900", reg, str(qtd)))
    total_geral = sum(contagens.values()) + len(contagens) + 3  # 9900 lines + 9001 + 9990 + 9999
    linhas_9.append(_pipe("9990", str(len(linhas_9) + 1)))
    linhas_9.append(_pipe("9999", str(total_geral)))

    todas = linhas_0 + linhas_c + linhas_e + linhas_9
    return "\n".join(todas)


def gerar_todos() -> int:
    total = 0
    for empresa in EMPRESAS:
        for periodo in PERIODOS:
            nfs = gerar_notas(empresa, periodo)
            conteudo = gerar_sped(empresa, periodo, nfs)
            saida = OUTPUT_DIR / empresa["cnpj"]
            saida.mkdir(parents=True, exist_ok=True)
            nome = f"SPED_{empresa['cnpj']}_{periodo.strftime('%Y%m')}.txt"
            (saida / nome).write_text(conteudo, encoding="utf-8")
            total += 1
            print(f"  SPED | {empresa['nome_fantasia']} | {periodo.strftime('%Y-%m')} | {len(nfs)} NFs")
    return total


if __name__ == "__main__":
    n = gerar_todos()
    print(f"\nTotal: {n} arquivos SPED gerados em {OUTPUT_DIR}")

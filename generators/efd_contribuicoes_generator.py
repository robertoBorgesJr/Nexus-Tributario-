"""
Gerador de EFD Contribuições (PIS/COFINS) — leiaute 6.

Gera um arquivo .txt por empresa por mês com os registros:
  0000, 0001, 0150, 0190, 0200, 0990
  A001, A990 (bloco vazio — sem notas de serviço)
  C001, C100, C170, C990
  M001, M100, M200, M500, M600, M990
  9001, 9900, 9990, 9999
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from config import EMPRESAS, PERIODOS
from nfe_data import gerar_notas

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw" / "efd_contribuicoes"


def _fmt(v) -> str:
    return f"{v:.2f}"


def _pipe(*campos) -> str:
    return "|" + "|".join(str(c) for c in campos) + "|"


def _participantes(nfs: list[dict]):
    vistos = {}
    for nf in nfs:
        c = nf["cliente"]
        if c["cnpj"] not in vistos:
            vistos[c["cnpj"]] = c
    linhas = []
    mapa = {}
    for i, (cnpj, c) in enumerate(vistos.items(), start=1):
        cod = str(i).zfill(3)
        mapa[cnpj] = cod
        linhas.append(_pipe("0150", cod, "1", "1058", cnpj, "",
                            c["ie"], c["cmun"], "", c["xnome"]))
    return linhas, mapa


def _produtos(nfs: list[dict]) -> list[str]:
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


def gerar_efd(empresa: dict, periodo, nfs: list[dict]) -> str:
    dt_ini = periodo.strftime("%d%m%Y")
    dt_fin_dia = 28 if periodo.month == 2 else 30
    dt_fin = periodo.replace(day=dt_fin_dia).strftime("%d%m%Y")

    ind_nat = "01" if empresa["regime"] == "SIMPLES_NACIONAL" else "05"
    part_linhas, cod_part_map = _participantes(nfs)
    prod_linhas = _produtos(nfs)

    # ── Bloco 0 ────────────────────────────────────────────────────────────────
    linhas_0 = [
        _pipe("0000", "006", "LEIAUTE 6", dt_ini, dt_fin,
              empresa["razao_social"], empresa["cnpj"],
              empresa["uf"], empresa["ie"], empresa["cmun"], "", ind_nat, "1"),
        _pipe("0001", "1"),
    ]
    linhas_0 += part_linhas
    linhas_0 += prod_linhas
    linhas_0.append(_pipe("0990", str(len(linhas_0) + 1)))

    # ── Bloco A (serviços — vazio) ─────────────────────────────────────────────
    linhas_a = [_pipe("A001", "8"), _pipe("A990", "2")]

    # ── Bloco C (mercadorias) ──────────────────────────────────────────────────
    linhas_c = [_pipe("C001", "1")]
    for nf in nfs:
        cod_part = cod_part_map.get(nf["cliente"]["cnpj"], "001")
        linhas_c.append(_pipe(
            "C100", "1", "0", cod_part, "55", "00",
            str(nf["serie"]), str(nf["nnf"]), nf["chave"],
            nf["dt_doc"], nf["dt_doc"],
            _fmt(nf["v_nf"]), "0.00",
            _fmt(nf["v_bc_pis"]), _fmt(nf["v_pis"]),
            _fmt(nf["v_bc_cofins"]), _fmt(nf["v_cofins"]),
            "", "", "0", "0.00", "0.00", "9",
        ))
        linhas_c.append(_pipe(
            "C170", "1", nf["produto"]["cprod"], "",
            _fmt(nf["v_prod"]), "0.00", "", "0",
            nf["cst_pis"],
            _fmt(nf["v_bc_pis"]), _fmt(nf["aliq_pis"]), _fmt(nf["v_pis"]),
            nf["cst_cofins"],
            _fmt(nf["v_bc_cofins"]), _fmt(nf["aliq_cofins"]), _fmt(nf["v_cofins"]),
            "",
        ))
    linhas_c.append(_pipe("C990", str(len(linhas_c) + 1)))

    # ── Bloco M (apuração PIS/COFINS) ─────────────────────────────────────────
    v_bc_pis_total = sum(nf["v_bc_pis"] for nf in nfs)
    v_pis_total = sum(nf["v_pis"] for nf in nfs)
    v_bc_cofins_total = sum(nf["v_bc_cofins"] for nf in nfs)
    v_cofins_total = sum(nf["v_cofins"] for nf in nfs)

    linhas_m = [_pipe("M001", "1")]

    if v_pis_total > 0:
        linhas_m.append(_pipe(
            "M100", "101", "0",
            _fmt(v_bc_pis_total), _fmt(1.65), "0",
            _fmt(v_pis_total), "0.00", "0.00", "0.00",
            _fmt(v_pis_total), "0", "0.00", _fmt(v_pis_total),
        ))

    linhas_m.append(_pipe(
        "M200",
        _fmt(v_pis_total), "0.00", "0.00", "0.00", "0.00", "0.00",
        _fmt(v_pis_total), _fmt(v_pis_total),
    ))

    if v_cofins_total > 0:
        linhas_m.append(_pipe(
            "M500", "501", "0",
            _fmt(v_bc_cofins_total), _fmt(7.60), "0",
            _fmt(v_cofins_total), "0.00", "0.00", "0.00",
            _fmt(v_cofins_total), "0", "0.00", _fmt(v_cofins_total),
        ))

    linhas_m.append(_pipe(
        "M600",
        _fmt(v_cofins_total), "0.00", "0.00", "0.00", "0.00", "0.00",
        _fmt(v_cofins_total), _fmt(v_cofins_total),
    ))
    linhas_m.append(_pipe("M990", str(len(linhas_m) + 1)))

    # ── Bloco 9 ────────────────────────────────────────────────────────────────
    contagens = {
        "0000": 1, "0001": 1, "0150": len(cod_part_map),
        "0190": len(set(nf["produto"]["ucom"] for nf in nfs)),
        "0200": len(set(nf["produto"]["cprod"] for nf in nfs)),
        "0990": 1,
        "A001": 1, "A990": 1,
        "C001": 1, "C100": len(nfs), "C170": len(nfs), "C990": 1,
        "M001": 1, "M200": 1, "M600": 1, "M990": 1,
        "9001": 1, "9990": 1, "9999": 1,
    }
    if v_pis_total > 0:
        contagens["M100"] = 1
    if v_cofins_total > 0:
        contagens["M500"] = 1

    linhas_9 = [_pipe("9001", "1")]
    for reg, qtd in sorted(contagens.items()):
        linhas_9.append(_pipe("9900", reg, str(qtd)))
    total_geral = sum(contagens.values()) + len(contagens) + 3
    linhas_9.append(_pipe("9990", str(len(linhas_9) + 1)))
    linhas_9.append(_pipe("9999", str(total_geral)))

    todas = linhas_0 + linhas_a + linhas_c + linhas_m + linhas_9
    return "\n".join(todas)


def gerar_todos() -> int:
    total = 0
    for empresa in EMPRESAS:
        for periodo in PERIODOS:
            nfs = gerar_notas(empresa, periodo)
            conteudo = gerar_efd(empresa, periodo, nfs)
            saida = OUTPUT_DIR / empresa["cnpj"]
            saida.mkdir(parents=True, exist_ok=True)
            nome = f"EFD_{empresa['cnpj']}_{periodo.strftime('%Y%m')}.txt"
            (saida / nome).write_text(conteudo, encoding="utf-8")
            total += 1
            print(f"  EFD  | {empresa['nome_fantasia']} | {periodo.strftime('%Y-%m')} | {len(nfs)} NFs")
    return total


if __name__ == "__main__":
    n = gerar_todos()
    print(f"\nTotal: {n} arquivos EFD gerados em {OUTPUT_DIR}")

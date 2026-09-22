"""
Gerador de NF-e (modelo 55) em XML.

Lê os dados de nfe_data.py e grava um arquivo XML por nota fiscal em
data/raw/nfe/<cnpj>/<aaaamm>/NFe<chave>.xml.
"""
import sys
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

sys.path.insert(0, str(Path(__file__).parent))

from config import EMPRESAS, PERIODOS
from nfe_data import gerar_notas

TEMPLATES_DIR = Path(__file__).parent / "templates"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "raw" / "nfe"


def gerar_xmls(nfs: list[dict], diretorio: Path) -> None:
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)), trim_blocks=True, lstrip_blocks=True)
    template = env.get_template("nfe_55.xml.j2")
    diretorio.mkdir(parents=True, exist_ok=True)
    for nf in nfs:
        xml = template.render(**nf)
        (diretorio / f"NFe{nf['chave']}.xml").write_text(xml, encoding="utf-8")


def gerar_todas() -> int:
    total = 0
    for empresa in EMPRESAS:
        for periodo in PERIODOS:
            nfs = gerar_notas(empresa, periodo)
            diretorio = OUTPUT_DIR / empresa["cnpj"] / periodo.strftime("%Y%m")
            gerar_xmls(nfs, diretorio)
            total += len(nfs)
            print(f"  NF-e | {empresa['nome_fantasia']} | {periodo.strftime('%Y-%m')} | {len(nfs)} notas")
    return total


if __name__ == "__main__":
    n = gerar_todas()
    print(f"\nTotal: {n} arquivos XML gerados em {OUTPUT_DIR}")

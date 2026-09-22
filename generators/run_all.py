"""
Orquestra a geração de todos os dados sintéticos.

Uso:
    cd generators
    python run_all.py [--only nfe|sped|efd]
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera dados sintéticos do Nexus Tributário")
    parser.add_argument("--only", choices=["nfe", "sped", "efd"],
                        help="Executa apenas um gerador")
    args = parser.parse_args()

    print("=" * 60)
    print("  Nexus Tributário — Gerador de Dados Sintéticos")
    print("=" * 60)

    t0 = time.perf_counter()

    if args.only in (None, "nfe"):
        print("\n[1/3] Gerando NF-e (XML modelo 55)...")
        from nfe_generator import gerar_todas
        n = gerar_todas()
        print(f"      {n} arquivos XML")

    if args.only in (None, "sped"):
        print("\n[2/3] Gerando SPED Fiscal (EFD ICMS/IPI)...")
        from sped_fiscal_generator import gerar_todos
        n = gerar_todos()
        print(f"      {n} arquivos SPED")

    if args.only in (None, "efd"):
        print("\n[3/3] Gerando EFD Contribuições (PIS/COFINS)...")
        from efd_contribuicoes_generator import gerar_todos
        n = gerar_todos()
        print(f"      {n} arquivos EFD")

    print(f"\nConcluído em {time.perf_counter() - t0:.1f}s")
    print("=" * 60)
    print("Próximo passo: execute o job de ingestão Spark em")
    print("  ingestao/ingest_to_bronze.py  (requer Databricks Connect)")
    print("=" * 60)


if __name__ == "__main__":
    main()

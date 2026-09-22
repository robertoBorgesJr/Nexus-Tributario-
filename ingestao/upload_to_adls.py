"""
Envia os arquivos gerados (data/raw/) para o landing zone no ADLS Gen2.

Alternativa ao AzCopy para contas pessoais Microsoft.
Usa InteractiveBrowserCredential: abre o navegador para autenticação Azure AD
— funciona com contas pessoais (@outlook.com / @hotmail.com).

Pré-requisitos:
    pip install azure-storage-blob azure-identity

Uso:
    python ingestao/upload_to_adls.py [--source nfe|sped|efd|all] [--dry-run]
"""
import argparse
import sys
from pathlib import Path

# ── Configuração (igual ao ingest_to_bronze.py) ───────────────────────────────
STORAGE_ACCOUNT = "stnexustributarioprod"
FILESYSTEM      = "nexus-data"
LANDING_PREFIX  = "landing"

LOCAL_RAW = Path(__file__).parent.parent / "data" / "raw"

FONTES = {
    "nfe":   ("nfe",              "**/*.xml"),
    "sped":  ("sped_fiscal",      "**/*.txt"),
    "efd":   ("efd_contribuicoes","**/*.txt"),
}


def _get_credential():
    """
    Tenta, em ordem:
      1. AzureCliCredential  — funciona se 'az login' já foi executado
      2. InteractiveBrowserCredential — abre o navegador (melhor para conta pessoal)
    """
    from azure.identity import AzureCliCredential, InteractiveBrowserCredential, ChainedTokenCredential
    return ChainedTokenCredential(
        AzureCliCredential(),
        InteractiveBrowserCredential(),
    )


def _service_client(credential):
    from azure.storage.blob import BlobServiceClient
    url = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"
    return BlobServiceClient(account_url=url, credential=credential)


def upload_fonte(client, nome_fonte: str, dry_run: bool) -> int:
    pasta_local, padrao = FONTES[nome_fonte]
    raiz = LOCAL_RAW / pasta_local

    arquivos = list(raiz.glob(padrao))
    if not arquivos:
        print(f"  [{nome_fonte.upper()}] Nenhum arquivo encontrado em {raiz}")
        print(f"         Execute primeiro: python generators/run_all.py --only {nome_fonte}")
        return 0

    container = client.get_container_client(FILESYSTEM)
    enviados = 0

    for arquivo in sorted(arquivos):
        # Monta o caminho no ADLS: landing/nfe/<cnpj>/<aamm>/NFe<chave>.xml
        relativo = arquivo.relative_to(LOCAL_RAW / pasta_local)
        blob_path = f"{LANDING_PREFIX}/{pasta_local}/{relativo.as_posix()}"

        if dry_run:
            print(f"  [dry-run] {arquivo.name}  →  {blob_path}")
        else:
            with arquivo.open("rb") as f:
                container.upload_blob(name=blob_path, data=f, overwrite=True)
            enviados += 1
            print(f"  ✓  {blob_path}")

    return len(arquivos) if dry_run else enviados


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload de dados sintéticos para ADLS Gen2")
    parser.add_argument("--source", choices=["nfe", "sped", "efd", "all"], default="all",
                        help="Qual fonte enviar (padrão: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Mostra os arquivos que seriam enviados sem enviar nada")
    args = parser.parse_args()

    try:
        from azure.storage.blob import BlobServiceClient
        from azure.identity import ChainedTokenCredential
    except ImportError:
        print("Dependências ausentes. Execute:")
        print("  pip install azure-storage-blob azure-identity")
        sys.exit(1)

    print(f"Destino : https://{STORAGE_ACCOUNT}.blob.core.windows.net/{FILESYSTEM}/{LANDING_PREFIX}/")
    if args.dry_run:
        print("Modo    : DRY-RUN (nenhum arquivo será enviado)\n")
    else:
        print("Modo    : UPLOAD REAL\n")

    credential = _get_credential()
    client = _service_client(credential)

    fontes = ["nfe", "sped", "efd"] if args.source == "all" else [args.source]
    total = 0
    for fonte in fontes:
        print(f"[{fonte.upper()}]")
        total += upload_fonte(client, fonte, args.dry_run)

    acao = "encontrados" if args.dry_run else "enviados"
    print(f"\nTotal: {total} arquivos {acao}.")


if __name__ == "__main__":
    main()

"""Script auxiliar para download dos datasets utilizados pelo projeto.

Clona os repositórios públicos com os datasets de pedidos e de pagamentos
para o diretório ``data/input/``. É escrito em Python puro (em vez de
shell/PowerShell) para que funcione da mesma forma em Windows, Linux ou
macOS, mantendo o projeto agnóstico à plataforma onde é executado.

Uso:
    python scripts/download_datasets.py
"""

import pathlib
import subprocess
import sys

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
DATA_INPUT_DIR = REPO_ROOT / "data" / "input"

DATASETS = {
    "datasets-csv-pedidos": "https://github.com/infobarbosa/datasets-csv-pedidos",
    "dataset-json-pagamentos": "https://github.com/infobarbosa/dataset-json-pagamentos",
}


def clonar_repositorio(nome: str, url: str) -> None:
    """Clona o repositório `url` em `data/input/<nome>`, caso ainda não exista."""
    destino = DATA_INPUT_DIR / nome

    if destino.exists() and any(destino.iterdir()):
        print(f"[OK] '{nome}' já existe em '{destino}'. Pulando download.")
        return

    print(f"Clonando '{url}' em '{destino}'...")
    DATA_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.run(
        ["git", "clone", "--depth", "1", url, str(destino)],
        check=True,
    )
    print(f"[OK] '{nome}' baixado com sucesso.")


def main() -> None:
    for nome, url in DATASETS.items():
        try:
            clonar_repositorio(nome, url)
        except subprocess.CalledProcessError as erro:
            print(f"[ERRO] Falha ao clonar '{nome}': {erro}", file=sys.stderr)
            sys.exit(1)

    print("\nDownload dos datasets concluído.")
    print(f"Diretório de entrada: {DATA_INPUT_DIR}")


if __name__ == "__main__":
    main()

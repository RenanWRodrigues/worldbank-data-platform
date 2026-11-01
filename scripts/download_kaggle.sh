#!/usr/bin/env bash
# Baixa o dataset usado pelo notebook do Kaggle
# (https://www.kaggle.com/code/nilaychauhan/etl-pipelines-tutorial-world-bank-datasets)
# Pré-requisito: pip install kaggle  +  ~/.kaggle/kaggle.json (Kaggle > Settings > Create New Token)
set -euo pipefail
DEST="${1:-data/raw}"
# CLI do Kaggle: o do .venv (via `make data`) ou o do PATH
KAGGLE="${KAGGLE:-kaggle}"
mkdir -p "$DEST"
"$KAGGLE" datasets download -d nilaychauhan/world-bank-datasets -p "$DEST" --unzip
# O zip pode vir com subpastas: traz todos os arquivos de dados para a raiz
find "$DEST" -mindepth 2 -type f \( -name '*.csv' -o -name '*.json' -o -name '*.xml' -o -name '*.db' \) \
  -exec mv -n {} "$DEST"/ \;
find "$DEST" -mindepth 1 -type d -empty -delete
echo "Arquivos em $DEST:"; ls -lh "$DEST"

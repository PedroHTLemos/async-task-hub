#!/usr/bin/env sh
# Regenera constraints.txt com todas as dependências (diretas e transitivas),
# instaladas dentro de um python:3.12-slim, igual à imagem da API.
# Não precisa de Python local, só de Docker.
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
IMAGE="python:3.12-slim"
OUT="$ROOT/constraints.txt"
TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

docker run --rm -v "$ROOT":/src:ro -w /src "$IMAGE" sh -c '
    pip install -q --no-cache-dir -r requirements-dev.txt >/dev/null 2>&1 &&
    pip freeze
' | grep -vE '^(-e |\.|/|#|$)' > "$TMP"

{
    echo "# constraints.txt: versões travadas de TODAS as dependências (diretas e transitivas)."
    echo "# Gerado num $IMAGE, igual à imagem Docker da API."
    echo "# Para regenerar: ./scripts/update-constraints.sh (precisa só de Docker)."
    echo "# Revise o diff antes de commitar. Uso: pip install -r requirements.txt -c constraints.txt"
    cat "$TMP"
} > "$OUT"

echo "constraints.txt atualizado: $(grep -cvE '^#' "$OUT") pacotes."

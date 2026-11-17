#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

pdflatex -interaction=nonstopmode main.tex
pdflatex -interaction=nonstopmode main.tex

cp main.pdf relatorio.pdf
echo "Successfully generated report/relatorio.pdf"

#! /bin/bash

set -e
mkdir -p build
latexmk -pdf -outdir=build main.tex
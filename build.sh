#!/bin/bash

set -e

command -v cmake >/dev/null 2>&1 || { echo "ERROR: cmake not found. Install: sudo apt install cmake"; exit 1; }


CUR_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="${CUR_DIR}/build"

echo "Configuring build..."

mkdir -p "${BUILD_DIR}"
cmake -B "${BUILD_DIR}" -S .

echo "Building..."

cmake --build "${BUILD_DIR}" -j$(nproc)

echo "=== Build complete ==="

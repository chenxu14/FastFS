#!/bin/bash

set -e

FIO_DIR="${1}"

echo "[INFO] Compile FIO $FIO_DIR"

if [ ! -d "$FIO_DIR" ]; then
    echo "[ERROR] $FIO_DIR NOT Exist."
    exit 1
fi

if [ -f "$FIO_DIR/fio" ]; then
    echo "[INFO] FIO Complied，Skiped."
    exit 0
fi

cd "$FIO_DIR"

echo "[INFO] Configure FIO..."
./configure

echo "[INFO] Build FIO..."
make -j2

cd ..

+echo "[INFO] FIO Build Complete."

#!/bin/bash

set -e

SPDK_DIR="${1}"
PATCH_FILE="${2:-spdk.patch}"

echo "[INFO] Build SPDK $SPDK_DIR"
echo "[INFO] Patch File : $PATCH_FILE"

if [ ! -d "$SPDK_DIR" ]; then
    echo "[ERROR] $SPDK_DIR NOT Exist."
    exit 1
fi

if [ -f "$SPDK_DIR/build/lib/libspdk_event.so" ] && [ -f "$SPDK_DIR/dpdk/build/lib/librte_eal.so" ]; then
    echo "[INFO] SPDK Compiled，Skipped."
    exit 0
fi

if [ -f "$PATCH_FILE" ]; then
    echo "[INFO] Apply $PATCH_FILE"
    cd "$SPDK_DIR"

    if git apply --check $PATCH_FILE 2>/dev/null; then
        if git apply $PATCH_FILE; then
            echo "[INFO] Apply Patch Success."
        else
            echo "[ERROR] Apply Patch Failed."
            exit 1
        fi
    else
        if grep -q "struct spdk_bit_pool {" include/spdk/bit_pool.h 2>/dev/null; then
            echo "[INFO] Patch Applied，Skipped."
        else
            echo "[WARNNING] Can't Apply SPDK Patch."
        fi
    fi
    cd ..
else
    echo "[WARNNING] $PATCH_FILE NOT Exist，Skipped."
fi

cd "$SPDK_DIR"
git submodule update --init
# ./scripts/pkgdep.sh
./configure --disable-tests --disable-unit-tests --disable-apps --disable-examples --with-shared

echo "[INFO] Compile SPDK..."
make -j2

cd ..

echo "[INFO] SPDK Build Success."

#!/bin/bash

# SPDK build script
# Used to automatically compile SPDK submodule in CMake configuration

set -e

SPDK_DIR="${1:-third_party/spdk}"
PATCH_FILE="${2:-spdk.patch}"
BUILD_DIR="${3:-build}"

echo "=== Building SPDK ==="
echo "SPDK directory: $SPDK_DIR"
echo "Patch file: $PATCH_FILE"
echo "Build directory: $BUILD_DIR"

# Check SPDK directory
if [ ! -d "$SPDK_DIR" ]; then
    echo "Error: SPDK directory $SPDK_DIR does not exist"
    echo "Please run first: git submodule update --init"
    exit 1
fi

# Check if already compiled
if [ -f "$SPDK_DIR/build/lib/libspdk_event.a" ] && [ -f "$SPDK_DIR/dpdk/build/lib/librte_eal.a" ]; then
    echo "Detected that SPDK is already compiled, skipping compilation step"
    exit 0
fi

# Apply patch (if exists)
if [ -f "$PATCH_FILE" ]; then
    echo "Applying patch: $PATCH_FILE"
    cd "$SPDK_DIR"
    
    # Check if patch is already applied
    if git apply --check "$PATCH_FILE" 2>/dev/null; then
        # Patch can be applied, meaning it hasn't been applied yet
        echo "Detected that patch has not been applied yet, applying now..."
        if git apply "$PATCH_FILE"; then
            echo "Patch applied successfully"
        else
            echo "Error: Patch application failed"
            exit 1
        fi
    else
        # Check if patch has already been applied (by checking key file content)
        if grep -q "struct spdk_bit_pool {" include/spdk/bit_pool.h 2>/dev/null; then
            echo "Detected that patch has been applied, skipping patch application step"
        else
            echo "Warning: Patch cannot be applied and no applied state detected"
            echo "Possible reasons: SPDK version incompatible or files have been modified"
            echo "Attempting to continue compilation..."
        fi
    fi
    cd ..
else
    echo "Warning: Patch file $PATCH_FILE does not exist, skipping patch application"
fi

# Configure SPDK
cd "$SPDK_DIR"
git submodule update --init
./scripts/pkgdep.sh
echo "Configuring SPDK..."
./configure --disable-tests --disable-unit-tests --disable-apps --disable-examples --with-shared

# Compile SPDK
echo "Compiling SPDK..."
make -j2

# Return to project root directory
cd ..

echo "=== SPDK compilation completed ==="
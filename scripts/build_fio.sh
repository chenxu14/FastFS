#!/bin/bash

set -e

FIO_DIR="${1}"
BUILD_DIR="${2:-build}"

echo "=== compile FIO ==="
echo "FIO dir: $FIO_DIR"
echo "build dir: $BUILD_DIR"

if [ ! -d "$FIO_DIR" ]; then
    echo "Error: FIO directory $FIO_DIR does not exist"
    exit 1
fi

# Check if FIO is already compiled
if [ -f "$FIO_DIR/fio" ]; then
    echo "Detected that FIO is already compiled, skipping compilation step"
    exit 0
fi

cd "$FIO_DIR"

# Configure FIO
echo "Configuring FIO..."
./configure

# Compile FIO
echo "Compiling FIO..."
make -j2

# Return to project root directory
cd ..

echo "=== FIO compilation completed ==="
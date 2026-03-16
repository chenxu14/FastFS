#!/bin/bash

# FastFS CMake build script

set -e

# Default configuration
ENABLE_FIO_PLUGIN=0
BUILD_SPDK=1
BUILD_FIO=1
FIO_VERSION="fio-3.39"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --enable-fio)
            ENABLE_FIO=1
            shift
            ;;
        --disable-spdk-build)
            BUILD_SPDK=0
            shift
            ;;
        --disable-fio-build)
            BUILD_FIO=0
            shift
            ;;
        --fio-version)
            FIO_VERSION="$2"
            shift 2
            ;;
        --disable-fuse)
            DISABLE_FUSE=1
            shift
            ;;
        --disable-tests)
            DISABLE_TESTS=1
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Check SPDK submodule (now in third_party/spdk)
if [ ! -d "third_party/spdk" ] || [ ! -f "third_party/spdk/mk/spdk.common.mk" ]; then
    echo "Error: SPDK submodule not found. Please run:"
    echo "  git submodule update --init"
    exit 1
fi

# Create build directory
BUILD_DIR="build"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# Build CMake configuration command
CMAKE_CMD="cmake .."

if [[ $ENABLE_FIO -eq 1 ]]; then
    CMAKE_CMD+=" -DENABLE_FIO=ON"
    CMAKE_CMD+=" -DFIO_VERSION=$FIO_VERSION"
fi

if [[ $BUILD_SPDK -eq 0 ]]; then
    CMAKE_CMD+=" -DBUILD_SPDK=OFF"
fi

if [[ $BUILD_FIO -eq 0 ]]; then
    CMAKE_CMD+=" -DBUILD_FIO=OFF"
fi

if [[ -n "$DISABLE_FUSE" ]]; then
    CMAKE_CMD+=" -DENABLE_FUSE=OFF"
fi

if [[ -n "$DISABLE_TESTS" ]]; then
    CMAKE_CMD+=" -DENABLE_TESTS=OFF"
fi

# Run CMake configuration
echo "Running CMake configuration..."
eval "$CMAKE_CMD"

# Build project
echo "Building FastFS..."
make -j2

echo "Build completed successfully!"
echo "Binaries are located in: $BUILD_DIR/bin"
echo "Libraries are located in: $BUILD_DIR/lib64"
#!/bin/bash

# FastFS CMake 编译脚本

set -e

# 默认配置
ENABLE_FIO_PLUGIN=0
BUILD_SPDK=1
BUILD_FIO=1
FIO_VERSION="fio-3.39"

# 解析命令行参数
while [[ $# -gt 0 ]]; do
    case $1 in
        --enable-fio-plugin)
            ENABLE_FIO_PLUGIN=1
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

# 检查 SPDK submodule (现在在 third_party/spdk)
if [ ! -d "third_party/spdk" ] || [ ! -f "third_party/spdk/mk/spdk.common.mk" ]; then
    echo "Error: SPDK submodule not found. Please run:"
    echo "  git submodule update --init"
    exit 1
fi

# 创建构建目录
BUILD_DIR="build"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

# 构建 CMake 配置命令
CMAKE_CMD="cmake .."

if [[ $ENABLE_FIO_PLUGIN -eq 1 ]]; then
    CMAKE_CMD+=" -DENABLE_FIO_PLUGIN=ON"
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

# 运行 CMake 配置
echo "Running CMake configuration..."
eval "$CMAKE_CMD"

# 编译项目
echo "Building FastFS..."
make -j16

echo "Build completed successfully!"
echo "Binaries are located in: $BUILD_DIR/bin"
echo "Libraries are located in: $BUILD_DIR/lib"
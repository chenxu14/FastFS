#!/bin/bash

# FIO 编译脚本
# 用于在启用 FIO 插件时编译 FIO

set -e

FIO_DIR="${1}"
BUILD_DIR="${2:-build}"

echo "=== 编译 FIO ==="
echo "FIO 目录: $FIO_DIR"
echo "构建目录: $BUILD_DIR"

if [ ! -d "$FIO_DIR" ]; then
    echo "错误: FIO 目录 $FIO_DIR 不存在"
    exit 1
fi

# 检查是否已经编译完成
if [ -f "$FIO_DIR/fio" ]; then
    echo "检测到 FIO 已经编译完成，跳过编译步骤"
    exit 0
fi

cd "$FIO_DIR"

# 配置 FIO
echo "配置 FIO..."
./configure

# 编译 FIO
echo "编译 FIO..."
make -j16

# 返回项目根目录
cd ..

echo "=== FIO 编译完成 ==="
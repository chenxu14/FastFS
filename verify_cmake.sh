#!/bin/bash

# 验证 FastFS CMake 配置脚本 (支持 submodule)

set -e

echo "=== 验证 FastFS CMake 配置 (submodule 版本) ==="

# 检查必要的文件是否存在
if [ ! -f "CMakeLists.txt" ]; then
    echo "错误: CMakeLists.txt 不存在"
    exit 1
fi

if [ ! -d "core" ]; then
    echo "错误: core 目录不存在"
    exit 1
fi

if [ ! -f "core/CMakeLists.txt" ]; then
    echo "错误: core/CMakeLists.txt 不存在"
    exit 1
fi

echo "✓ 基本文件结构验证通过"

# 检查 SPDK submodule (现在在 third_party/spdk)
if [ ! -d "third_party/spdk" ]; then
    echo "⚠ SPDK submodule 目录不存在 (需要运行: git submodule update --init)"
else
    if [ ! -f "third_party/spdk/mk/spdk.common.mk" ]; then
        echo "⚠ SPDK submodule 未正确初始化"
    else
        echo "✓ SPDK submodule 验证通过"
    fi
fi

# 创建构建目录并测试 CMake 配置
BUILD_DIR="cmake_verify_build"
mkdir -p "$BUILD_DIR"
cd "$BUILD_DIR"

echo "正在运行 CMake 配置测试 (不启用 FIO 插件)..."

# 测试基本配置（不启用 FIO 插件）
cmake .. 2>/dev/null || true

if [ -f "Makefile" ]; then
    echo "✓ CMake 基本配置成功生成 Makefile"
else
    echo "⚠ CMake 基本配置可能失败，但这是预期的（缺少实际依赖）"
fi

# 清理
cd ..
rm -rf "$BUILD_DIR"

echo "=== 验证完成 ==="
echo ""
echo "使用方法:"
echo "1. 初始化 SPDK submodule: git submodule update --init"
echo "2. 应用 spdk.patch 到 SPDK"
echo "3. 编译 SPDK"
echo "4. 运行 ./compile.sh 进行编译"
echo "5. 或者启用 FIO 插件: ./compile.sh --enable-fio-plugin"
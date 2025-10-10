#!/bin/bash

# SPDK 编译脚本
# 用于在 CMake 配置中自动编译 SPDK submodule

set -e

SPDK_DIR="${1:-third_party/spdk}"
PATCH_FILE="${2:-spdk.patch}"
BUILD_DIR="${3:-build}"

echo "=== 编译 SPDK ==="
echo "SPDK 目录: $SPDK_DIR"
echo "补丁文件: $PATCH_FILE"
echo "构建目录: $BUILD_DIR"

# 检查 SPDK 目录
if [ ! -d "$SPDK_DIR" ]; then
    echo "错误: SPDK 目录 $SPDK_DIR 不存在"
    echo "请先运行: git submodule update --init"
    exit 1
fi

# 检查是否已经编译完成
if [ -f "$SPDK_DIR/build/lib/libspdk_event.a" ] && [ -f "$SPDK_DIR/dpdk/build/lib/librte_eal.a" ]; then
    echo "检测到 SPDK 已经编译完成，跳过编译步骤"
    exit 0
fi

# 应用补丁（如果存在）
if [ -f "$PATCH_FILE" ]; then
    echo "应用补丁: $PATCH_FILE"
    cd "$SPDK_DIR"
    
    # 检查补丁是否已经应用
    if git apply --check "../../$PATCH_FILE" 2>/dev/null; then
        # 补丁可以应用，说明尚未应用
        echo "检测到补丁尚未应用，正在应用..."
        if git apply "../../$PATCH_FILE"; then
            echo "补丁应用成功"
        else
            echo "错误: 补丁应用失败"
            exit 1
        fi
    else
        # 检查是否已经应用了补丁（通过检查关键文件内容）
        if grep -q "struct spdk_bit_pool {" include/spdk/bit_pool.h 2>/dev/null; then
            echo "检测到补丁已应用，跳过补丁应用步骤"
        else
            echo "警告: 补丁无法应用且未检测到已应用状态"
            echo "可能的原因: SPDK 版本不兼容或文件已被修改"
            echo "尝试继续编译..."
        fi
    fi
    cd ..
else
    echo "警告: 补丁文件 $PATCH_FILE 不存在，跳过补丁应用"
fi

# 配置 SPDK
cd "$SPDK_DIR"
git submodule update --init
./scripts/pkgdep.sh
echo "配置 SPDK..."
./configure --disable-tests --disable-unit-tests --disable-apps --disable-examples

# 编译 SPDK
echo "编译 SPDK..."
make -j16

# 返回项目根目录
cd ..

echo "=== SPDK 编译完成 ==="
cmake_minimum_required(VERSION 3.16)
project(FastFS VERSION 1.0 LANGUAGES CXX C)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# 查找 Threads 库
find_package(Threads REQUIRED)

# 设置输出目录
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 脚本目录
set(SCRIPTS_DIR ${CMAKE_SOURCE_DIR}/scripts)

# SPDK 作为 submodule 的路径 (现在在 third_party/spdk)
set(SPDK_SUBMODULE_DIR ${CMAKE_SOURCE_DIR}/third_party/spdk)
set(SPDK_BUILD_DIR ${SPDK_SUBMODULE_DIR}/build)

# deps 目录用于临时依赖
set(DEPS_DIR ${CMAKE_SOURCE_DIR}/deps)

# 编译选项
option(ENABLE_FIO_PLUGIN "Enable FIO plugin support" ON)
set(FIO_VERSION "fio-3.39" CACHE STRING "FIO version to download")
option(ENABLE_FUSE "Enable FUSE support" ON)
option(ENABLE_TESTS "Enable unit tests" ON)
option(BUILD_SPDK "Automatically build SPDK submodule" ON)
option(BUILD_FIO "Automatically build FIO when plugin enabled" ON)

# 设置编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")

# 检查 SPDK submodule 是否存在
if(NOT EXISTS ${SPDK_SUBMODULE_DIR}/mk/spdk.common.mk)
    message(FATAL_ERROR "SPDK submodule not found at ${SPDK_SUBMODULE_DIR}. Please run: git submodule update --init")
endif()

# SPDK 自动编译逻辑
if(BUILD_SPDK)
    # 创建 SPDK 编译命令
    set(SPDK_BUILD_COMMAND ${CMAKE_SOURCE_DIR}/scripts/build_spdk.sh ${SPDK_SUBMODULE_DIR} ${CMAKE_SOURCE_DIR}/spdk.patch)
    
    # 添加自定义目标来编译 SPDK
    add_custom_target(build_spdk
        COMMAND ${SPDK_BUILD_COMMAND}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building SPDK submodule"
    )
    
    # 确保 SPDK 在使用前已编译
    set(SPDK_DEPENDENCY build_spdk)
else()
    set(SPDK_DEPENDENCY "")
endif()

# FIO 下载和构建逻辑（仅在启用 FIO 插件时）
if(ENABLE_FIO_PLUGIN)
    include(FetchContent)
    
    # 下载 FIO 到 deps 目录
    FetchContent_Declare(
        fio
        URL https://github.com/axboe/fio/archive/refs/tags/${FIO_VERSION}.tar.gz
        DOWNLOAD_DIR ${DEPS_DIR}
        SOURCE_DIR ${DEPS_DIR}/${FIO_VERSION}
    )
    
    FetchContent_MakeAvailable(fio)
    
    # 设置 FIO 源码路径
    set(FIO_SRC_DIR ${DEPS_DIR}/${FIO_VERSION})
    set(FIO_BUILD_DIR ${FIO_SRC_DIR})
    
    # FIO 自动编译逻辑
    if(BUILD_FIO)
        set(FIO_BUILD_COMMAND ${CMAKE_SOURCE_DIR}/scripts/build_fio.sh ${FIO_SRC_DIR})
        
        add_custom_target(build_fio
            COMMAND ${FIO_BUILD_COMMAND}
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            COMMENT "Building FIO"
        )
        
        set(FIO_DEPENDENCY build_fio)
    else()
        set(FIO_DEPENDENCY "")
    endif()
endif()

# 添加子目录
add_subdirectory(core)
add_subdirectory(tools)
add_subdirectory(fuse)
add_subdirectory(test)
add_subdirectory(bench)

# 安装规则
install(DIRECTORY ${CMAKE_SOURCE_DIR}/bench/ DESTINATION share/fastfs/bench
        FILES_MATCHING PATTERN "*.json")

message(STATUS "FastFS configuration:")
message(STATUS "  SPDK_SUBMODULE_DIR: ${SPDK_SUBMODULE_DIR}")
message(STATUS "  DEPS_DIR: ${DEPS_DIR}")
message(STATUS "  BUILD_SPDK: ${BUILD_SPDK}")
message(STATUS "  ENABLE_FIO_PLUGIN: ${ENABLE_FIO_PLUGIN}")
message(STATUS "  BUILD_FIO: ${BUILD_FIO}")
message(STATUS "  ENABLE_FUSE: ${ENABLE_FUSE}")
message(STATUS "  ENABLE_TESTS: ${ENABLE_TESTS}")
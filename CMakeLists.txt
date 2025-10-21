cmake_minimum_required(VERSION 3.16)
project(FastFS VERSION 1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib64)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib64)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(SCRIPTS_DIR ${CMAKE_SOURCE_DIR}/scripts)

# SPDK configuration
if(NOT DEFINED SPDK_ROOT_DIR)
    set(SPDK_ROOT_DIR ${CMAKE_SOURCE_DIR}/third_party/spdk)
    set(SPDK_AUTO_DOWNLOAD ON)
else()
    set(SPDK_AUTO_DOWNLOAD OFF)
endif()
set(SPDK_BUILD_DIR ${SPDK_ROOT_DIR}/build)

set(DEPS_DIR ${CMAKE_SOURCE_DIR}/deps)

option(ENABLE_FIO "Enable FIO support" ON)
set(FIO_VERSION "fio-3.39" CACHE STRING "FIO version to download")
option(ENABLE_FUSE "Enable FUSE support" ON)
option(ENABLE_TESTS "Enable unit tests" ON)
option(BUILD_SPDK "Automatically build SPDK submodule" ON)
option(BUILD_FIO "Automatically build FIO when enabled" ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")

# SPDK setup and build logic
if(SPDK_AUTO_DOWNLOAD)
    if(NOT EXISTS ${SPDK_ROOT_DIR}/mk/spdk.common.mk)
        message(FATAL_ERROR "SPDK submodule not found at ${SPDK_ROOT_DIR}. Please run: git submodule update --init")
    endif()
endif()

if(BUILD_SPDK)
    set(SPDK_BUILD_COMMAND ${CMAKE_SOURCE_DIR}/scripts/build_spdk.sh ${SPDK_ROOT_DIR} ${CMAKE_SOURCE_DIR}/spdk.patch)
    
    add_custom_target(build_spdk
        COMMAND ${SPDK_BUILD_COMMAND}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building SPDK"
    )
    
    set(SPDK_DEPENDENCY build_spdk)
else()
    set(SPDK_DEPENDENCY "")
endif()

# FIO configuration and build logic
if(ENABLE_FIO)
    if(NOT DEFINED FIO_ROOT_DIR)
        include(FetchContent)
        
        FetchContent_Declare(
            fio
            URL https://github.com/axboe/fio/archive/refs/tags/${FIO_VERSION}.tar.gz
            URL_HASH SHA256=e2f4ff137061b44ceb83a55eb9ca8856fe188db6d9b00cb59f8629c9162afe0a
            DOWNLOAD_DIR ${DEPS_DIR}
            SOURCE_DIR ${DEPS_DIR}/${FIO_VERSION}
        )
        
        FetchContent_MakeAvailable(fio)
        
        set(FIO_ROOT_DIR ${DEPS_DIR}/${FIO_VERSION})
        set(FIO_AUTO_DOWNLOAD ON)
    else()
        set(FIO_AUTO_DOWNLOAD OFF)
    endif()
    
    set(FIO_SRC_DIR ${FIO_ROOT_DIR})
    set(FIO_BUILD_DIR ${FIO_ROOT_DIR})
    
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

# Core library (always built)
add_subdirectory(core)
add_subdirectory(tools)

# Optional components with centralized condition checks
if(ENABLE_FUSE)
    add_subdirectory(fuse)
endif()

if(ENABLE_TESTS)
    add_subdirectory(test)
endif()

# Bench directory (contains both general benchmarks and FIO plugin)
add_subdirectory(bench)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/bench/ DESTINATION share/fastfs/bench
        FILES_MATCHING PATTERN "*.json")

message(STATUS "FastFS configuration:")
message(STATUS "  SPDK_ROOT_DIR: ${SPDK_ROOT_DIR}")
message(STATUS "  SPDK_AUTO_DOWNLOAD: ${SPDK_AUTO_DOWNLOAD}")
message(STATUS "  DEPS_DIR: ${DEPS_DIR}")
message(STATUS "  BUILD_SPDK: ${BUILD_SPDK}")
message(STATUS "  ENABLE_FIO: ${ENABLE_FIO}")
if(ENABLE_FIO)
    message(STATUS "  FIO_ROOT_DIR: ${FIO_ROOT_DIR}")
    message(STATUS "  FIO_AUTO_DOWNLOAD: ${FIO_AUTO_DOWNLOAD}")
endif()
message(STATUS "  BUILD_FIO: ${BUILD_FIO}")
message(STATUS "  ENABLE_FUSE: ${ENABLE_FUSE}")
message(STATUS "  ENABLE_TESTS: ${ENABLE_TESTS}")
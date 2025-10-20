#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

cmake_minimum_required(VERSION 3.16)
project(FastFS VERSION 1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(SCRIPTS_DIR ${CMAKE_SOURCE_DIR}/scripts)
set(DEPS_DIR ${CMAKE_SOURCE_DIR}/deps)

set(SPDK_ROOT_DIR "" CACHE STRING "SPDK Root Dir")
set(SPDK_VERSION "v25.09" CACHE STRING "SPDK version to download")

set(FIO_ROOT_DIR "" CACHE STRING "FIO Root Dir")
set(FIO_VERSION "fio-3.39" CACHE STRING "FIO version to download")

option(ENABLE_FUSE "Enable FUSE support" ON)
option(ENABLE_FIO "Enable FIO support" ON)
option(ENABLE_TESTS "Enable unit tests" ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")

if(NOT SPDK_ROOT_DIR)
    message(STATUS "Fetching SPDK...")
    include(FetchContent)
    FetchContent_Declare(
        spdk
        GIT_REPOSITORY https://github.com/spdk/spdk.git
        GIT_TAG ${SPDK_VERSION}
        GIT_SHALLOW TRUE
        GIT_PROGRESS TRUE
        SOURCE_DIR ${DEPS_DIR}/spdk-${SPDK_VERSION}
    )
    FetchContent_MakeAvailable(spdk)

    set(SPDK_ROOT_DIR ${DEPS_DIR}/spdk-${SPDK_VERSION})
    set(SPDK_BUILD_COMMAND ${CMAKE_SOURCE_DIR}/scripts/build_spdk.sh ${SPDK_ROOT_DIR} ${CMAKE_SOURCE_DIR}/spdk.patch)
    add_custom_target(build_spdk
        COMMAND ${SPDK_BUILD_COMMAND}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building SPDK"
    )
endif()

if(ENABLE_FIO AND NOT FIO_ROOT_DIR)
    message(STATUS "Fetching FIO...")
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
    set(FIO_BUILD_COMMAND ${CMAKE_SOURCE_DIR}/scripts/build_fio.sh ${FIO_ROOT_DIR})
    add_custom_target(build_fio
        COMMAND ${FIO_BUILD_COMMAND}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Building FIO"
    )
endif()

add_subdirectory(core)
add_subdirectory(tools)
if(ENABLE_FUSE)
    add_subdirectory(fuse)
endif()
if(ENABLE_TESTS)
    add_subdirectory(test)
endif()
add_subdirectory(bench)

install(DIRECTORY ${CMAKE_SOURCE_DIR}/bench/ DESTINATION share/fastfs/bench
        FILES_MATCHING PATTERN "*.json")

message(STATUS "FastFS Configuration:")
message(STATUS "  SPDK_ROOT_DIR: ${SPDK_ROOT_DIR}")
message(STATUS "  FIO_ROOT_DIR: ${FIO_ROOT_DIR}")
message(STATUS "  ENABLE_FIO: ${ENABLE_FIO}")
message(STATUS "  ENABLE_FUSE: ${ENABLE_FUSE}")
message(STATUS "  ENABLE_TESTS: ${ENABLE_TESTS}")

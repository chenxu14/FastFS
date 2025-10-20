#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

set(TARGET_NAME fastfs_tools)
set(SOURCES fastfs_tools.cpp)

add_executable(${TARGET_NAME} ${SOURCES})

target_include_directories(${TARGET_NAME} 
    PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${SPDK_ROOT_DIR}/build/include
        ${SPDK_ROOT_DIR}/dpdk/build/include
)

target_link_libraries(${TARGET_NAME} 
    PRIVATE
        fastfs
        Threads::Threads
)

set_target_properties(${TARGET_NAME} PROPERTIES
    INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib64:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
    BUILD_RPATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
)

install(TARGETS ${TARGET_NAME} DESTINATION bin)

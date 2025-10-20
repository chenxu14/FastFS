#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

set(TARGET_NAME fastfs_fio)
set(SOURCES ${TARGET_NAME}.cpp)

add_library(${TARGET_NAME} SHARED ${SOURCES})

target_include_directories(${TARGET_NAME} 
    PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${FIO_ROOT_DIR}
        ${SPDK_ROOT_DIR}/build/include
        ${SPDK_ROOT_DIR}/dpdk/build/include
)

target_link_libraries(${TARGET_NAME} 
    PRIVATE
        fastfs
        Threads::Threads
)

if(FIO_BUILD_COMMAND)
    add_dependencies(${TARGET_NAME} build_fio)
endif()

target_compile_options(${TARGET_NAME} PRIVATE
    -fpermissive
    -O3
    -rdynamic
    -w
)

set_target_properties(${TARGET_NAME} PROPERTIES
    PREFIX ""
    SUFFIX ".so"
    INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib64:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
    BUILD_RPATH "${CMAKE_LIBRARY_OUTPUT_DIRECTORY}:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
)

install(TARGETS ${TARGET_NAME} DESTINATION lib64)

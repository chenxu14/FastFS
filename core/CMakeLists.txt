#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

set(TARGET_NAME fastfs)

file(GLOB SOURCES "*.cpp")
file(GLOB HEADERS "*.h")

add_library(${TARGET_NAME} SHARED ${SOURCES} ${HEADERS})

target_include_directories(${TARGET_NAME} 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${SPDK_ROOT_DIR}/build/include
        ${SPDK_ROOT_DIR}/dpdk/build/include
)

execute_process(
    COMMAND bash -c "PKG_CONFIG_PATH=${SPDK_ROOT_DIR}/build/lib/pkgconfig pkg-config --libs spdk_env_dpdk spdk_event_bdev spdk_event"
    OUTPUT_VARIABLE SPDK_LIB_LIST
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE result_code
    ERROR_QUIET
)

if(NOT result_code EQUAL 0)
    message(FATAL_ERROR "Unable to retrieve SPDK library information.")
endif()

target_link_libraries(${TARGET_NAME} 
    PUBLIC
        -Wl,--no-as-needed ${SPDK_LIB_LIST} -lrte_timer -lrte_meter -Wl,--as-needed
)

if(SPDK_BUILD_COMMAND)
    add_dependencies(${TARGET_NAME} build_spdk)
endif()

set_target_properties(${TARGET_NAME} PROPERTIES
    INSTALL_RPATH "${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
    BUILD_RPATH "${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
)

install(TARGETS ${TARGET_NAME} DESTINATION lib64)
install(FILES ${HEADERS} DESTINATION include/fastfs)

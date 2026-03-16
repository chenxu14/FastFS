set(TARGET_NAME fastfs_ut)

set(SOURCES fastfs_ut.cpp)

add_executable(${TARGET_NAME} ${SOURCES})

target_include_directories(${TARGET_NAME} 
    PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${SPDK_ROOT_DIR}/include
        ${SPDK_ROOT_DIR}/build/include
        ${SPDK_ROOT_DIR}/dpdk/build/include
        ${SPDK_ROOT_DIR}/test
        ${CUNIT_INCLUDE_DIRS}
)

target_link_libraries(${TARGET_NAME} 
    PRIVATE
        fastfs
        ${CUNIT_LIBRARIES}
        ${SPDK_ROOT_DIR}/build/lib/libspdk_event.a
        ${SPDK_ROOT_DIR}/build/lib/libspdk_event_bdev.a
        Threads::Threads
)

target_compile_options(${TARGET_NAME} PRIVATE 
    ${CUNIT_CFLAGS_OTHER}
    -ffunction-sections
    -DSPDK_UNIT_TEST=1
)

target_link_options(${TARGET_NAME} PRIVATE
    -Wl,--gc-sections
)

set_target_properties(${TARGET_NAME} PROPERTIES
    INSTALL_RPATH "${CMAKE_SOURCE_DIR}/core:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
    BUILD_RPATH "${CMAKE_BINARY_DIR}/lib:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
)
set(TARGET_NAME fastfs_ut)

# 源文件
set(SOURCES fastfs_ut.cpp)

# 创建可执行文件
add_executable(${TARGET_NAME} ${SOURCES})

# 包含目录
target_include_directories(${TARGET_NAME} 
    PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${SPDK_SUBMODULE_DIR}/include
        ${SPDK_SUBMODULE_DIR}/build/include
        ${SPDK_SUBMODULE_DIR}/dpdk/build/include
        ${SPDK_SUBMODULE_DIR}/test
        ${CUNIT_INCLUDE_DIRS}
)

# 链接库
target_link_libraries(${TARGET_NAME} 
    PRIVATE
        fastfs
        ${CUNIT_LIBRARIES}
        ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event.a
        ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_bdev.a
        Threads::Threads
)

# 设置编译选项
target_compile_options(${TARGET_NAME} PRIVATE 
    ${CUNIT_CFLAGS_OTHER}
    -ffunction-sections
    -DSPDK_UNIT_TEST=1
)

# 设置链接选项
target_link_options(${TARGET_NAME} PRIVATE
    -Wl,--gc-sections
)

# 设置 RPATH
set_target_properties(${TARGET_NAME} PROPERTIES
    INSTALL_RPATH "${CMAKE_SOURCE_DIR}/core:${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
    BUILD_RPATH "${CMAKE_BINARY_DIR}/lib:${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
)
set(TARGET_NAME fastfs_tools)

# 源文件
set(SOURCES fastfs_tools.cpp)

# 创建可执行文件
add_executable(${TARGET_NAME} ${SOURCES})

# 包含目录
target_include_directories(${TARGET_NAME} 
    PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${SPDK_SUBMODULE_DIR}/include
        ${SPDK_SUBMODULE_DIR}/build/include
        ${SPDK_SUBMODULE_DIR}/dpdk/build/include
)

# 链接 fastfs 库和 SPDK 静态库
target_link_libraries(${TARGET_NAME} 
    PRIVATE
        fastfs
        ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event.a
        ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_bdev.a
        Threads::Threads
)

# 设置 RPATH
set_target_properties(${TARGET_NAME} PROPERTIES
    INSTALL_RPATH "${CMAKE_SOURCE_DIR}/core:${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
    BUILD_RPATH "${CMAKE_BINARY_DIR}/lib:${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
)

# 安装规则
install(TARGETS ${TARGET_NAME} DESTINATION bin)
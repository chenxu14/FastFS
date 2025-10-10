set(TARGET_NAME fastfs)

# 源文件
file(GLOB SOURCES "*.cpp")
file(GLOB HEADERS "*.h")

# 创建共享库
add_library(${TARGET_NAME} SHARED ${SOURCES} ${HEADERS})

# 包含目录
target_include_directories(${TARGET_NAME} 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${SPDK_SUBMODULE_DIR}/include
        ${SPDK_SUBMODULE_DIR}/build/include
        ${SPDK_SUBMODULE_DIR}/dpdk/build/include
)

# 自动发现所有 SPDK 静态库（按依赖顺序）
file(GLOB SPDK_LIBS "${SPDK_SUBMODULE_DIR}/build/lib/libspdk_*.a")

# 关键的 SPDK 库顺序（基于 Makefile 中的 SPDK_LIB_LIST）
# 这些库必须按照依赖顺序链接
set(SPDK_CORE_LIBS
    # Event 子系统（最上层）
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_bdev.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_accel.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_vmd.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_sock.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_iobuf.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_keyring.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event.a
    
    # 所有 bdev 模块
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_malloc.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_null.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_nvme.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_passthru.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_lvol.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_raid.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_error.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_gpt.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_split.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_delay.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_zone_block.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_aio.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_ftl.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev_virtio.a
    
    # Blob 和 LVol
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_blob_bdev.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_lvol.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_blob.a
    
    # FTL
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_ftl.a
    
    # VMD 和 NVMe
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_vmd.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_nvme.a
    
    # Virtio
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_virtio.a
    
    # VFIO User
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_vfio_user.a
    
    # Accel 子系统
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_accel_error.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_accel_ioat.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_accel.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_ioat.a
    
    # Sock 子系统
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_sock_posix.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_sock.a
    
    # Keyring 子系统
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_keyring_file.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_keyring_linux.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_keyring.a
    
    # 核心 BDEV 层
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_bdev.a
    
    # Notify
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_notify.a
    
    # 基础库
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_init.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_thread.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_trace.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_rpc.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_jsonrpc.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_json.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_util.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_log.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_dma.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_conf.a
    
    # 环境抽象层
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_env_dpdk.a
    ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_env_dpdk_rpc.a
)

# 过滤出实际存在的库
set(SPDK_LIBS_FILTERED)
foreach(lib ${SPDK_CORE_LIBS})
    if(EXISTS ${lib})
        list(APPEND SPDK_LIBS_FILTERED ${lib})
    endif()
endforeach()

# 自动发现所有 DPDK 静态库
file(GLOB DPDK_LIBS "${SPDK_SUBMODULE_DIR}/dpdk/build/lib/librte_*.a")

# 链接库（使用 --whole-archive 确保所有符号都被包含）
target_link_libraries(${TARGET_NAME} 
    PRIVATE
        # 使用 --whole-archive 包含所有 SPDK 库
        -Wl,--whole-archive
        ${SPDK_LIBS_FILTERED}
        -Wl,--no-whole-archive
        
        # DPDK 库
        ${DPDK_LIBS}
        
        # ISA-L 库（SPDK 内置，提供压缩、加密、CRC 等功能）
        ${SPDK_SUBMODULE_DIR}/isa-l/.libs/libisal.a
        ${SPDK_SUBMODULE_DIR}/isa-l-crypto/.libs/libisal_crypto.a
        
        # SPDK 依赖的外部库
        ssl
        crypto
        uuid
        fuse3
        lz4
        keyutils
        
        # 系统库
        Threads::Threads
        dl
        pthread
        numa
        rt
        m
        aio
)

# 添加 SPDK 依赖（如果启用自动编译）
if(DEFINED SPDK_DEPENDENCY AND SPDK_DEPENDENCY)
    add_dependencies(${TARGET_NAME} ${SPDK_DEPENDENCY})
endif()

# 设置 RPATH
set_target_properties(${TARGET_NAME} PROPERTIES
    INSTALL_RPATH "${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
    BUILD_RPATH "${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
)

# 安装规则
install(TARGETS ${TARGET_NAME}
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(FILES ${HEADERS} DESTINATION include/fastfs)
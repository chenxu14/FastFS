set(TARGET_NAME fastfs)

file(GLOB SOURCES "*.cpp")
file(GLOB HEADERS "*.h")

add_library(${TARGET_NAME} SHARED ${SOURCES} ${HEADERS})

target_include_directories(${TARGET_NAME} 
    PUBLIC 
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${SPDK_ROOT_DIR}/include
        ${SPDK_ROOT_DIR}/build/include
        ${SPDK_ROOT_DIR}/dpdk/build/include
)

file(GLOB SPDK_LIBS "${SPDK_ROOT_DIR}/build/lib/libspdk_*.a")


# Use manual library list for better control and compatibility
set(SPDK_CORE_LIBS
    ${SPDK_ROOT_DIR}/build/lib/libspdk_event_bdev.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_event_accel.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_event_vmd.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_event_sock.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_event_iobuf.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_event_keyring.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_event.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_malloc.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_null.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_nvme.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_passthru.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_lvol.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_raid.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_error.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_gpt.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_split.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_delay.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_zone_block.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_aio.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_ftl.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev_virtio.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_blob_bdev.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_lvol.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_blob.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_ftl.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_vmd.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_nvme.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_virtio.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_vfio_user.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_accel_error.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_accel_ioat.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_accel.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_ioat.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_sock_posix.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_sock.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_keyring_file.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_keyring_linux.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_keyring.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_bdev.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_notify.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_init.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_thread.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_trace.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_rpc.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_jsonrpc.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_json.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_util.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_log.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_dma.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_conf.a
    
    ${SPDK_ROOT_DIR}/build/lib/libspdk_env_dpdk.a
    ${SPDK_ROOT_DIR}/build/lib/libspdk_env_dpdk_rpc.a
)

set(SPDK_LIBS_FILTERED)
foreach(lib ${SPDK_CORE_LIBS})
    if(EXISTS ${lib})
        list(APPEND SPDK_LIBS_FILTERED ${lib})
    endif()
endforeach()

file(GLOB DPDK_LIBS "${SPDK_ROOT_DIR}/dpdk/build/lib/librte_*.a")

target_link_libraries(${TARGET_NAME} 
    PRIVATE
        -Wl,--whole-archive
        ${SPDK_LIBS_FILTERED}
        -Wl,--no-whole-archive
        
        ${DPDK_LIBS}
        
        ${SPDK_ROOT_DIR}/isa-l/.libs/libisal.a
        ${SPDK_ROOT_DIR}/isa-l-crypto/.libs/libisal_crypto.a
        
        ssl
        crypto
        uuid
        fuse3
        lz4
        keyutils
        
        Threads::Threads
        dl
        pthread
        numa
        rt
        m
        aio
)

if(DEFINED SPDK_DEPENDENCY AND SPDK_DEPENDENCY)
    add_dependencies(${TARGET_NAME} ${SPDK_DEPENDENCY})
endif()

set_target_properties(${TARGET_NAME} PROPERTIES
    INSTALL_RPATH "${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
    BUILD_RPATH "${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
)

install(TARGETS ${TARGET_NAME}
    LIBRARY DESTINATION lib64
    ARCHIVE DESTINATION lib64
    RUNTIME DESTINATION bin
)

install(FILES ${HEADERS} DESTINATION include/fastfs)
# FUSE support is optional
if(ENABLE_FUSE)
    set(TARGET_NAME fastfs_fuse)
    
    # 查找 FUSE3
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(FUSE3 REQUIRED fuse3)
    
    # 源文件
    set(SOURCES fastfs_fuse.cpp)
    
    # 创建可执行文件
    add_executable(${TARGET_NAME} ${SOURCES})
    
    # 包含目录
    target_include_directories(${TARGET_NAME} 
        PRIVATE
            ${CMAKE_SOURCE_DIR}
            ${FUSE3_INCLUDE_DIRS}
            ${SPDK_SUBMODULE_DIR}/include
            ${SPDK_SUBMODULE_DIR}/build/include
            ${SPDK_SUBMODULE_DIR}/dpdk/build/include
    )
    
    # 链接库
    target_link_libraries(${TARGET_NAME} 
        PRIVATE
            fastfs
            ${FUSE3_LIBRARIES}
            ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event.a
            ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_bdev.a
            Threads::Threads
    )
    
    # 设置编译选项
    target_compile_options(${TARGET_NAME} PRIVATE ${FUSE3_CFLAGS_OTHER})
    
    # 设置 RPATH
    set_target_properties(${TARGET_NAME} PROPERTIES
        INSTALL_RPATH "${CMAKE_SOURCE_DIR}/core:${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
        BUILD_RPATH "${CMAKE_BINARY_DIR}/lib:${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
    )
    
    # 安装规则
    install(TARGETS ${TARGET_NAME} DESTINATION bin)
endif()
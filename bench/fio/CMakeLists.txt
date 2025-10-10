# FIO plugin support is optional
if(ENABLE_FIO_PLUGIN)
    set(TARGET_NAME fastfs_fio)
    
    # 源文件
    set(SOURCES ${TARGET_NAME}.cpp)
    
    # 创建共享库 (FIO plugin)
    add_library(${TARGET_NAME} SHARED ${SOURCES})
    
    # 包含目录
    target_include_directories(${TARGET_NAME} 
        PRIVATE
            ${CMAKE_SOURCE_DIR}
            ${FIO_SRC_DIR}
            ${SPDK_SUBMODULE_DIR}/include
            ${SPDK_SUBMODULE_DIR}/build/include
            ${SPDK_SUBMODULE_DIR}/dpdk/build/include
    )
    
    # 链接 fastfs 库
    target_link_libraries(${TARGET_NAME} 
        PRIVATE
            fastfs
            ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event.a
            ${SPDK_SUBMODULE_DIR}/build/lib/libspdk_event_bdev.a
            Threads::Threads
    )
    
    # 添加依赖
    if(DEFINED SPDK_DEPENDENCY AND SPDK_DEPENDENCY)
        add_dependencies(${TARGET_NAME} ${SPDK_DEPENDENCY})
    endif()
    if(DEFINED FIO_DEPENDENCY AND FIO_DEPENDENCY)
        add_dependencies(${TARGET_NAME} ${FIO_DEPENDENCY})
    endif()
    
    # 设置编译选项
    target_compile_options(${TARGET_NAME} PRIVATE
        -fpermissive
        -O3
        -rdynamic
    )
    
    # 设置 RPATH
    set_target_properties(${TARGET_NAME} PROPERTIES
        PREFIX ""
        SUFFIX ".so"
        INSTALL_RPATH "${CMAKE_SOURCE_DIR}/core:${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
        BUILD_RPATH "${CMAKE_BINARY_DIR}/lib:${SPDK_SUBMODULE_DIR}/dpdk/build/lib:${SPDK_SUBMODULE_DIR}/build/lib"
    )
    
    # 安装规则
    install(TARGETS ${TARGET_NAME} DESTINATION lib/fio)
endif()
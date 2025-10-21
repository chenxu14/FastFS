# FUSE support is optional
if(ENABLE_FUSE)
    set(TARGET_NAME fastfs_fuse)
    
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(FUSE3 REQUIRED fuse3)
    
    set(SOURCES fastfs_fuse.cpp)
    
    add_executable(${TARGET_NAME} ${SOURCES})
    
    target_include_directories(${TARGET_NAME} 
        PRIVATE
            ${CMAKE_SOURCE_DIR}
            ${FUSE3_INCLUDE_DIRS}
            ${SPDK_ROOT_DIR}/include
            ${SPDK_ROOT_DIR}/build/include
            ${SPDK_ROOT_DIR}/dpdk/build/include
    )
    
    target_link_libraries(${TARGET_NAME} 
        PRIVATE
            fastfs
            ${FUSE3_LIBRARIES}
            ${SPDK_ROOT_DIR}/build/lib/libspdk_event.a
            ${SPDK_ROOT_DIR}/build/lib/libspdk_event_bdev.a
            Threads::Threads
    )
    
    target_compile_options(${TARGET_NAME} PRIVATE ${FUSE3_CFLAGS_OTHER})
    
    set_target_properties(${TARGET_NAME} PROPERTIES
        INSTALL_RPATH "${CMAKE_SOURCE_DIR}/core:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
        BUILD_RPATH "${CMAKE_BINARY_DIR}/lib:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
    )
    
    install(TARGETS ${TARGET_NAME} DESTINATION bin)
endif()
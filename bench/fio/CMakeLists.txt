# FIO plugin support is optional
set(TARGET_NAME fastfs_fio)

set(SOURCES ${TARGET_NAME}.cpp)

add_library(${TARGET_NAME} SHARED ${SOURCES})

target_include_directories(${TARGET_NAME} 
    PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${FIO_ROOT_DIR}
        ${SPDK_ROOT_DIR}/include
        ${SPDK_ROOT_DIR}/build/include
        ${SPDK_ROOT_DIR}/dpdk/build/include
)

target_link_libraries(${TARGET_NAME} 
    PRIVATE
        fastfs
        ${SPDK_ROOT_DIR}/build/lib/libspdk_event.a
        ${SPDK_ROOT_DIR}/build/lib/libspdk_event_bdev.a
        Threads::Threads
)

if(DEFINED SPDK_DEPENDENCY AND SPDK_DEPENDENCY)
    add_dependencies(${TARGET_NAME} ${SPDK_DEPENDENCY})
endif()
if(DEFINED FIO_DEPENDENCY AND FIO_DEPENDENCY)
    add_dependencies(${TARGET_NAME} ${FIO_DEPENDENCY})
endif()

target_compile_options(${TARGET_NAME} PRIVATE
    -fpermissive
    -O3
    -rdynamic
)

set_target_properties(${TARGET_NAME} PROPERTIES
    PREFIX ""
    SUFFIX ".so"
    INSTALL_RPATH "${CMAKE_SOURCE_DIR}/core:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
    BUILD_RPATH "${CMAKE_BINARY_DIR}/lib:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
)

install(TARGETS ${TARGET_NAME} DESTINATION lib64/fio)
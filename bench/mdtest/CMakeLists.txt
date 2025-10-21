set(TARGET_NAME fs_bench)

set(SOURCES fs_bench.cpp)

add_executable(${TARGET_NAME} ${SOURCES})

target_include_directories(${TARGET_NAME} 
    PRIVATE
        ${CMAKE_SOURCE_DIR}
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

set_target_properties(${TARGET_NAME} PROPERTIES
    INSTALL_RPATH "${CMAKE_SOURCE_DIR}/core:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
    BUILD_RPATH "${CMAKE_BINARY_DIR}/lib:${SPDK_ROOT_DIR}/dpdk/build/lib:${SPDK_ROOT_DIR}/build/lib"
)

install(TARGETS ${TARGET_NAME} DESTINATION bin)
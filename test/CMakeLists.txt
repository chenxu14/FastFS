# Test support is optional
if(ENABLE_TESTS)
    # 首先尝试使用 pkg-config 查找 CUnit
    find_package(PkgConfig QUIET)
    if(PKG_CONFIG_FOUND)
        pkg_check_modules(CUNIT QUIET cunit)
    endif()
    
    # 如果 pkg-config 找不到，尝试手动查找
    if(NOT CUNIT_FOUND)
        find_path(CUNIT_INCLUDE_DIR CUnit/CUnit.h)
        find_library(CUNIT_LIBRARY NAMES cunit)
        
        if(CUNIT_INCLUDE_DIR AND CUNIT_LIBRARY)
            set(CUNIT_FOUND TRUE)
            set(CUNIT_INCLUDE_DIRS ${CUNIT_INCLUDE_DIR})
            set(CUNIT_LIBRARIES ${CUNIT_LIBRARY})
        endif()
    endif()
    
    # 检查 CUnit 是否最终找到
    if(NOT CUNIT_FOUND)
        message(FATAL_ERROR "CUnit not found. Please install CUnit development package.")
    endif()
    
    # 添加 CUnit 包含目录
    include_directories(${CUNIT_INCLUDE_DIRS})
    
    # 添加子目录
    add_subdirectory(bytebuffer)
    add_subdirectory(ckpt)
    add_subdirectory(fs)
    add_subdirectory(journal)
endif()
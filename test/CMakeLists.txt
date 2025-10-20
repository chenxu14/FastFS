#  Copyright (C) 2025 chenxu14
#  SPDX-License-Identifier: BSD-3-Clause

find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(CUNIT QUIET cunit)
endif()

if(NOT CUNIT_FOUND)
    find_path(CUNIT_INCLUDE_DIR CUnit/CUnit.h)
    find_library(CUNIT_LIBRARY NAMES cunit)

    if(CUNIT_INCLUDE_DIR AND CUNIT_LIBRARY)
        set(CUNIT_FOUND TRUE)
        set(CUNIT_INCLUDE_DIRS ${CUNIT_INCLUDE_DIR})
        set(CUNIT_LIBRARIES ${CUNIT_LIBRARY})
    endif()
endif()

if(NOT CUNIT_FOUND)
    message(FATAL_ERROR "CUnit not found. Please install CUnit development package.")
endif()

include_directories(${CUNIT_INCLUDE_DIRS})

add_subdirectory(bytebuffer)
add_subdirectory(ckpt)
add_subdirectory(fs)
add_subdirectory(journal)

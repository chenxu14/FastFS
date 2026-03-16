# Add benchmark subdirectories
add_subdirectory(mdtest)

if(ENABLE_FIO)
    add_subdirectory(fio)
endif()
set(EXEC_FILES
        ./storage/lake/lake_primary_key_consistency_test.cpp
        )

if ("${WITH_STARCACHE}" STREQUAL "ON")
endif ()

if ("${USE_STAROS}" STREQUAL "ON")
endif ()

if (USE_AVX2)
    set(EXEC_FILES ${EXEC_FILES} ./column/avx_numeric_column_test.cpp)
endif ()

add_library(starrocks_test_objs OBJECT ${EXEC_FILES})
SET_TARGET_PROPERTIES(starrocks_test_objs PROPERTIES COMPILE_FLAGS "-fno-access-control")
add_executable(starrocks_test test_main.cpp $<TARGET_OBJECTS:starrocks_test_objs>)

TARGET_LINK_LIBRARIES(starrocks_test ${TEST_LINK_LIBS})

# =================================================
# test cases must be compiled as a standalone binary

# you can simply add test case as single binary without adding `main`
# ADD_BE_TEST(exec/sorting_test)

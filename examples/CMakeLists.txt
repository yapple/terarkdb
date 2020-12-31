add_executable(simple_example
        simple_example.cc)
target_link_libraries(simple_example
        gflags ${ROCKSDB_STATIC_LIB})
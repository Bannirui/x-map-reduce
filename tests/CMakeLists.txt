add_executable(mapreduce_test mapreduce_test.cpp)
target_link_libraries(mapreduce_test PRIVATE mapreduce)
add_test(NAME mapreduce_test COMMAND mapreduce_test)

add_executable(coordinator_test coordinator_test.cpp)
add_test(NAME coordinator_test COMMAND coordinator_test $<TARGET_FILE:coordinator>)

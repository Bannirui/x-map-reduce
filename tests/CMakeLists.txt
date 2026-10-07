add_executable(mapreduce_test mapreduce_test.cpp)
target_link_libraries(mapreduce_test PRIVATE x-mapreduce)
add_test(NAME mapreduce_test COMMAND mapreduce_test)

find_package(Threads REQUIRED)

add_executable(net_test net_test.cpp)
target_link_libraries(net_test PRIVATE x-net Threads::Threads)
add_test(NAME net_test COMMAND net_test)

add_executable(control_plane_test control_plane_test.cpp)
target_link_libraries(control_plane_test PRIVATE x-mapreduce)
add_dependencies(control_plane_test x-coordinator x-worker plugin_word_count)
add_test(NAME control_plane_test COMMAND control_plane_test
    $<TARGET_FILE:x-coordinator> $<TARGET_FILE:x-worker> $<TARGET_FILE:plugin_word_count>)

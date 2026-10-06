add_executable(mapreduce_test mapreduce_test.cpp)
target_link_libraries(mapreduce_test PRIVATE mapreduce)
add_test(NAME mapreduce_test COMMAND mapreduce_test)

find_package(Threads REQUIRED)

add_executable(net_test net_test.cpp)
target_link_libraries(net_test PRIVATE net Threads::Threads)
add_test(NAME net_test COMMAND net_test)

add_executable(control_plane_test control_plane_test.cpp)
target_link_libraries(control_plane_test PRIVATE mapreduce)
add_test(NAME control_plane_test COMMAND control_plane_test
    $<TARGET_FILE:x-coordinator> $<TARGET_FILE:x-worker>)

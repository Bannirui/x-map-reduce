add_executable(mapreduce_test mapreduce_test.cpp)
target_link_libraries(mapreduce_test PRIVATE x-mapreduce)
add_test(NAME mapreduce_test COMMAND mapreduce_test)

find_package(Threads REQUIRED)

add_executable(worker_registry_test worker_registry_test.cpp)
target_link_libraries(worker_registry_test PRIVATE x-runtime)
add_test(NAME worker_registry_test COMMAND worker_registry_test)

add_executable(scheduler_test scheduler_test.cpp)
target_link_libraries(scheduler_test PRIVATE x-runtime)
add_test(NAME scheduler_test COMMAND scheduler_test)

add_executable(protocol_test protocol_test.cpp)
target_link_libraries(protocol_test PRIVATE x-protocol)
add_test(NAME protocol_test COMMAND protocol_test)

add_executable(messages_test messages_test.cpp)
target_link_libraries(messages_test PRIVATE x-protocol)
add_test(NAME messages_test COMMAND messages_test)

add_executable(framing_test framing_test.cpp)
target_link_libraries(framing_test PRIVATE x-protocol)
add_test(NAME framing_test COMMAND framing_test)

add_executable(frame_codec_test frame_codec_test.cpp)
target_link_libraries(frame_codec_test PRIVATE x-protocol Threads::Threads)
add_test(NAME frame_codec_test COMMAND frame_codec_test)

add_executable(control_plane_test control_plane_test.cpp)
target_link_libraries(control_plane_test PRIVATE x-mapreduce)
add_dependencies(control_plane_test x-master x-worker xmr-submit plugin_word_count)
add_test(NAME control_plane_test COMMAND control_plane_test
    $<TARGET_FILE:x-master> $<TARGET_FILE:x-worker> $<TARGET_FILE:xmr-submit> $<TARGET_FILE:plugin_word_count>)

find_package(Threads REQUIRED)

add_executable(net_test net_test.cpp)
target_link_libraries(net_test PRIVATE xnet Threads::Threads)
add_test(NAME net_test COMMAND net_test)

add_executable(timer_test timer_test.cpp)
target_link_libraries(timer_test PRIVATE xnet)
add_test(NAME timer_test COMMAND timer_test)

add_executable(thread_pool_test thread_pool_test.cpp)
target_link_libraries(thread_pool_test PRIVATE xnet Threads::Threads)
add_test(NAME thread_pool_test COMMAND thread_pool_test)

add_executable(notifier_test notifier_test.cpp)
target_link_libraries(notifier_test PRIVATE xnet)
add_test(NAME notifier_test COMMAND notifier_test)

add_executable(event_loop_test event_loop_test.cpp)
target_link_libraries(event_loop_test PRIVATE xnet Threads::Threads)
add_test(NAME event_loop_test COMMAND event_loop_test)

add_executable(channel_test channel_test.cpp)
target_link_libraries(channel_test PRIVATE xnet Threads::Threads)
add_test(NAME channel_test COMMAND channel_test)

add_executable(bootstrap_test bootstrap_test.cpp)
target_link_libraries(bootstrap_test PRIVATE xnet Threads::Threads)
add_test(NAME bootstrap_test COMMAND bootstrap_test)

add_executable(codec_test codec_test.cpp)
target_link_libraries(codec_test PRIVATE xnet Threads::Threads)
add_test(NAME codec_test COMMAND codec_test)

add_executable(robustness_test robustness_test.cpp)
target_link_libraries(robustness_test PRIVATE xnet Threads::Threads)
add_test(NAME robustness_test COMMAND robustness_test)

add_executable(half_close_test half_close_test.cpp)
target_link_libraries(half_close_test PRIVATE xnet Threads::Threads)
add_test(NAME half_close_test COMMAND half_close_test)

add_executable(outbound_test outbound_test.cpp)
target_link_libraries(outbound_test PRIVATE xnet Threads::Threads)
add_test(NAME outbound_test COMMAND outbound_test)

add_executable(option_test option_test.cpp)
target_link_libraries(option_test PRIVATE xnet Threads::Threads)
add_test(NAME option_test COMMAND option_test)

add_executable(delimiter_test delimiter_test.cpp)
target_link_libraries(delimiter_test PRIVATE xnet Threads::Threads)
add_test(NAME delimiter_test COMMAND delimiter_test)

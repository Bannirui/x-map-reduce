# V4.1: the coordinator is the user-facing driver; the runtime library and the
# TCP layer are provided by the runtime/ and net/ components.
add_executable(coordinator coordinator.cpp)
target_link_libraries(coordinator PRIVATE xmr_runtime xmr_net)

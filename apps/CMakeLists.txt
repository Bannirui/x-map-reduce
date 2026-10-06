add_executable(x-coordinator coordinator.cpp)
target_link_libraries(x-coordinator PRIVATE x-runtime x-net)

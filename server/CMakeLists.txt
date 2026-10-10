add_executable(x-master master.cpp)
target_link_libraries(x-master PRIVATE x-runtime xnet)

add_executable(x-worker worker.cpp)
target_link_libraries(x-worker PRIVATE x-runtime xnet)

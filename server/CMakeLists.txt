add_executable(x-master master.cpp)
target_link_libraries(x-master PRIVATE runtime xnet)

add_executable(x-worker worker_main.cpp)
target_link_libraries(x-worker PRIVATE worker)

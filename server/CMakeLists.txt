add_executable(x-master master_main.cpp)
target_link_libraries(x-master PRIVATE master)

add_executable(x-worker worker_main.cpp)
target_link_libraries(x-worker PRIVATE worker)

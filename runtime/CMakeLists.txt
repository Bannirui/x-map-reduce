# V4.1: control-plane runtime. The job registry and file-naming helpers are
# shared by the coordinator and the persistent worker, so they live here.
add_library(xmr_runtime STATIC src/jobs.cpp src/task.cpp src/scheduler.cpp)
target_link_libraries(xmr_runtime PUBLIC mapreduce)
target_include_directories(xmr_runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(worker src/worker.cpp)
target_link_libraries(worker PRIVATE xmr_runtime xmr_net)

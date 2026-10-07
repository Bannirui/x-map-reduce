add_library(x-runtime STATIC src/jobs.cpp src/task.cpp src/scheduler.cpp src/plugin.cpp)
target_link_libraries(x-runtime PUBLIC x-mapreduce ${CMAKE_DL_LIBS})
target_include_directories(x-runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(x-worker src/worker.cpp)
target_link_libraries(x-worker PRIVATE x-runtime x-net)

# V3: job registry shared by the coordinator and worker executables.
add_library(jobs STATIC jobs.cpp)
target_link_libraries(jobs PUBLIC mapreduce)
target_include_directories(jobs PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_executable(map_worker map_worker.cpp)
target_link_libraries(map_worker PRIVATE jobs)

add_executable(coordinator coordinator.cpp)
target_link_libraries(coordinator PRIVATE jobs)

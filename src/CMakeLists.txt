file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

find_package(Threads REQUIRED)

add_library(mapreduce ${sources})

target_link_libraries(mapreduce PUBLIC Threads::Threads)

target_include_directories(mapreduce PUBLIC ${PROJECT_SOURCE_DIR}/include)

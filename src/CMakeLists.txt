file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(mapreduce ${sources})

target_include_directories(mapreduce PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

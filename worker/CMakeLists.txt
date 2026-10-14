file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(worker STATIC ${sources})

target_link_libraries(worker PUBLIC runtime x-dfs xnet)

target_include_directories(worker PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(x-dfs STATIC ${sources})

target_link_libraries(x-dfs PUBLIC xnet)

target_include_directories(x-dfs PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

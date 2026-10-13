file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(protocol STATIC ${sources})

target_link_libraries(protocol PUBLIC xnet)

target_include_directories(protocol PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

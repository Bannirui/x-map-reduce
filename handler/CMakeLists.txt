file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(handler STATIC ${sources})

target_link_libraries(handler PUBLIC xnet PRIVATE protocol)

target_include_directories(handler PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

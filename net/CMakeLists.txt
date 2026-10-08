file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(x-net STATIC ${sources})

target_include_directories(x-net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(x-protocol STATIC ${sources})

target_include_directories(x-protocol PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(master STATIC ${sources})

target_link_libraries(master PUBLIC runtime protocol xnet)

target_include_directories(master PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

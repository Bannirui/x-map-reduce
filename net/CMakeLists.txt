file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(x-net STATIC ${sources})

find_package(Threads REQUIRED)
target_link_libraries(x-net PUBLIC Threads::Threads)

target_include_directories(x-net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

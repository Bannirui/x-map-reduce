add_library(x-net STATIC src/net.cpp)

target_include_directories(x-net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

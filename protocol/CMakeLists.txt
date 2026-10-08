add_library(x-protocol STATIC src/protocol.cpp)

target_include_directories(x-protocol PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_library(x-client STATIC src/client.cpp src/capi.cpp)
target_link_libraries(x-client PUBLIC x-protocol)
target_include_directories(x-client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(xmr-submit src/submit.cpp)
target_link_libraries(xmr-submit PRIVATE x-client)

add_library(client STATIC src/client.cpp src/capi.cpp)
target_link_libraries(client PUBLIC protocol x-dfs)
target_include_directories(client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_executable(x-submit src/submit.cpp)
target_link_libraries(x-submit PRIVATE client)

add_executable(x-dfs-put src/dfs.cpp)
target_link_libraries(x-dfs-put PRIVATE client)

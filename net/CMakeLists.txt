# V4.0: standalone TCP library. Depends on nothing (no mapreduce, no assets).
add_library(xmr_net STATIC src/net.cpp)

target_include_directories(xmr_net PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

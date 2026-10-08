file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(x-runtime STATIC ${sources})
target_link_libraries(x-runtime PUBLIC x-mapreduce x-protocol ${CMAKE_DL_LIBS})
target_include_directories(x-runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

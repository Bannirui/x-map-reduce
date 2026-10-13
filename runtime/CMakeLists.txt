file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(runtime STATIC ${sources})
target_link_libraries(runtime PUBLIC mapreduce protocol ${CMAKE_DL_LIBS})
target_include_directories(runtime PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)

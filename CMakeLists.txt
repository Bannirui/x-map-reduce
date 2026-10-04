cmake_minimum_required(VERSION 3.21)
project(x-map-reduce VERSION 1.0.0 LANGUAGES CXX)

# 关掉cmake的warning
set(CMAKE_SUPPRESS_DEVELOPER_WARNINGS 1)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# 输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

include(${CMAKE_SOURCE_DIR}/cmake/AssertCpy.cmake)

add_subdirectory(src)

add_executable(wordCount wordCount.cpp)
target_link_libraries(wordCount PRIVATE mapreduce)
add_asset_copy_target(wordCount "${CMAKE_CURRENT_SOURCE_DIR}/asset")
add_dependencies(wordCount ${ASSET_COPY_TARGET})

add_executable(sum sum.cpp)
target_link_libraries(sum PRIVATE mapreduce)
add_asset_copy_target(sum "${CMAKE_CURRENT_SOURCE_DIR}/asset")
add_dependencies(sum ${ASSET_COPY_TARGET})
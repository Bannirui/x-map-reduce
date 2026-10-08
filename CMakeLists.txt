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

include(${CMAKE_SOURCE_DIR}/cmake/AssetCopy.cmake)

# Shared sample inputs copied next to the runtime binaries in build/bin/asset.
add_asset_copy_target("${CMAKE_SOURCE_DIR}/asset")

include(CTest)

add_subdirectory(mapreduce)
add_subdirectory(net)
add_subdirectory(protocol)
add_subdirectory(runtime)
add_subdirectory(jobs)
add_subdirectory(server)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

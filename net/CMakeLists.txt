# xnet: Linux epoll + reactor 网络库
# 既可作为独立顶层项目构建，也可被父仓库 add_subdirectory 引入。

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    cmake_minimum_required(VERSION 3.21)
    project(xnet VERSION 1.0.0 LANGUAGES CXX)
    set(CMAKE_CXX_STANDARD 20)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_CXX_EXTENSIONS OFF)
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
    include(CTest)
endif()

find_package(Threads REQUIRED)

file(GLOB_RECURSE sources CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)
add_library(xnet STATIC ${sources})
add_library(xnet::xnet ALIAS xnet)

target_link_libraries(xnet PUBLIC Threads::Threads)
target_include_directories(xnet PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()

include(GNUInstallDirs)
install(TARGETS xnet EXPORT xnetTargets ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT xnetTargets
    NAMESPACE xnet::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xnet)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/xnetConfig.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/xnetConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xnet)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/xnetConfigVersion.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/xnetConfig.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/xnetConfigVersion.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/xnet)

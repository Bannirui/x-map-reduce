# Shared assets are copied to build/bin/asset by this single target.
add_asset_copy_target("${CMAKE_CURRENT_SOURCE_DIR}/asset")

add_executable(word_count word_count.cpp)
target_link_libraries(word_count PRIVATE mapreduce)
add_dependencies(word_count copy-assets)

add_executable(sum sum.cpp)
target_link_libraries(sum PRIVATE mapreduce)
add_dependencies(sum copy-assets)

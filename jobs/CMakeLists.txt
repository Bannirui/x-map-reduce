# 插件形式把job导入到框架
function(add_job_plugin name source)
    add_library(plugin_${name} SHARED ${source})
    target_include_directories(plugin_${name} PRIVATE ${PROJECT_SOURCE_DIR}/mapreduce/include)
    set_target_properties(plugin_${name} PROPERTIES
        PREFIX ""
        OUTPUT_NAME "${name}")
endfunction()

add_job_plugin(word_count word_count.cpp)
add_job_plugin(sum sum.cpp)

#pragma once

#include<string>

/**
 * 用户层会按照约定把job的实现编译成动态库
 * 找到这些动态库加载到当前框架里面 也就是找到对应的Map函数要Reduce函数 后面框架要调用
 * @param path 动态库的路径
 */
void loadJobPlugin(const std::string& path);

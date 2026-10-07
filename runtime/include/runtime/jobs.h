#pragma once

#include"mapreduce/mapreduce.h"

#include<string>

struct Job {
    // job的名称 唯一索引
    std::string name;
    // job的map
    MapFunction mapper;
    // job的reduce
    ReduceFunction reducer;
};

/**
 * 用job的名称找缓存着的job运行时动态库
 * @param name job的名称
 */
const Job* findJob(const std::string& name);

/**
 * @param job 运行时加载的动态库
 */
void registerJob(Job job);
#pragma once

#include"mapreduce.h"

#include<string>

struct Job {
    std::string name;
    MapFunction mapper;
    ReduceFunction reducer;
};

// Returns nullptr when no job with that name is registered.
const Job* findJob(const std::string& name);

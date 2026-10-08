#pragma once

#include"mapreduce/mapreduce.h"

#include<cstdint>

inline constexpr std::uint32_t XMR_PLUGIN_ABI_VERSION = 1;

struct XmrJob {
    // 校验用的
    std::uint32_t abi_version;
    // Job名称
    const char* name;
    // 用户实现的Map函数
    MapFunction mapper;
    // 用户实现的Reduce函数
    ReduceFunction reducer;
};

// kaifang开放给用户层用为灭来向框架 向框架导入Job的实现
extern "C" const XmrJob* xmr_get_job_v1();
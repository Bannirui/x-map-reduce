#include"runtime/plugin.h"

#include"mapreduce/plugin.h"
#include"runtime/jobs.h"

#include<dlfcn.h>

#include<stdexcept>
#include<string>
#include<vector>

namespace {
    using GetJobFunction = const XmrJob* (*)();

    struct LoadedPlugin {
        void* handle = nullptr;
        std::string path;
    };

    // 已经加载好的Job插件 在Job整个使用的生命周期内都不会调用dlclose()的
    std::vector<LoadedPlugin>& loadedPlugins() {
        static std::vector<LoadedPlugin> plugins;
        return plugins;
    }
} // namespace

void loadJobPlugin(const std::string& path) {
    // 这个Job已经加载过了
    for (const auto& plugin : loadedPlugins()) {
        if (plugin.path == path) {
            return;
        }
    }

    void* handle = ::dlopen(path.c_str(),RTLD_NOW | RTLD_LOCAL);
    if (handle == nullptr) {
        throw std::runtime_error("failed to load plugin '" + path + "': " + ::dlerror());
    }
    // 防止有残留的异常
    ::dlerror();
    // 开放给用户的注册Job插件注册函数
    auto* getJob = reinterpret_cast<GetJobFunction>(::dlsym(handle, "xmr_get_job_v1"));
    const char* symbolError = ::dlerror();
    if (symbolError != nullptr || getJob == nullptr) {
        ::dlclose(handle);
        throw std::runtime_error("plugin '" + path + "' does not export xmr_get_job_v1");
    }

    const XmrJob* job = getJob();
    if (job == nullptr) {
        ::dlclose(handle);
        throw std::runtime_error("plugin '" + path + "' returned a null job");
    }
    // 校验
    if (job->abi_version != XMR_PLUGIN_ABI_VERSION) {
        ::dlclose(handle);
        throw std::runtime_error("plugin '" + path + "' has ABI version "
                                 + std::to_string(job->abi_version) + ", expected "
                                 + std::to_string(XMR_PLUGIN_ABI_VERSION));
    }

    try {
        // 缓存Job的map reduce
        registerJob(Job{job->name, job->mapper, job->reducer});
    } catch (...) {
        ::dlclose(handle);
        throw;
    }
    // 防止重复加载插件
    loadedPlugins().push_back(LoadedPlugin{handle, path});
}
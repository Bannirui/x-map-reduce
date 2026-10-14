#pragma once

#include <cstddef>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace xmr::master {
    class PluginDistributor {
    public:
        void reset(const std::string& hash) {
            hash_ = hash;
            ready_ = 0;
        }

        const std::string& hash() const {
            return hash_;
        }

        bool empty() const {
            return hash_.empty();
        }

        std::size_t ready() const {
            return ready_;
        }

        void markLoaded() {
            ++ready_;
        }

        void unmarkLoaded() {
            if (ready_ > 0) {
                --ready_;
            }
        }

        void store(const std::string& hash, std::vector<std::uint8_t> bytes) {
            store_[hash] = std::move(bytes);
        }

        bool has(const std::string& hash) const {
            return store_.find(hash) != store_.end();
        }

        const std::vector<std::uint8_t>* find(const std::string& hash) const {
            const auto it = store_.find(hash);
            return it == store_.end() ? nullptr : &it->second;
        }

    private:
        // 按内容哈希存插件二进制
        std::unordered_map<std::string, std::vector<std::uint8_t> > store_;
        // 当前job的插件哈希 空表示worker本地已预加载
        std::string hash_;
        // 已经加载当前插件的worker数
        std::size_t ready_ = 0;
    };
}  // namespace xmr::master

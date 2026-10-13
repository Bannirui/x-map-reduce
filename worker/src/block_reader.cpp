#include "worker/block_reader.h"

#include "dfs/block_client.h"
#include "dfs/block_store.h"

#include <stdexcept>

namespace xmr::worker {
    // "blockId,host,port" -> BlockRef
    BlockRef parseBlockRef(const std::string& text) {
        BlockRef ref;
        const auto first = text.find(',');
        if (first == std::string::npos) {
            return ref;
        }
        const auto second = text.find(',', first + 1);
        if (second == std::string::npos) {
            return ref;
        }
        ref.blockId = text.substr(0, first);
        ref.host = text.substr(first + 1, second - first - 1);
        ref.port = static_cast<std::uint16_t>(std::stoul(text.substr(second + 1)));
        return ref;
    }

    // 收到map任务 在线程池里从master数据面拉输入 跑map 结果留在本地并上报Done
    // 读map任务的全部输入block 优先本地 否则向对端worker拉 按block顺序拼接
    std::string readBlocks(BlockStore& store, const std::vector<std::string>& refs) {
        std::string content;
        std::size_t index = 0;
        while (index < refs.size()) {
            const BlockRef ref = parseBlockRef(refs[index]);
            if (ref.blockId.empty()) {
                ++index;
                continue;
            }
            bool got = false;
            if (store.has(ref.blockId)) {
                const auto data = store.get(ref.blockId);
                if (data.has_value()) {
                    content.append(reinterpret_cast<const char*>(data->data()), data->size());
                    got = true;
                }
            }
            std::size_t peer = index;
            while (!got && peer < refs.size()) {
                const BlockRef replica = parseBlockRef(refs[peer]);
                if (replica.blockId != ref.blockId) {
                    break;
                }
                try {
                    BlockClient client(replica.host, replica.port);
                    const auto data = client.readBlock(ref.blockId);
                    if (data.has_value()) {
                        content.append(reinterpret_cast<const char*>(data->data()), data->size());
                        got = true;
                    }
                } catch (const std::exception&) {
                }
                ++peer;
            }
            if (!got) {
                throw std::runtime_error("cannot read block " + ref.blockId);
            }
            while (index < refs.size() && parseBlockRef(refs[index]).blockId == ref.blockId) {
                ++index;
            }
        }
        return content;
    }

    bool copyBlock(BlockStore& store, const std::string& blockId, const std::string& host, std::uint16_t port) {
        try {
            BlockClient client(host, port);
            const auto data = client.readBlock(blockId);
            if (data.has_value()) {
                store.put(blockId, *data);
                return true;
            }
        } catch (const std::exception&) {
        }
        return false;
    }
}  // namespace xmr::worker

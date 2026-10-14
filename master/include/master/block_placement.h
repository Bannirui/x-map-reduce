#pragma once

#include "master/types.h"
#include "protocol/messages.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace xmr::master {
    inline constexpr std::size_t kBlockReplicas{3};

    std::string manifestBlockId(const std::string& entry);

    struct BlockPlacementResult {
        ConnId client = 0;
        std::uint32_t requestId = 0;
        xmr::protocol::BlockPlacement message;
    };

    struct BlockReplication {
        ConnId target = 0;
        xmr::protocol::ReplicateBlock request;
    };

    class BlockPlacement {
    public:
        struct PendingManifest {
            ConnId client = 0;
            std::uint32_t requestId = 0;
            xmr::protocol::BlockManifest manifest;
        };

        void setWorkerAddress(const std::string& workerId, const std::string& host, std::uint16_t port);

        void setWorkerBlockPort(const std::string& workerId, std::uint16_t port);

        void eraseWorker(const std::string& workerId);

        void onReport(const std::string& workerId, const std::vector<std::string>& blocks);

        void queueManifest(ConnId client, std::uint32_t requestId, xmr::protocol::BlockManifest manifest);

        std::optional<BlockPlacementResult> tryPlace(const std::function<bool(const std::string&)>& isLive);

        std::vector<std::vector<std::string> > buildMapBlocks(
            const std::vector<std::vector<std::string> >& pendingInputBlocks) const;

        std::vector<BlockReplication> reReplicate(
            const std::function<bool(const std::string&)>& isLive,
            const std::function<std::optional<ConnId>(const std::string&)>& connOf);

        void forgetWorker(const std::string& workerId);

        void clear();

        std::optional<std::pair<std::string, std::uint16_t> > addressOf(const std::string& workerId) const;

        const std::unordered_map<std::string, std::unordered_set<std::string> >& heldWorkers() const {
            return workerBlocks_;
        }

    private:
        void pruneReplicating(const std::string& workerId);

        std::unordered_map<std::string, std::pair<std::string, std::uint16_t> > workerData_;
        std::unordered_map<std::string, std::uint16_t> workerBlock_;
        std::unordered_map<std::string, std::unordered_set<std::string> > workerBlocks_;
        std::unordered_set<std::string> replicating_;
        std::optional<PendingManifest> pendingManifest_;
    };
}  // namespace xmr::master

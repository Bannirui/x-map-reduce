#include "master/block_placement.h"

#include <algorithm>

namespace xmr::master {
    std::string manifestBlockId(const std::string& entry) {
        const auto first = entry.find(',');
        if (first == std::string::npos) {
            return {};
        }
        const auto second = entry.find(',', first + 1);
        if (second == std::string::npos) {
            return {};
        }
        const auto third = entry.find(',', second + 1);
        if (third == std::string::npos) {
            return {};
        }
        return entry.substr(second + 1, third - second - 1);
    }

    void BlockPlacement::setWorkerAddress(const std::string& workerId, const std::string& host, std::uint16_t port) {
        workerData_[workerId] = {host, port};
    }

    void BlockPlacement::setWorkerBlockPort(const std::string& workerId, std::uint16_t port) {
        workerBlock_[workerId] = port;
    }

    void BlockPlacement::eraseWorker(const std::string& workerId) {
        workerData_.erase(workerId);
        workerBlock_.erase(workerId);
        forgetWorker(workerId);
    }

    void BlockPlacement::forgetWorker(const std::string& workerId) {
        workerBlocks_.erase(workerId);
        pruneReplicating(workerId);
    }

    void BlockPlacement::pruneReplicating(const std::string& workerId) {
        const std::string suffix = "@" + workerId;
        for (auto it = replicating_.begin(); it != replicating_.end();) {
            if (it->size() >= suffix.size() && it->compare(it->size() - suffix.size(), suffix.size(), suffix) == 0) {
                it = replicating_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void BlockPlacement::onReport(const std::string& workerId, const std::vector<std::string>& blocks) {
        std::unordered_set<std::string>& held = workerBlocks_[workerId];
        held.clear();
        for (const auto& blockId : blocks) {
            held.insert(blockId);
        }
        pruneReplicating(workerId);
    }

    void BlockPlacement::queueManifest(ConnId client, std::uint32_t requestId, xmr::protocol::BlockManifest manifest) {
        pendingManifest_ = PendingManifest{client, requestId, std::move(manifest)};
    }

    std::optional<BlockPlacementResult> BlockPlacement::tryPlace(const std::function<bool(const std::string&)>& isLive) {
        if (!pendingManifest_) {
            return std::nullopt;
        }
        std::vector<std::string> ready;
        for (const auto& entry : workerBlock_) {
            if (workerData_.count(entry.first) != 0 && isLive(entry.first)) {
                ready.push_back(entry.first);
            }
        }
        if (ready.empty()) {
            return std::nullopt;
        }
        std::sort(ready.begin(), ready.end());
        const std::size_t replicas = std::min<std::size_t>(kBlockReplicas, ready.size());
        BlockPlacementResult result;
        result.client = pendingManifest_->client;
        result.requestId = pendingManifest_->requestId;
        std::size_t blockIndex = 0;
        for (const auto& block : pendingManifest_->manifest.blocks) {
            const std::string blockId = manifestBlockId(block);
            if (blockId.empty()) {
                ++blockIndex;
                continue;
            }
            for (std::size_t r = 0; r < replicas; ++r) {
                const std::string& workerId = ready[(blockIndex + r) % ready.size()];
                workerBlocks_[workerId].insert(blockId);
                const auto address = workerData_.find(workerId);
                result.message.replicas.push_back(blockId + "," + address->second.first + "," +
                                                  std::to_string(workerBlock_[workerId]));
            }
            ++blockIndex;
        }
        result.message.statusCode = xmr::protocol::StatusCode::Ok;
        pendingManifest_.reset();
        return result;
    }

    std::vector<std::vector<std::string> > BlockPlacement::buildMapBlocks(
        const std::vector<std::vector<std::string> >& pendingInputBlocks) const {
        std::vector<std::vector<std::string> > result(pendingInputBlocks.size());
        for (std::size_t input = 0; input < pendingInputBlocks.size(); ++input) {
            for (const auto& blockId : pendingInputBlocks[input]) {
                for (const auto& entry : workerBlocks_) {
                    if (entry.second.count(blockId) == 0) {
                        continue;
                    }
                    const std::string& workerId = entry.first;
                    const auto address = workerData_.find(workerId);
                    const auto port = workerBlock_.find(workerId);
                    if (address == workerData_.end() || port == workerBlock_.end()) {
                        continue;
                    }
                    result[input].push_back(blockId + "," + address->second.first + "," +
                                            std::to_string(port->second));
                }
            }
        }
        return result;
    }

    std::vector<BlockReplication> BlockPlacement::reReplicate(
        const std::function<bool(const std::string&)>& isLive,
        const std::function<std::optional<ConnId>(const std::string&)>& connOf) {
        std::vector<BlockReplication> commands;
        if (workerBlocks_.empty()) {
            return commands;
        }
        std::unordered_map<std::string, std::vector<std::string> > holders;
        for (const auto& entry : workerBlocks_) {
            if (!isLive(entry.first)) {
                continue;
            }
            for (const auto& blockId : entry.second) {
                holders[blockId].push_back(entry.first);
            }
        }
        std::vector<std::string> targets;
        for (const auto& entry : workerBlock_) {
            if (isLive(entry.first)) {
                targets.push_back(entry.first);
            }
        }
        std::sort(targets.begin(), targets.end());
        for (const auto& block : holders) {
            const std::string& blockId = block.first;
            const std::vector<std::string>& live = block.second;
            if (live.empty() || live.size() >= kBlockReplicas) {
                continue;
            }
            const auto sourceAddress = workerData_.find(live.front());
            const auto sourcePort = workerBlock_.find(live.front());
            if (sourceAddress == workerData_.end() || sourcePort == workerBlock_.end()) {
                continue;
            }
            for (const auto& target : targets) {
                if (std::find(live.begin(), live.end(), target) != live.end()) {
                    continue;
                }
                const std::string key = blockId + "@" + target;
                if (replicating_.count(key) != 0) {
                    continue;
                }
                const auto targetConn = connOf(target);
                if (!targetConn) {
                    continue;
                }
                BlockReplication command;
                command.target = *targetConn;
                command.request.blockId = blockId;
                command.request.sourceHost = sourceAddress->second.first;
                command.request.sourcePort = sourcePort->second;
                commands.push_back(std::move(command));
                replicating_.insert(key);
                workerBlocks_[target].insert(blockId);
                break;
            }
        }
        return commands;
    }

    void BlockPlacement::clear() {
        workerBlocks_.clear();
        replicating_.clear();
    }

    std::optional<std::pair<std::string, std::uint16_t> > BlockPlacement::addressOf(
        const std::string& workerId) const {
        const auto it = workerData_.find(workerId);
        if (it == workerData_.end()) {
            return std::nullopt;
        }
        return it->second;
    }
}  // namespace xmr::master

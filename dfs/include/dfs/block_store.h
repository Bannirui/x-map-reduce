#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

using BlockId = std::string;

// dfs的存储单元 本地数据
class BlockStore {
public:
    /// @param rootDir 存放所有block文件的路径
    explicit BlockStore(std::string rootDir);

    /// @brief block落盘的格式=[大端序的8字节校验和][原始的数据] 先写到临时文件 然后原子rename
    /// @param blockId block文件名
    /// @param data 要落盘的原始数据
    bool put(const BlockId& blockId, const std::vector<std::uint8_t>& data);

    /// @param blockId 文件名 有没有这个block文件
    bool has(const BlockId& blockId) const;

    /// @brief 拿出校验和检查 读出来block的原始数据
    /// @param blockId block文件名
    /// @return block的原始数据
    std::optional<std::vector<std::uint8_t>> get(const BlockId& blockId) const;

    /// @brief 删除block
    /// @param blockId block文件名
    bool remove(const BlockId& blockId);

    /// @return block目录下所有的block文件名 排过序了
    std::vector<BlockId> list() const;

    /// @return 目录下有多少个block文件
    std::size_t count() const;

    /// @param blockId block文件名
    /// @return block有存储了多大的原始内容 实际block文件头会有8字节的大端校验和
    std::optional<std::uint64_t> size(const BlockId& blockId) const;

    const std::string& root() const noexcept {
        return rootDir_;
    }

    /// @brief 约束block文件名的规范
    /// @param blockId block文件名
    static bool isValidBlockId(const BlockId& blockId);

private:
    /// @param blockId block文件名
    /// @return block文件全路径
    std::string pathOf(const BlockId& blockId) const;

    /// @return block文件全路径 临时用 put的时候先写到临时文件 然后再原子rename
    std::string tempPath() const;

    // 一个block一个文件 所有的block文件都存放在这个路径
    std::string rootDir_;
};

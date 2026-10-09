#pragma once

#include<cstddef>
#include<cstdint>
#include<vector>

namespace xmr::net {
    // 模仿Netty
    class ByteBuffer {
    public:
        void append(const void* data, std::size_t size);

        void append(const std::vector<std::uint8_t>& data);

        const std::uint8_t* data() const noexcept {
            return storage_.data() + readOffset_;
        }

        std::size_t size() const noexcept {
            return storage_.size() - readOffset_;
        }

        bool empty() const noexcept {
            return size() == 0;
        }

        void consume(std::size_t size);

        std::vector<std::uint8_t> take(std::size_t size);

    private:
        void compact();

        std::vector<std::uint8_t> storage_;
        std::size_t readOffset_ = 0;
    };
} // namespace xmr::net

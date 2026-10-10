#include"net/buffer.h"

#include<stdexcept>

ByteBuffer::ByteBuffer(const std::vector<std::uint8_t>& data) : storage_(data) {
}

void ByteBuffer::compact() {
    if (readOffset_ == 0) {
        return;
    }
    if (readOffset_ == storage_.size()) {
        storage_.clear();
        readOffset_ = 0;
        return;
    }
    if (readOffset_ >= 4096 && readOffset_ * 2 >= storage_.size()) {
        storage_.erase(storage_.begin(), storage_.begin() + static_cast<std::ptrdiff_t>(readOffset_));
        readOffset_ = 0;
    }
}

void ByteBuffer::append(const void* data, std::size_t size) {
    compact();
    const auto* bytes = static_cast<const std::uint8_t*>(data);
    storage_.insert(storage_.end(), bytes, bytes + size);
}

void ByteBuffer::append(const std::vector<std::uint8_t>& data) {
    append(data.data(), data.size());
}

/// @param size 已经发出去这么多数据了 更新缓冲区 从缓冲区摘掉这么多的数据
void ByteBuffer::consume(std::size_t size) {
    if (size > this->size()) {
        throw std::out_of_range("ByteBuffer::consume past end");
    }
    readOffset_ += size;
    compact();
}

std::vector<std::uint8_t> ByteBuffer::take(std::size_t size) {
    if (size > this->size()) {
        throw std::out_of_range("ByteBuffer::take past end");
    }
    std::vector<std::uint8_t> out(storage_.begin() + static_cast<std::ptrdiff_t>(readOffset_),
                                  storage_.begin() + static_cast<std::ptrdiff_t>(readOffset_ + size));
    readOffset_ += size;
    compact();
    return out;
}

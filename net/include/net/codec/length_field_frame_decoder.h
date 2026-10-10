#pragma once

#include "net/codec/byte_to_message_decoder.h"

#include <any>
#include <cstddef>
#include <cstdint>

class LengthFieldBasedFrameDecoder : public ByteToMessageDecoder {
public:
    LengthFieldBasedFrameDecoder(std::size_t lengthFieldOffset,
                                 std::size_t lengthFieldLength,
                                 std::size_t lengthAdjustment,
                                 std::size_t initialBytesToStrip,
                                 std::size_t maxFrameLength);

protected:
    bool decode(std::any& out) override;

private:
    static std::size_t readLength(const std::uint8_t* data, std::size_t length);

    std::size_t lengthFieldOffset_;
    std::size_t lengthFieldLength_;
    std::size_t lengthAdjustment_;
    std::size_t initialBytesToStrip_;
    std::size_t maxFrameLength_;
};

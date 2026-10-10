#include "net/codec/delimiter_based_frame_decoder.h"

#include "net/buffer.h"

#include <any>
#include <cstdint>
#include <stdexcept>
#include <vector>

DelimiterBasedFrameDecoder::DelimiterBasedFrameDecoder(std::uint8_t delimiter,
                                                       std::size_t maxFrameLength,
                                                       bool stripDelimiter)
    : delimiter_(delimiter), maxFrameLength_(maxFrameLength), stripDelimiter_(stripDelimiter) {
}

bool DelimiterBasedFrameDecoder::decode(std::any& out) {
    ByteBuffer& in = cumulation();
    const std::uint8_t* data = in.data();
    const std::size_t size = in.size();
    std::size_t index = size;
    for (std::size_t i = 0; i < size; ++i) {
        if (data[i] == delimiter_) {
            index = i;
            break;
        }
    }
    if (index == size) {
        if (size > maxFrameLength_) {
            throw std::runtime_error("delimiter frame decoder: frame too large");
        }
        return false;
    }
    const std::size_t frameLength = index + 1;
    const std::size_t payloadLength = stripDelimiter_ ? index : frameLength;
    std::vector<std::uint8_t> frame(data, data + payloadLength);
    in.consume(frameLength);
    out = ByteBuffer(frame);
    return true;
}

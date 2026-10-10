#include "net/codec/length_field_frame_decoder.h"

#include <stdexcept>
#include <vector>

LengthFieldBasedFrameDecoder::LengthFieldBasedFrameDecoder(std::size_t lengthFieldOffset,
                                                           std::size_t lengthFieldLength,
                                                           std::size_t lengthAdjustment,
                                                           std::size_t initialBytesToStrip,
                                                           std::size_t maxFrameLength)
    : lengthFieldOffset_(lengthFieldOffset),
      lengthFieldLength_(lengthFieldLength),
      lengthAdjustment_(lengthAdjustment),
      initialBytesToStrip_(initialBytesToStrip),
      maxFrameLength_(maxFrameLength) {
}

std::size_t LengthFieldBasedFrameDecoder::readLength(const std::uint8_t* data, std::size_t length) {
    std::size_t value = 0;
    for (std::size_t i = 0; i < length; ++i) {
        value = (value << 8) | data[i];
    }
    return value;
}

bool LengthFieldBasedFrameDecoder::decode(std::any& out) {
    ByteBuffer& in = cumulation();
    const std::size_t lengthFieldEnd = lengthFieldOffset_ + lengthFieldLength_;
    if (in.size() < lengthFieldEnd) {
        return false;
    }
    std::size_t frameLength = readLength(in.data() + lengthFieldOffset_, lengthFieldLength_);
    frameLength += lengthFieldOffset_ + lengthFieldLength_ + lengthAdjustment_;
    if (frameLength < lengthFieldEnd || frameLength > maxFrameLength_) {
        throw std::runtime_error("length field frame decoder: bad frame length");
    }
    if (in.size() < frameLength) {
        return false;
    }
    if (initialBytesToStrip_ > frameLength) {
        throw std::runtime_error("length field frame decoder: strip exceeds frame");
    }
    const std::size_t payloadLength = frameLength - initialBytesToStrip_;
    std::vector<std::uint8_t> frame(in.data() + initialBytesToStrip_,
                                    in.data() + initialBytesToStrip_ + payloadLength);
    in.consume(frameLength);
    out = ByteBuffer(frame);
    return true;
}

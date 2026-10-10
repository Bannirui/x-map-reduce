#pragma once

#include "net/codec/byte_to_message_decoder.h"

#include <any>
#include <cstddef>
#include <cstdint>

class DelimiterBasedFrameDecoder : public ByteToMessageDecoder {
public:
    DelimiterBasedFrameDecoder(std::uint8_t delimiter, std::size_t maxFrameLength, bool stripDelimiter = true);

protected:
    bool decode(std::any& out) override;

private:
    std::uint8_t delimiter_;
    std::size_t maxFrameLength_;
    bool stripDelimiter_;
};

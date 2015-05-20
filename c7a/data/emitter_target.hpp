#pragma once
#ifndef C7A_DATA_EMITTER_TARGET_HEADER
#define C7A_DATA_EMITTER_TARGET_HEADER

namespace c7a {
namespace data {
class BinaryBuffer;

class EmitterTarget {
public:
    virtual void Close() = 0;
    virtual void Append(BinaryBuffer buffer) = 0;
};
} //namespace data
} //namespace c7a
#endif // !C7A_DATA_EMITTER_TARGET_HEADER

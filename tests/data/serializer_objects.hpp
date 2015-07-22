#pragma once
#include <cereal/archives/c7a.hpp>

namespace c7a {
namespace data {
namespace serializers {

struct TestSerializeObject {
    TestSerializeObject(int bla, int blu) : bla_(bla), blu_(blu) { }
    int bla_;
    int blu_;
};

struct TestCerealObject {
    int x_, y_, z_;

    // This method lets cereal know which data members to serialize
    template <class Archive>
    void serialize(Archive& archive) {
        archive(x_, y_, z_);   // serialize things by passing them to the archive
    }
};

struct TestCerealObject2 {
    TestCerealObject2() { }
    TestCerealObject2(int x, int y, int z) : x_(x), y_(y), z_(z) {
        tco.x_ = x_;
        tco.y_ = y_;
        tco.z_ = z_;
    }

    int              x_, y_, z_;
    TestCerealObject tco;

    // This method lets cereal know which data members to serialize
    template <class Archive>
    void serialize(Archive& archive) {
        archive(x_, y_, z_, tco);   // serialize things by passing them to the archive
    }
};
}
}
}

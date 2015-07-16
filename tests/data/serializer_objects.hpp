#pragma once

namespace c7a {
namespace data {
namespace serializers {

struct TestSerializeObject;

struct TestSerializeObject {
    TestSerializeObject(int bla, int blu) : bla_(bla), blu_(blu) { }
    int bla_;
    int blu_;
};

struct TestCerealObject {
    // TestCerealObject();
    // TestCerealObject(int x, int y, int z) : x_(x), y_(y), z_(z) { };
    int x_, y_, z_;

    // This method lets cereal know which data members to serialize
    template <class Archive>
    void serialize(Archive& archive) {
        archive(x_, y_, z_);   // serialize things by passing them to the archive
    }
};

}
}
}

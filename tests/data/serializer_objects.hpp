/*******************************************************************************
 * tests/data/serializer_objects.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_TESTS_DATA_SERIALIZER_OBJECTS_HEADER
#define C7A_TESTS_DATA_SERIALIZER_OBJECTS_HEADER
#include <c7a/data/serializer_cereal_archive.hpp>
#include <string>

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

struct CerealMyRecord
{
    uint8_t x, y;
    float   z;

    template <class Archive>
    void serialize(Archive& ar) {
        ar(x, y, z);
    }
};

struct CerealSomeData
{
    int32_t                                                        id;
    std::shared_ptr<std::unordered_map<uint32_t, CerealMyRecord> > data;

    template <class Archive>
    void save(Archive& ar) const {
        ar(data);
    }

    template <class Archive>
    void load(Archive& ar) {
        static int32_t idGen = 0;
        id = idGen++;
        ar(data);
    }
};

}
}
}

#endif // !C7A_TESTS_DATA_SERIALIZER_OBJECTS_HEADER

/******************************************************************************/

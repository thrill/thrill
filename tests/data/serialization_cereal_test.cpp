/*******************************************************************************
 * tests/data/serialization_cereal_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <cereal/types/string.hpp>
#include <cereal/types/vector.hpp>
#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_queue.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/serialization.hpp>
#include <thrill/data/serialization_cereal.hpp>

#include <string>
#include <tuple>
#include <typeinfo>
#include <utility>
#include <vector>

using namespace c7a::data;

static const bool debug = false;

// struct ProtobufObject {
//     TestSerializeObject(int bla, int blu) : bla_(bla), blu_(blu) { }
//     int bla_;
//     int blu_;
// };

struct CerealObject3 {
    int x_, y_, z_;

    template <class Archive>
    void serialize(Archive& archive) {
        archive(x_, y_, z_);
    }
};

struct CerealObject2 {
    CerealObject2() { }
    CerealObject2(int x, int y, int z) : x_(x), y_(y), z_(z) {
        tco.x_ = x_;
        tco.y_ = y_;
        tco.z_ = z_;
    }

    int           x_, y_, z_;
    CerealObject3 tco;

    // This method lets cereal know which data members to serialize
    template <class Archive>
    void serialize(Archive& archive) {
        archive(x_, y_, z_, tco);
    }
};

struct CerealObject
{
    uint8_t                  x, y;
    float                    z;
    std::string              a;
    std::vector<std::string> b;

    template <class Archive>
    void serialize(Archive& ar) {
        ar(x, y, z, a, b);
    }
};

TEST(SerializationCereal, cereal_w_FileWriter)
{
    c7a::data::File f;

    auto w = f.GetWriter();

    CerealObject co;
    co.a = "asdfasdf";
    co.b = { "asdf", "asdf" };

    CerealObject2 co2(1, 2, 3);

    w(co);
    w(co2);
    w.Close();

    File::Reader r = f.GetReader();

    ASSERT_TRUE(r.HasNext());
    CerealObject coserial = r.Next<CerealObject>();
    ASSERT_TRUE(r.HasNext());
    CerealObject2 coserial2 = r.Next<CerealObject2>();

    ASSERT_EQ(coserial.a, co.a);
    ASSERT_EQ(coserial.b, co.b);
    ASSERT_EQ(coserial2.x_, co2.x_);
    ASSERT_EQ(coserial2.tco.x_, co2.tco.x_);
    ASSERT_FALSE(r.HasNext());

    LOG << coserial.a;
}

TEST(SerializationCereal, cereal_w_BlockQueue)
{
    BlockQueue q;
    {
        auto qw = q.GetWriter(16);
        CerealObject myData;
        myData.a = "asdfasdf";
        myData.b = { "asdf", "asdf" };
        qw(myData);
    }
    {
        auto qr = q.GetReader();

        ASSERT_TRUE(qr.HasNext());
        CerealObject myData2 = qr.Next<CerealObject>();

        ASSERT_EQ("asdfasdf", myData2.a);
        ASSERT_EQ("asdf", myData2.b[0]);
        ASSERT_EQ("asdf", myData2.b[1]);
        ASSERT_FALSE(qr.HasNext());
    }
}

/******************************************************************************/

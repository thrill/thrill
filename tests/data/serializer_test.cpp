/*******************************************************************************
 * tests/data/serializer_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/logger.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/serializer.hpp>
#include <gtest/gtest.h>
#include <tests/data/serializer_objects.hpp>

#include <string>
#include <tuple>
#include <typeinfo>
#include <utility>

using namespace c7a::data;

static const bool debug = false;

TEST(Serializer, StringSerializeDeserialize) {
    c7a::data::File f;
    std::string foo = "foo";
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, IntSerializeDeserialize) {
    int foo = -123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, LongSerializeDeserialize) {
    long foo = -123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, LongLongSerializeDeserialize) {
    long long foo = -123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, UnsignedSerializeDeserialize) {
    unsigned int foo = 2154910440;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
    ASSERT_EQ(sizeof(foo), sizeof(fooserial));
}

TEST(Serializer, UnsignedLongSerializeDeserialize) {
    unsigned long foo = 123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, UnsignedLongLongSerializeDeserialize) {
    unsigned long long foo = 123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, FloatSerializeDeserialize) {
    float foo = 123.123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();

    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, DoubleSerializeDeserialize) {
    double foo = 123.123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();

    ASSERT_DOUBLE_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, LongDoubleSerializeDeserialize) {
    long double foo = 123.123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_DOUBLE_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, SizeTSerializeDeserialize) {
    size_t foo = 123;
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(foo, fooserial);
    ASSERT_EQ(typeid(foo).name(), typeid(fooserial).name());
}

TEST(Serializer, StringIntPairSerializeDeserialize) {
    auto foo = std::make_pair("foo", 123);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
}

TEST(Serializer, IntString_Pair_SerializeDeserialize_Test) {
    auto t1 = 3;
    auto t2 = "4";
    auto foo = std::make_pair(t1, t2);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
}

TEST(Serializer, StringString_Pair_SerializeDeserialize_Test) {
    auto t1 = "first";
    auto t2 = "second";
    auto foo = std::make_pair(t1, t2);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
}

TEST(Serializer, IntInt_Pair_SerializeDeserialize_Test) {
    auto t1 = 3;
    auto t2 = 4;
    auto foo = std::make_pair(t1, t2);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
}

TEST(Serializer, Tuple_SerializeDeserialize_Test) {
    auto foo = std::make_tuple(3, "foo", 5.5);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
    ASSERT_EQ(std::get<2>(foo), std::get<2>(fooserial));
}

TEST(Serializer, TuplePair_SerializeDeserialize_Test) {
    auto p = std::make_pair(-4.673, "string");
    auto foo = std::make_tuple(3, "foo", 5.5, p);
    c7a::data::File f;
    {
        auto w = f.GetWriter();
        w(foo); //gets serialized
    }
    auto r = f.GetReader();
    auto fooserial = r.Next<decltype(foo)>();
    ASSERT_EQ(std::get<0>(foo), std::get<0>(fooserial));
    ASSERT_EQ(std::get<1>(foo), std::get<1>(fooserial));
    ASSERT_EQ(std::get<2>(foo), std::get<2>(fooserial));
    ASSERT_FLOAT_EQ(std::get<3>(foo).first, std::get<3>(fooserial).first);
    ASSERT_EQ(std::get<3>(foo).second, std::get<3>(fooserial).second);
}

// TEST(Serializer, ProtoBuf_Test) {
//     serializers::TestSerializeObject t(1, 2);
//     auto serialized = Serialize<serializers::TestSerializeObject>(t);
//     auto result = Deserialize<serializers::TestSerializeObject>(serialized);
//     ASSERT_EQ(t.bla_, result.bla_);
//     ASSERT_EQ(t.blu_, result.blu_);
// }

// TEST(Serializer, Cereal_Test) {
//     serializers::TestCerealObject t;
//     t.x_ = 1;
//     t.y_ = 2;
//     t.z_ = 3;
//     auto serialized = Serialize<serializers::TestCerealObject>(t);
//     auto result = Deserialize<serializers::TestCerealObject>(serialized);
//     ASSERT_EQ(t.x_, result.x_);
//     ASSERT_EQ(t.y_, result.y_);
//     ASSERT_EQ(t.z_, result.z_);
//     LOG << "X: " << result.x_;
//     LOG << "Y: " << result.y_;
//     LOG << "Z: " << result.z_;
// }

TEST(Serializer, CerealObject_Archive_Test) {
    c7a::data::File f;
    serializers::TestCerealObject2 t(1, 2, 3);
    {
        auto w = f.GetWriter();
        w(t); //gets serialized
    }
    auto r = f.GetReader();
    auto res = r.Next<serializers::TestCerealObject2>();
    ASSERT_EQ(t.x_, res.x_);
    ASSERT_EQ(t.y_, res.y_);
    ASSERT_EQ(t.z_, res.z_);
    ASSERT_EQ(t.tco.z_, res.tco.z_);
    sLOG << res.x_ << res.y_ << res.z_;
}

TEST(Serializer, Tuple_Archive_Test) {
    c7a::data::File f;
    auto t = std::make_tuple(1, 2, 3, std::string("blaaaa"));
    {
        auto w = f.GetWriter();
        w(t); //gets serialized
    }
    auto r = f.GetReader();
    auto res = r.Next<decltype(t)>();

    ASSERT_EQ(std::get<0>(res), std::get<0>(t));
    ASSERT_EQ(std::get<1>(res), std::get<1>(t));
    ASSERT_EQ(std::get<2>(res), std::get<2>(t));
    ASSERT_EQ(std::get<3>(res), std::get<3>(t));
}

TEST(Serializer, Tuple_Size_Archive_Test) {
    c7a::data::File f;
    auto n = std::make_tuple(1, 2, 3, std::string("blaaaa"));
    auto y = std::make_tuple(1, 2, 3, "4");
    auto w = f.GetWriter();
    auto no = serializers::Impl<decltype(w), decltype(n)>::fixed_size;
    auto yes = serializers::Impl<decltype(w), decltype(y)>::fixed_size;

    ASSERT_EQ(no, false);
    ASSERT_EQ(yes, true);
}

TEST(Serializer, Has_Cereal_Imp_Archive_Test) {
    c7a::data::File f1;
    c7a::data::File f2;

    serializers::CerealMyRecord mr;
    mr.x = 23;
    mr.y = 33;
    mr.z = -23.2;

    serializers::CerealSomeData sd;
    sd.id = 2;
    {
        auto w1 = f1.GetWriter();
        auto w2 = f2.GetWriter();
        w1(mr); //gets serialized
        // w2(sd); //gets serialized
    }
    auto r1 = f1.GetReader();
    auto res1 = r1.Next<decltype(mr)>();

    ASSERT_EQ(mr.x, res1.x);
    ASSERT_EQ(mr.y, res1.y);
    ASSERT_EQ(mr.z, res1.z);
}

/******************************************************************************/

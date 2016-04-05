/*******************************************************************************
 * tests/api/operations_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/all_gather.hpp>
#include <thrill/api/bernoulli_sample.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/concat.hpp>
#include <thrill/api/concat_to_dia.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/equal_to_dia.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/sample.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/write_lines_many.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

static constexpr bool debug = false;

class Integer
{
public:
    explicit Integer(size_t v)
        : value_(v) { }

    size_t value() const { return value_; }

    static constexpr bool thrill_is_fixed_size = true;
    static constexpr size_t thrill_fixed_size = sizeof(size_t);

    template <typename Archive>
    void ThrillSerialize(Archive& ar) const {
        ar.template PutRaw<size_t>(value_);
    }

    template <typename Archive>
    static Integer ThrillDeserialize(Archive& ar) {
        return Integer(ar.template GetRaw<size_t>());
    }

    friend std::ostream& operator << (std::ostream& os, const Integer& i) {
        return os << i.value_;
    }

protected:
    size_t value_;
};

TEST(Operations, EqualToDIAAndAllGatherElements) {

    auto start_func =
        [](Context& ctx) {

            static constexpr size_t test_size = 1024;

            std::vector<size_t> in_vector;

            // generate data everywhere
            for (size_t i = 0; i < test_size; ++i) {
                in_vector.push_back(i);
            }

            // "randomly" shuffle.
            std::default_random_engine gen(123456);
            std::shuffle(in_vector.begin(), in_vector.end(), gen);

            DIA<size_t> integers = EqualToDIA(ctx, in_vector).Collapse();

            std::vector<size_t> out_vec = integers.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            ASSERT_EQ(test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i, out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, ConcatToDIAAndAllGatherElements) {

    auto start_func =
        [](Context& ctx) {

            static constexpr size_t test_size = 1024;

            std::vector<size_t> in_vector;

            // generate data everywhere
            for (size_t i = 0; i < test_size; ++i) {
                in_vector.push_back(i);
            }

            DIA<size_t> integers = ConcatToDIA(ctx, in_vector).Collapse();

            std::vector<size_t> out_vec = integers.AllGather();

            ASSERT_EQ(ctx.num_workers() * test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i % test_size, out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, DistributeAndAllGatherElements) {

    auto start_func =
        [](Context& ctx) {

            static constexpr size_t test_size = 1024;

            std::vector<size_t> in_vector;

            if (ctx.my_rank() == 0) {
                // generate data only on worker 0.
                for (size_t i = 0; i < test_size; ++i) {
                    in_vector.push_back(i);
                }

                std::random_shuffle(in_vector.begin(), in_vector.end());
            }

            DIA<size_t> integers = Distribute(ctx, in_vector, 0).Collapse();

            std::vector<size_t> out_vec = integers.AllGather();

            std::sort(out_vec.begin(), out_vec.end());

            ASSERT_EQ(test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i, out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, EqualToDIAAndGatherElements) {

    auto start_func =
        [](Context& ctx) {

            static constexpr size_t test_size = 1024;

            std::vector<size_t> in_vector;

            // generate data everywhere
            for (size_t i = 0; i < test_size; ++i) {
                in_vector.push_back(i);
            }

            // "randomly" shuffle.
            std::default_random_engine gen(123456);
            std::shuffle(in_vector.begin(), in_vector.end(), gen);

            DIA<size_t> integers = EqualToDIA(ctx, in_vector).Cache();

            std::vector<size_t> out_vec = integers.Gather(0);

            std::sort(out_vec.begin(), out_vec.end());

            if (ctx.my_rank() == 0) {
                ASSERT_EQ(test_size, out_vec.size());
                for (size_t i = 0; i < out_vec.size(); ++i) {
                    ASSERT_EQ(i, out_vec[i]);
                }
            }
            else {
                ASSERT_EQ(0u, out_vec.size());
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, GenerateIntegers) {

    static constexpr size_t test_size = 1000;

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) { return index; },
                test_size);

            std::vector<size_t> out_vec = integers.AllGather();

            ASSERT_EQ(test_size, out_vec.size());

            for (size_t i = 0; i < test_size; ++i) {
                ASSERT_EQ(i, out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, GenerateAndConcatTwo) {

    static constexpr size_t test_size = 1024;

    auto start_func =
        [](Context& ctx) {

            auto dia1 = Generate(ctx, test_size).Cache();
            auto dia2 = Generate(ctx, 2 * test_size);

            auto cdia = dia1.Concat(dia2);

            std::vector<size_t> out_vec = cdia.AllGather();

            ASSERT_EQ(3 * test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i < test_size ? i : i - test_size, out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, GenerateAndConcatThree) {

    static constexpr size_t test_size = 1024;

    auto start_func =
        [](Context& ctx) {

            auto dia1 = Generate(ctx, test_size).Cache();
            auto dia2 = Generate(ctx, 2 * test_size).Collapse();
            auto dia3 = Generate(ctx, 3 * test_size).Collapse();
            auto dia4 = Generate(ctx, 7).Collapse();

            auto cdia = Concat({ dia1, dia2, dia3, dia4 });

            std::vector<size_t> out_vec = cdia.AllGather();

            ASSERT_EQ(6 * test_size + 7, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i < test_size ? i :
                          i < 3 * test_size ? i - test_size :
                          i < 6 * test_size ? i - 3 * test_size :
                          i - 6 * test_size,
                          out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, GenerateAndUnionTwo) {

    static constexpr size_t test_size = 1024;

    auto start_func =
        [](Context& ctx) {

            auto dia1 = Generate(ctx, test_size).Cache();
            auto dia2 = Generate(ctx, 2 * test_size);

            auto udia = dia1.Union(dia2);

            // check udia
            std::vector<size_t> out_vec = udia.AllGather();
            std::sort(out_vec.begin(), out_vec.end());

            ASSERT_EQ(3 * test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i < 2 * test_size ? i / 2 : i - test_size,
                          out_vec[i]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, GenerateAndUnionThree) {

    static constexpr size_t test_size = 1024;

    auto start_func =
        [](Context& ctx) {

            auto dia1 = Generate(ctx, test_size).Cache();
            auto dia2 = Generate(ctx, 2 * test_size).Collapse();
            auto dia3 = Generate(ctx, 3 * test_size).Collapse();
            auto dia4 = Generate(ctx, 7).Collapse();

            auto udia = Union({ dia1, dia2, dia3, dia4 });

            std::vector<size_t> out_vec = udia.AllGather();
            std::sort(out_vec.begin(), out_vec.end());

            ASSERT_EQ(6 * test_size + 7, out_vec.size());

            std::vector<size_t> correct_vec;
            for (size_t i = 0; i < test_size; ++i)
                correct_vec.push_back(i);
            for (size_t i = 0; i < 2 * test_size; ++i)
                correct_vec.push_back(i);
            for (size_t i = 0; i < 3 * test_size; ++i)
                correct_vec.push_back(i);
            for (size_t i = 0; i < 7; ++i)
                correct_vec.push_back(i);
            std::sort(correct_vec.begin(), correct_vec.end());
            ASSERT_EQ(correct_vec, out_vec);
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, GenerateAndUnionExecuteOrder) {

    static constexpr size_t test_size = 1024;

    auto start_func =
        [](Context& ctx) {

            auto dia1 = Generate(ctx, test_size).Collapse();
            auto dia2 = Generate(ctx, 2 * test_size).Collapse();

            // create union of two, which will be sorted
            auto udia = Union({ dia1, dia2 });

            auto sorted_udia = udia.Sort();

            // now execute the first input, this will also push the data from
            // dia1 into udia, which forwards it to the Sort().
            ASSERT_EQ(test_size, dia1.Size());

            // check udia
            std::vector<size_t> out_vec = sorted_udia.AllGather();

            ASSERT_EQ(3 * test_size, out_vec.size());
            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(i < 2 * test_size ? i / 2 : i - test_size,
                          out_vec[i]);
            }

            // check size of udia again, which requires a full recalculation.
            ASSERT_EQ(3 * test_size, udia.Size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, MapResultsCorrectChangingType) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index + 1;
                },
                16);

            auto double_elements =
                [](size_t in) {
                    return 2.0 * static_cast<double>(in);
                };

            auto doubled = integers.Map(double_elements);

            std::vector<double> out_vec = doubled.AllGather();

            size_t i = 1;
            for (double& element : out_vec) {
                ASSERT_DOUBLE_EQ(element, (2.0 * static_cast<double>(i++)));
            }

            ASSERT_EQ(16u, out_vec.size());
            static_assert(std::is_same<decltype(doubled)::ValueType, double>::value, "DIA must be double");
            static_assert(std::is_same<decltype(doubled)::StackInput, size_t>::value, "Node must be size_t");
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, FlatMapResultsCorrectChangingType) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index;
                },
                16);

            auto flatmap_double =
                [](size_t in, auto emit) {
                    emit(static_cast<double>(2 * in));
                    emit(static_cast<double>(2 * (in + 16)));
                };

            auto doubled = integers.FlatMap<double>(flatmap_double);

            std::vector<double> out_vec = doubled.AllGather();

            ASSERT_EQ(32u, out_vec.size());

            for (size_t i = 0; i != out_vec.size() / 2; ++i) {
                ASSERT_DOUBLE_EQ(out_vec[2 * i + 0],
                                 2.0 * static_cast<double>(i));

                ASSERT_DOUBLE_EQ(out_vec[2 * i + 1],
                                 2.0 * static_cast<double>(i + 16));
            }

            static_assert(
                std::is_same<decltype(doubled)::ValueType, double>::value,
                "DIA must be double");

            static_assert(
                std::is_same<decltype(doubled)::StackInput, size_t>::value,
                "Node must be size_t");
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, BernoulliSampleCompileAndExecute) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            size_t n = 1024;

            auto sizets = Generate(ctx, n);

            // sample
            auto reduced1 = sizets.BernoulliSample(0.25);
            auto reduced2 = sizets.BernoulliSample(0.05);
            auto out_vec1 = reduced1.AllGather();
            auto out_vec2 = reduced2.AllGather();

            LOG << "result size 0.25: " << out_vec1.size() << " / " << sizets.Size();
            LOG << "result size 0.05: " << out_vec2.size() << " / " << sizets.Size();
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, PrefixSumCorrectResults) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& input) {
                    return input + 1;
                },
                16);

            auto prefixsums = integers.PrefixSum();

            std::vector<size_t> out_vec = prefixsums.AllGather();

            size_t ctr = 0;
            for (size_t i = 0; i < out_vec.size(); i++) {
                ctr += i + 1;
                ASSERT_EQ(out_vec[i], ctr);
            }

            ASSERT_EQ((size_t)16, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, PrefixSumFacultyCorrectResults) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& input) {
                    return input + 1;
                },
                10);

            auto prefixsums = integers.PrefixSum(
                [](size_t in1, size_t in2) {
                    return in1 * in2;
                }, 1);

            std::vector<size_t> out_vec = prefixsums.AllGather();

            size_t ctr = 1;
            for (size_t i = 0; i < out_vec.size(); i++) {
                ctr *= i + 1;
                ASSERT_EQ(out_vec[i], ctr);
            }

            ASSERT_EQ(10u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, GenerateAndSumHaveEqualAmount1) {

    std::default_random_engine generator(std::random_device { } ());
    std::uniform_int_distribution<int> distribution(1000, 10000);

    size_t generate_size = distribution(generator);

    auto start_func =
        [generate_size](Context& ctx) {

            auto input = GenerateFromFile(
                ctx,
                "inputs/test1",
                [](const std::string& line) {
                    return std::stoi(line);
                },
                generate_size);

            auto ones = input.Map([](int) {
                                      return 1;
                                  });

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            ASSERT_EQ((int)generate_size + 42, ones.Sum(add_function, 42));
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, GenerateAndSumHaveEqualAmount2) {

    auto start_func =
        [](Context& ctx) {

            // TODO(ms): Replace this with some test-specific rendered file
            auto input = ReadLines(ctx, "inputs/test1")
                         .Map([](const std::string& line) {
                                  return std::stoi(line);
                              });

            auto ones = input.Map([](int in) {
                                      return in;
                                  });

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            ASSERT_EQ(136, ones.Sum(add_function));
            ASSERT_EQ(16u, ones.Size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, WindowCorrectResults) {

    static constexpr bool debug = false;
    static constexpr size_t test_size = 144;
    static constexpr size_t window_size = 10;

    auto start_func =
        [](Context& ctx) {

            sLOG << ctx.num_hosts();

            auto integers = Generate(
                ctx,
                [](const size_t& input) { return input * input; },
                test_size);

            auto window = integers.Window(
                window_size, [](size_t rank,
                                const common::RingBuffer<size_t>& window) {

                    // check received window
                    die_unequal(window_size, window.size());

                    for (size_t i = 0; i < window.size(); ++i) {
                        sLOG << rank + i << window[i];
                        die_unequal((rank + i) * (rank + i), window[i]);
                    }

                    // return rank to check completeness
                    return Integer(rank);
                });

            // check rank completeness

            std::vector<Integer> out_vec = window.AllGather();

            if (ctx.my_rank() == 0)
                sLOG << common::Join(" - ", out_vec);

            for (size_t i = 0; i < out_vec.size(); i++) {
                ASSERT_EQ(i, out_vec[i].value());
            }

            ASSERT_EQ(test_size - window_size + 1, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, DisjointWindowCorrectResults) {

    static constexpr bool debug = false;
    static constexpr size_t test_size = 144;
    static constexpr size_t window_size = 10;

    auto start_func =
        [](Context& ctx) {

            sLOG << ctx.num_hosts();

            auto integers = Generate(
                ctx,
                [](const size_t& input) { return input * input; },
                test_size);

            auto window = integers.Window(
                DisjointTag, window_size,
                [](size_t rank, const std::vector<size_t>& window) {

                    sLOG << "rank" << rank << "window.size()" << window.size()
                         << test_size - (test_size % window_size);

                    // check received window
                    die_unless(window_size == window.size() ||
                               rank == test_size - (test_size % window_size));

                    for (size_t i = 0; i < window.size(); ++i) {
                        sLOG << rank + i << window[i];
                        die_unequal((rank + i) * (rank + i), window[i]);
                    }

                    // return rank to check completeness
                    return Integer(rank);
                });

            // check rank completeness

            std::vector<Integer> out_vec = window.AllGather();

            if (ctx.my_rank() == 0)
                sLOG << common::Join(" - ", out_vec);

            for (size_t i = 0; i < out_vec.size(); ++i) {
                ASSERT_EQ(window_size * i, out_vec[i].value());
            }

            ASSERT_EQ(
                (test_size + window_size - 1) / window_size, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, FilterResultsCorrectly) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return (size_t)index + 1;
                },
                16);

            auto even = [](size_t in) {
                            return (in % 2 == 0);
                        };

            auto doubled = integers.Filter(even);

            std::vector<size_t> out_vec = doubled.AllGather();

            size_t i = 1;

            for (size_t element : out_vec) {
                ASSERT_EQ(element, (i++ *2));
            }

            ASSERT_EQ(8u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, DIACasting) {

    auto start_func =
        [](Context& ctx) {

            auto even = [](size_t in) {
                            return (in % 2 == 0);
                        };

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return index + 1;
                },
                16);

            DIA<size_t> doubled = integers.Filter(even).Collapse();

            std::vector<size_t> out_vec = doubled.AllGather();

            size_t i = 1;

            for (size_t element : out_vec) {
                ASSERT_EQ(element, (i++ *2));
            }

            ASSERT_EQ(8u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, Sample) {

    auto start_func =
        [](Context& ctx) {
            size_t n = 9999;

            // test with sample smaller than the input
            {
                auto int_sampled = Generate(ctx, n).Sample(100);

                ASSERT_EQ(100u, int_sampled.Size());

                std::vector<size_t> int_vec = int_sampled.AllGather();
                ASSERT_EQ(100u, int_vec.size());
            }

            // test with sample larger than the input
            {
                auto int_sampled = Generate(ctx, n).Sample(20000);

                ASSERT_EQ(9999u, int_sampled.Size());

                std::vector<size_t> int_vec = int_sampled.AllGather();
                ASSERT_EQ(9999u, int_vec.size());
            }

            // test with disbalanced input
            {
                auto int_sampled =
                    Generate(ctx, 1000)
                    .Filter([](size_t i) { return i < 80 || i % 10 == 1; })
                    .Sample(100);

                ASSERT_EQ(100u, int_sampled.Size());

                std::vector<size_t> int_vec = int_sampled.AllGather();
                ASSERT_EQ(100u, int_vec.size());
            }
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, ForLoop) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index;
                },
                16);

            auto flatmap_duplicate = [](size_t in, auto emit) {
                                         emit(in);
                                         emit(in);
                                     };

            auto map_multiply = [](size_t in) {
                                    return 2 * in;
                                };

            DIA<size_t> squares = integers.Collapse();

            // run loop four times, inflating DIA of 16 items -> 256
            for (size_t i = 0; i < 4; ++i) {
                auto pairs = squares.FlatMap(flatmap_duplicate);
                auto multiplied = pairs.Map(map_multiply);
                squares = multiplied.Collapse();
            }

            std::vector<size_t> out_vec = squares.AllGather();

            ASSERT_EQ(256u, out_vec.size());
            for (size_t i = 0; i != 256; ++i) {
                ASSERT_EQ(out_vec[i], 16 * (i / 16));
            }
            ASSERT_EQ(256u, squares.Size());
        };

    api::RunLocalTests(start_func);
}

TEST(Operations, WhileLoop) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) -> size_t {
                    return index;
                },
                16);

            auto flatmap_duplicate = [](size_t in, auto emit) {
                                         emit(in);
                                         emit(in);
                                     };

            auto map_multiply = [](size_t in) {
                                    return 2 * in;
                                };

            DIA<size_t> squares = integers.Collapse();
            size_t sum = 0;

            // run loop four times, inflating DIA of 16 items -> 256
            while (sum < 256) {
                auto pairs = squares.FlatMap(flatmap_duplicate);
                auto multiplied = pairs.Map(map_multiply);
                squares = multiplied.Collapse();
                sum = squares.Size();
            }

            std::vector<size_t> out_vec = squares.AllGather();

            ASSERT_EQ(256u, out_vec.size());
            for (size_t i = 0; i != 256; ++i) {
                ASSERT_EQ(out_vec[i], 16 * (i / 16));
            }
            ASSERT_EQ(256u, squares.Size());
        };

    api::RunLocalTests(start_func);
}

namespace thrill {
namespace api {

// forced instantiation
template class DIA<std::string>;

} // namespace api
} // namespace thrill

/******************************************************************************/

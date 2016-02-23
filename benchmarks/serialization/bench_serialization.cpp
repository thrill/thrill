/*******************************************************************************
 * benchmarks/serialization/bench_serialization.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/stats_timer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/serialization.hpp>
#include <thrill/data/serialization_cereal.hpp>

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "data.hpp"

using namespace thrill; // NOLINT

//! serializes a given object and measures its time
/*! \param t The object that shall be serialized
 *  \param iterations The number how often the object should be serialized;
 *                    The measured time will be divided by number of iterations
 */
template <typename T>
int BenchmarkSerialization(T t, int iterations) {
    common::StatsTimerStopped timer;
    data::BlockPool block_pool;

    for (int i = 0; i < iterations; ++i) {
        data::File f(block_pool, 0);
        timer.Start();
        {
            auto w = f.GetWriter();
            w.Put(t);
        }
        auto r = f.GetConsumeReader();
        r.Next<T>();
        timer.Stop();
    }
    return static_cast<int>(timer.Microseconds() / iterations);
}

//! serializes the test string and measures its time
/*! \param t The object that shall be serialized
 *  \param iterations The number how often the object should be serialized;
 *                    The measured time will be divided by number of iterations
 */
int SerialString(int iterations) {
    return BenchmarkSerialization(bench_string, iterations);
}

//! serializes the test vector and measures its time
/*! \param t The object that shall be serialized
 *  \param iterations The number how often the object should be serialized;
 *                    The measured time will be divided by number of iterations
 */
int SerialVector(int iterations) {
    return BenchmarkSerialization(bench_vector, iterations);
}

//! serializes the test tuples and measures its time
/*! \param t The object that shall be serialized
 *  \param iterations The number how often the object should be serialized;
 *                    The measured time will be divided by number of iterations
 */
int SerialTuple(int iterations) {
    return BenchmarkSerialization(bench_tuple, iterations);
}

//! generates an vector with random ints
/*! \param res The vector that should be filled with random ints
 *  \param n The number of ints in the vector
 */
void GetRandomIntVector(std::vector<int64_t>& res, int n) {
    res.reserve(n);
    std::default_random_engine prng(std::random_device { } ());
    for (int i = 0; i < n; ++i) {
        res.push_back(prng());
    }
}

//! prints an output that is parsable by SQLPlotTools
void PrintSQLPlotTool(std::string datatype, size_t size, int iterations, int time) {
    std::cout << "RESULT"
              << " datatype=" << datatype
              << " size=" << size
              << " repeats=" << iterations
              << " time=" << time
              << std::endl;
}

//! generates random chars and fills a vector
/*! \param s The vector that should be filled with random chars
 *  \param len The number of chars in the vector
 */
void GetRandomString(std::vector<char>& s, const int len) {
    s.reserve(len);
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";

    for (int i = 0; i < len; ++i) {
        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
    }
}

//! executes some serializations and times it to use as benchmark
int main() {
    int iterations = 50;
    // string from cpp-serializer
    PrintSQLPlotTool("std::string", bench_string.size(), iterations, SerialString(iterations));
    // vector from cpp-serializer
    PrintSQLPlotTool("std::vector<int64_t>", sizeof(int64_t) * bench_vector.bench_vector.size(), iterations, SerialVector(iterations));
    // tuple-pair-construct from cpp-serializer
    PrintSQLPlotTool("tuple_construct", 0, iterations, SerialTuple(iterations));

    std::vector<int> size = { 99, 9999, 99999, 999999 };

    // serialize some random strings
    for (int s : size) {
        std::vector<char> x;
        GetRandomString(x, s);
        std::string x_str(x.begin(), x.begin() + s);

        PrintSQLPlotTool("std::string", s, iterations, BenchmarkSerialization(x_str, iterations));
    }

    // serialize some random ints
    for (int s : size) {
        int acc = 0;
        std::default_random_engine prng(std::random_device { } ());
        for (int i = 0; i < s; ++i) {
            int x = prng();
            acc += BenchmarkSerialization(x, 1);
        }
        PrintSQLPlotTool("int", s, iterations, acc);
    }

    // serialize some random int vectors
    for (int s : size) {
        std::vector<int64_t> x;
        GetRandomIntVector(x, s);
        BenchVector x_struct = BenchVector(x);

        PrintSQLPlotTool("std::vector<int64_t>", s, iterations, BenchmarkSerialization(x_struct, iterations));
    }
    return 1;
}

/******************************************************************************/

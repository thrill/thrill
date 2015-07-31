/*******************************************************************************
 * benchmarks/serialization/bench_serialization.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/
#ifndef C7A_BENCH_SERIALIZATION_HEADER
#define C7A_BENCH_SERIALIZATION_HEADER

#include "data.hpp"
#include <c7a/common/stats_timer.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/serializer.hpp>

#include <cstdlib>
#include <iomanip>
#include <iostream>
// #include <random>
#include <string>
#include <vector>

template <typename T>
int BenchmarkSerialization(T t, int iterrations) {
    c7a::common::StatsTimer<true> timer(false);
    for (int i = 0; i < iterrations; ++i) {
        c7a::data::File f;
        timer.Start();
        {
            auto w = f.GetWriter();
            w(t);
        }
        auto r = f.GetReader();
        r.Next<T>();
        timer.Stop();
    }
    return timer.Microseconds() / iterrations;
}

int SerialString(int iterrations) {
    return BenchmarkSerialization(bench_string, iterrations);
}

int SerialVector(int iterrations) {
    return BenchmarkSerialization(bv, iterrations);
}

int SerialTuple(int iterrations) {
    return BenchmarkSerialization(bench_tuple, iterrations);
}

void GetRandomIntVector(std::vector<int64_t>& res, int n) {
    res.reserve(n);
    for (int i = 0; i < n; ++i) {
        res.push_back(rand());
    }
}

void PrintSQLPlotTool(std::string datatype, size_t size, int iterations, int time) {
    std::cout << "RESULT"
              << " datatype=" << datatype
              << " size=" << size
              << " repeats=" << iterations
              << " time=" << time
              << std::endl;
}

// all glory to stackoverflow a/440240
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

int main() {
    int iterations = 50;
    // string from cpp-serializer
    PrintSQLPlotTool("std::string", bench_string.size(), iterations, SerialString(iterations));
    // vector from cpp-serializer
    PrintSQLPlotTool("std::vector<int64_t>", sizeof(int64_t) * bv.bench_vector.size(), iterations, SerialVector(iterations));
    // tuple-pair-construct from cpp-serializer
    PrintSQLPlotTool("???", 0, iterations, SerialTuple(iterations));

    std::vector<int> size = { 100, 8890, 121212, 999999 };

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
        for (int i = 0; i < s; ++i) {
            int x = rand();
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

#endif
/******************************************************************************/

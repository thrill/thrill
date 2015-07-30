#ifndef C7A_BENCH_SERIALIZATION_HEADER
#define C7A_BENCH_SERIALIZATION_HEADER

#include "data.hpp"
#include <c7a/common/stats_timer.hpp>
#include <c7a/data/serializer.hpp>
#include <c7a/data/file.hpp>

#include <iostream>
#include <iomanip>

int SerialString(int times) {
    c7a::common::StatsTimer<true> timer(false);
    for (int i = 0; i < times; ++i) {
        timer.Start();
        //TODO: File creation in or outside benchmark?!
        c7a::data::File f;
        {
            auto w = f.GetWriter();
            w(bench_string);
        }
        auto r = f.GetReader();
        auto deserial = r.Next<std::string>();
        timer.Stop();
    }
    return timer.Microseconds()/times;
}

int SerialVector(int times) {
    c7a::common::StatsTimer<true> timer(false);
    for (int i = 0; i < times; ++i) {

        timer.Start();
        c7a::data::File f;
        {
            auto w = f.GetWriter();
            w(bv);
        }

        auto r = f.GetReader();
        auto deserial = r.Next<BenchVector>();
        timer.Stop();
    }
    return timer.Microseconds()/times;
}

int SerialTuple(int times) {
    c7a::common::StatsTimer<true> timer(false);
    for (int i = 0; i < times; ++i) {

        timer.Start();
        c7a::data::File f;
        {
            auto w = f.GetWriter();
            w(bench_tuple);
        }

        auto r = f.GetReader();
        auto deserial = r.Next<decltype(bench_tuple)>();
        timer.Stop();
    }
    return timer.Microseconds()/times;
}



int main() {
    std::cout << std::setfill(' ') << std::setw(30) << std::left <<
                 "Serialization of a string:" << SerialString(50) << std::endl;
    std::cout << std::setfill(' ') << std::setw(30) << std::left <<
                 "Serialization of a vector:" << SerialVector(50) << std::endl;
    std::cout << std::setfill(' ') << std::setw(30) << std::left <<
                 "Serialization of a tuple:" << SerialTuple(50) << std::endl;
    return 123;
}



#endif
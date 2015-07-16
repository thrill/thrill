/*******************************************************************************
 * c7a/common/partitioning/trivial_target_determination.hpp
 *
 * Part of Project c7a.
 *
 * TODO(ma): Copyright
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_PARTITIONING_TRIVIAL_TARGET_DETERMINATION_HEADER
#define C7A_COMMON_PARTITIONING_TRIVIAL_TARGET_DETERMINATION_HEADER

#define ROUND_DOWN(x, s) ((x) & ~((s) - 1))

#include <c7a/data/emitter.hpp>

namespace c7a {
namespace sort {

template <class T1, typename CompareFunction>
struct BucketEmitter
{

    static void emitToBuckets(
        const T1* const a,
        const size_t n,
        const T1* const treearr, // Tree. sizeof |splitter|
        size_t k,                // Number of buckets
        size_t logK,
        std::vector<data::Emitter<T1> >& emitters,
        size_t actual_k,
        CompareFunction compare_function) {

        const size_t stepsize = 2;

        size_t i = 0;
        for ( ; i < ROUND_DOWN(n, stepsize); i += stepsize)
        {

            size_t j0 = 1;
            T1 el0 = a[i];
            size_t j1 = 1;
            T1 el1 = a[i + 1];

            for (size_t l = 0; l < logK; l++)
            {

                j0 = j0 * 2 + !(compare_function(el0, treearr[j0]));
                j1 = j1 * 2 + !(compare_function(el1, treearr[j1]));
            }

            size_t b0 = j0 - k;
            size_t b1 = j1 - k;

            //TODO(an): Remove this ugly workaround as soon as emitters are movable.
            //Move emitter[actual_k] to emitter[splitter_count] before calling this.
            if (b0 >= actual_k) {
                emitters[actual_k - 1](el0);
            }
            else {
                emitters[b0](el0);
            }
            if (b1 >= actual_k) {
                emitters[actual_k - 1](el1);
            }
            else {
                emitters[b1](el1);
            }
        }
        for ( ; i < n; i++)
        {

            size_t j = 1;
            for (size_t l = 0; l < logK; l++)
            {
                j = j * 2 + (a[i] >= treearr[j]);
            }
            size_t b = j - k;

            if (b >= actual_k) {
                emitters[actual_k - 1](a[i]);
            }
            else {
                emitters[b](a[i]);
            }
        }
    }
};

#endif // !C7A_COMMON_PARTITIONING_TRIVIAL_TARGET_DETERMINATION_HEADER
} //sort
} //c7a

/******************************************************************************/

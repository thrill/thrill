#pragma once

namespace c7a {
namespace sort {

template <typename ValueType>
struct TreeBuilder
{

    ValueType* tree_;
    ValueType* samples_;
    size_t index_;
    size_t ssplitter;

    TreeBuilder(ValueType* splitter_tree, // Target: tree. Size of 'number of splitter'
                ValueType* samples, // Source: sorted splitters. Size of 'number of splitter'
                size_t ssplitter) // Number of splitter
        : tree_( splitter_tree ),
          samples_( samples ),
          index_( 0 ),
          ssplitter(ssplitter)
    {
        recurse(samples, samples + ssplitter, 1);
    }

    ssize_t snum(ValueType* s) const
    {
        return (ssize_t)(s - samples_);
    }

    ValueType recurse(ValueType* lo, ValueType* hi, unsigned int treeidx)
    {

        // pick middle element as splitter
        ValueType* mid = lo + (ssize_t)(hi - lo) / 2;

        ValueType mykey = tree_[treeidx] = *mid;
        
        ValueType *midlo = mid, *midhi = mid+1;
        
        if (2 * treeidx < ssplitter)
        {
            recurse(lo, midlo, 2 * treeidx + 0);

            return recurse(midhi, hi, 2 * treeidx + 1);
        }
        else
        {
            return mykey;
        }
    }
};
} //sort
} //c7a

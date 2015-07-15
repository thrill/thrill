#pragma once

template <typename value_type>
struct TreeBuilder
{

    value_type* m_tree;
    value_type* m_samples;
    unsigned int m_index;
    size_t ssplitter;

    TreeBuilder(value_type* splitter_tree, // Target: tree. Size of 'number of splitter'
                value_type* samples, // Source: sorted splitters. Size of 'number of splitter'
                size_t ssplitter) // Number of splitter
        : m_tree( splitter_tree ),
          m_samples( samples ),
          m_index( 0 ),
          ssplitter(ssplitter)
    {
        recurse(samples, samples + ssplitter, 1);
    }

    ssize_t snum(value_type* s) const
    {
        return (ssize_t)(s - m_samples);
    }

    value_type recurse(value_type* lo, value_type* hi, unsigned int treeidx)
    {

        // pick middle element as splitter
        value_type* mid = lo + (ssize_t)(hi - lo) / 2;

        value_type mykey = m_tree[treeidx] = *mid;
#if 0
        value_type* midlo = mid;
        while (lo < midlo && *(midlo-1) == mykey) midlo--;

        value_type* midhi = mid;
        while (midhi+1 < hi && *midhi == mykey) midhi++;

#else
        value_type *midlo = mid, *midhi = mid+1;
#endif
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

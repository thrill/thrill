/*******************************************************************************
 * thrill/api/merge.hpp
 *
 * DIANode for a merge operation. Performs the actual merge operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MERGE_HEADER
#define THRILL_API_MERGE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/common/string.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/data/dyn_block_reader.hpp>
#include <thrill/data/file.hpp>
#include <tlx/meta/call_foreach_with_index.hpp>

#include <algorithm>
#include <array>
#include <functional>
#include <random>
#include <string>
#include <vector>

namespace thrill {
namespace api {

/*!
 * Implementation of Thrill's merge. This merge implementation balances all data
 * before merging, so each worker has the same amount of data when merge
 * finishes.
 *
 * The algorithm performs a distributed multi-sequence selection by picking
 * random pivots (from the largest remaining interval) for each DIA. The pivots
 * are selected via a global AllReduce. There is one pivot per DIA.
 *
 * Then the pivots are searched for in the interval [left,left + width) in each
 * local File's partition, where these are initialized with left = 0 and width =
 * File.size(). This delivers the local_rank of each pivot. From the local_ranks
 * the corresponding global_ranks of each pivot is calculated via a AllReduce.
 *
 * The global_ranks are then compared to the target_ranks (which are n/p *
 * rank). If global_ranks is smaller, the interval [left,left + width) is
 * reduced to [left,idx), where idx is the rank of the pivot in the local
 * File. If global_ranks is larger, the interval is reduced to [idx,left+width).
 *
 * left  -> width
 * V            V      V           V         V                   V
 * +------------+      +-----------+         +-------------------+ DIA 0
 *    ^
 *    local_ranks,  global_ranks = sum over all local_ranks
 *
 * \tparam ValueType The type of the first and second input DIA
 * \tparam Comparator The comparator defining input and output order.
 * \tparam ParentDIA0 The type of the first input DIA
 * \tparam ParentDIAs The types of the other input DIAs
 *
 * \ingroup api_layer
 */
template <typename ValueType, typename Comparator, size_t kNumInputs>
class MergeNode : public DOpNode<ValueType>
{
    static constexpr bool debug = false;
    static constexpr bool self_verify = debug && common::g_debug_mode;

    //! Set this variable to true to enable generation and output of merge stats
    static constexpr bool stats_enabled = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    static_assert(kNumInputs >= 2, "Merge requires at least two inputs.");

public:
    template <typename ParentDIA0, typename ... ParentDIAs>
    MergeNode(const Comparator& comparator,
              const ParentDIA0& parent0, const ParentDIAs& ... parents)
        : Super(parent0.ctx(), "Merge",
                { parent0.id(), parents.id() ... },
                { parent0.node(), parents.node() ... }),
          comparator_(comparator),
          // this weirdness is due to a MSVC2015 parser bug
          parent_stack_empty_(
              std::array<bool, kNumInputs>{
                  { ParentDIA0::stack_empty, (ParentDIAs::stack_empty)... }
              })
    {
        // allocate files.
        for (size_t i = 0; i < kNumInputs; ++i)
            files_[i] = context_.GetFilePtr(this);

        for (size_t i = 0; i < kNumInputs; ++i)
            writers_[i] = files_[i]->GetWriter();

        tlx::call_foreach_with_index(
            RegisterParent(this), parent0, parents ...);
    }

    //! Register Parent PreOp Hooks, instantiated and called for each Merge
    //! parent
    class RegisterParent
    {
    public:
        explicit RegisterParent(MergeNode* merge_node)
            : merge_node_(merge_node) { }

        template <typename Index, typename Parent>
        void operator () (const Index&, Parent& parent) {

            // construct lambda with only the writer in the closure
            data::File::Writer* writer = &merge_node_->writers_[Index::index];
            auto pre_op_fn = [writer](const ValueType& input) -> void {
                                 writer->Put(input);
                             };

            // close the function stacks with our pre ops and register it at
            // parent nodes for output
            auto lop_chain = parent.stack().push(pre_op_fn).fold();

            parent.node()->AddChild(merge_node_, lop_chain, Index::index);
        }

    private:
        MergeNode* merge_node_;
    };

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t parent_index) final {
        assert(parent_index < kNumInputs);
        if (!parent_stack_empty_[parent_index]) return false;

        // accept file
        assert(files_[parent_index]->num_items() == 0);
        *files_[parent_index] = file.Copy();
        return true;
    }

    void StopPreOp(size_t id) final {
        writers_[id].Close();
    }

    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {
        size_t result_count = 0;
        static constexpr bool debug = false;

        stats_.merge_timer_.Start();

        // get inbound readers from all Channels
        std::vector<data::CatStream::CatReader> readers;
        readers.reserve(kNumInputs);

        for (size_t i = 0; i < kNumInputs; i++)
            readers.emplace_back(streams_[i]->GetCatReader(consume));

        auto puller = core::make_multiway_merge_tree<ValueType>(
            readers.begin(), readers.end(), comparator_);

        while (puller.HasNext())
            this->PushItem(puller.Next());

        stats_.merge_timer_.Stop();

        sLOG << "Merge: result_count" << result_count;

        stats_.result_size_ = result_count;
        stats_.Print(context_);
    }

    void Dispose() final { }

private:
    //! Merge comparator
    Comparator comparator_;

    //! Whether the parent stack is empty
    const std::array<bool, kNumInputs> parent_stack_empty_;

    //! Random generator for pivot selection.
    std::default_random_engine rng_ { std::random_device { } () };

    //! Files for intermediate storage
    data::FilePtr files_[kNumInputs];

    //! Writers to intermediate files
    data::File::Writer writers_[kNumInputs];

    //! Array of inbound CatStreams
    data::CatStreamPtr streams_[kNumInputs];

    struct Pivot {
        ValueType value;
        size_t    tie_idx;
        size_t    segment_len;
    };

    //! Count of items on all prev workers.
    size_t prefix_size_;

    using ArrayNumInputsSizeT = std::array<size_t, kNumInputs>;

    //! Logging helper to print vectors of arrays of size_t
    static std::string
    VecVecToStr(const std::vector<ArrayNumInputsSizeT>& data) {
        std::ostringstream oss;
        for (typename std::vector<ArrayNumInputsSizeT>::const_iterator
             it = data.begin(); it != data.end(); ++it)
        {
            if (it != data.begin()) oss << " # ";
            oss << common::VecToStr(*it);
        }
        return oss.str();
    }

    //! Logging helper to print vectors of vectors of pivots.
    static std::string VToStr(const std::vector<Pivot>& data) {
        std::stringstream oss;
        for (const Pivot& elem : data) {
            oss << "(" << elem.value
                << ", itie: " << elem.tie_idx
                << ", len: " << elem.segment_len << ") ";
        }
        return oss.str();
    }

    //! Reduce functor that returns the pivot originating from the biggest
    //! range.  That removes some nasty corner cases, like selecting the same
    //! pivot over and over again from a tiny range.
    class ReducePivots
    {
    public:
        Pivot operator () (const Pivot& a, const Pivot& b) const {
            return a.segment_len > b.segment_len ? a : b;
        }
    };

    using StatsTimer = common::StatsTimerBaseStopped<stats_enabled>;

    /*!
     * Stats holds timers for measuring merge performance, that supports
     * accumulating the output and printing it to the standard out stream.
     */
    class Stats
    {
    public:
        //! A Timer accumulating all time spent in File operations.
        StatsTimer file_op_timer_;
        //! A Timer accumulating all time spent while actually merging.
        StatsTimer merge_timer_;
        //! A Timer accumulating all time spent while re-balancing the data.
        StatsTimer balancing_timer_;
        //! A Timer accumulating all time spent for selecting the global pivot
        //! elements.
        StatsTimer pivot_selection_timer_;
        //! A Timer accumulating all time spent in global search steps.
        StatsTimer search_step_timer_;
        //! A Timer accumulating all time spent communicating.
        StatsTimer comm_timer_;
        //! A Timer accumulating all time spent calling the scatter method of
        //! the data subsystem.
        StatsTimer scatter_timer_;
        //! The count of all elements processed on this host.
        size_t result_size_ = 0;
        //! The count of search iterations needed for balancing.
        size_t iterations_ = 0;

        void PrintToSQLPlotTool(
            const std::string& label, size_t p, size_t value) {

            LOG1 << "RESULT " << "operation=" << label << " time=" << value
                 << " workers=" << p << " result_size_=" << result_size_;
        }

        void Print(Context& ctx) {
            if (stats_enabled) {
                size_t p = ctx.num_workers();
                size_t merge =
                    ctx.net.AllReduce(merge_timer_.Milliseconds()) / p;
                size_t balance =
                    ctx.net.AllReduce(balancing_timer_.Milliseconds()) / p;
                size_t pivot_selection =
                    ctx.net.AllReduce(pivot_selection_timer_.Milliseconds()) / p;
                size_t search_step =
                    ctx.net.AllReduce(search_step_timer_.Milliseconds()) / p;
                size_t file_op =
                    ctx.net.AllReduce(file_op_timer_.Milliseconds()) / p;
                size_t comm =
                    ctx.net.AllReduce(comm_timer_.Milliseconds()) / p;
                size_t scatter =
                    ctx.net.AllReduce(scatter_timer_.Milliseconds()) / p;

                result_size_ = ctx.net.AllReduce(result_size_);

                if (ctx.my_rank() == 0) {
                    PrintToSQLPlotTool("merge", p, merge);
                    PrintToSQLPlotTool("balance", p, balance);
                    PrintToSQLPlotTool("pivot_selection", p, pivot_selection);
                    PrintToSQLPlotTool("search_step", p, search_step);
                    PrintToSQLPlotTool("file_op", p, file_op);
                    PrintToSQLPlotTool("communication", p, comm);
                    PrintToSQLPlotTool("scatter", p, scatter);
                    PrintToSQLPlotTool("iterations", p, iterations_);
                }
            }
        }
    };

    //! Instance of merge statistics
    Stats stats_;

    /*!
     * Selects random global pivots for all splitter searches based on all
     * worker's search ranges.
     *
     * \param left The left bounds of all search ranges for all files.  The
     * first index identifies the splitter, the second index identifies the
     * file.
     *
     * \param width The width of all search ranges for all files.  The first
     * index identifies the splitter, the second index identifies the file.
     *
     * \param out_pivots The output pivots.
     */
    void SelectPivots(
        const std::vector<ArrayNumInputsSizeT>& left,
        const std::vector<ArrayNumInputsSizeT>& width,
        std::vector<Pivot>& out_pivots) {

        // Select a random pivot for the largest range we have for each
        // splitter.
        for (size_t s = 0; s < width.size(); s++) {
            size_t mp = 0;

            // Search for the largest range.
            for (size_t p = 1; p < width[s].size(); p++) {
                if (width[s][p] > width[s][mp]) {
                    mp = p;
                }
            }

            // We can leave pivot_elem uninitialized.  If it is not initialized
            // below, then an other worker's pivot will be taken for this range,
            // since our range is zero.
            ValueType pivot_elem = ValueType();
            size_t pivot_idx = left[s][mp];

            if (width[s][mp] > 0) {
                pivot_idx = left[s][mp] + (rng_() % width[s][mp]);
                assert(pivot_idx < files_[mp]->num_items());
                stats_.file_op_timer_.Start();
                pivot_elem = files_[mp]->template GetItemAt<ValueType>(pivot_idx);
                stats_.file_op_timer_.Stop();
            }

            out_pivots[s] = Pivot {
                pivot_elem,
                pivot_idx,
                width[s][mp]
            };
        }

        LOG << "local pivots: " << VToStr(out_pivots);

        // Reduce vectors of pivots globally to select the pivots from the
        // largest ranges.
        stats_.comm_timer_.Start();
        out_pivots = context_.net.AllReduce(
            out_pivots, common::ComponentSum<std::vector<Pivot>, ReducePivots>());
        stats_.comm_timer_.Stop();
    }

    /*!
     * Calculates the global ranks of the given pivots.
     * Additionally returns the local ranks so we can use them in the next step.
     */
    void GetGlobalRanks(
        const std::vector<Pivot>& pivots,
        std::vector<size_t>& global_ranks,
        std::vector<ArrayNumInputsSizeT>& out_local_ranks,
        const std::vector<ArrayNumInputsSizeT>& left,
        const std::vector<ArrayNumInputsSizeT>& width) {

        // Simply get the rank of each pivot in each file. Sum the ranks up
        // locally.
        for (size_t s = 0; s < pivots.size(); s++) {
            size_t rank = 0;
            for (size_t i = 0; i < kNumInputs; i++) {
                stats_.file_op_timer_.Start();

                size_t idx = files_[i]->GetIndexOf(
                    pivots[s].value, pivots[s].tie_idx,
                    left[s][i], left[s][i] + width[s][i],
                    comparator_);

                stats_.file_op_timer_.Stop();

                rank += idx;
                out_local_ranks[s][i] = idx;
            }
            global_ranks[s] = rank;
        }

        stats_.comm_timer_.Start();
        // Sum up ranks globally.
        global_ranks = context_.net.AllReduce(
            global_ranks, common::ComponentSum<std::vector<size_t> >());
        stats_.comm_timer_.Stop();
    }

    /*!
     * Shrinks the search ranges according to the global ranks of the pivots.
     *
     * \param global_ranks The global ranks of all pivots.
     *
     * \param local_ranks The local ranks of each pivot in each file.
     *
     * \param target_ranks The desired ranks of the splitters we are looking
     * for.
     *
     * \param left The left bounds of all search ranges for all files.  The
     * first index identifies the splitter, the second index identifies the
     * file.  This parameter will be modified.
     *
     * \param width The width of all search ranges for all files.  The first
     * index identifies the splitter, the second index identifies the file.
     * This parameter will be modified.
     */
    void SearchStep(
        const std::vector<size_t>& global_ranks,
        const std::vector<ArrayNumInputsSizeT>& local_ranks,
        const std::vector<size_t>& target_ranks,
        std::vector<ArrayNumInputsSizeT>& left,
        std::vector<ArrayNumInputsSizeT>& width) {

        for (size_t s = 0; s < width.size(); s++) {
            for (size_t p = 0; p < width[s].size(); p++) {

                if (width[s][p] == 0)
                    continue;

                size_t local_rank = local_ranks[s][p];
                size_t old_width = width[s][p];
                assert(left[s][p] <= local_rank);

                if (global_ranks[s] < target_ranks[s]) {
                    width[s][p] -= local_rank - left[s][p];
                    left[s][p] = local_rank;
                }
                else if (global_ranks[s] >= target_ranks[s]) {
                    width[s][p] = local_rank - left[s][p];
                }

                if (debug) {
                    die_unless(width[s][p] <= old_width);
                }
            }
        }
    }

    /*!
     * Receives elements from other workers and re-balance them, so each worker
     * has the same amount after merging.
     */
    void MainOp() {
        // *** Setup Environment for merging ***

        // Count of all workers (and count of target partitions)
        size_t p = context_.num_workers();
        LOG << "splitting to " << p << " workers";

        // Count of all local elements.
        size_t local_size = 0;

        for (size_t i = 0; i < kNumInputs; i++) {
            local_size += files_[i]->num_items();
        }

        // test that the data we got is sorted!
        if (self_verify) {
            for (size_t i = 0; i < kNumInputs; i++) {
                auto reader = files_[i]->GetKeepReader();
                if (!reader.HasNext()) continue;

                ValueType prev = reader.template Next<ValueType>();
                while (reader.HasNext()) {
                    ValueType next = reader.template Next<ValueType>();
                    if (comparator_(next, prev)) {
                        die("Merge input was not sorted!");
                    }
                    prev = std::move(next);
                }
            }
        }

        // Count of all global elements.
        stats_.comm_timer_.Start();
        size_t global_size = context_.net.AllReduce(local_size);
        stats_.comm_timer_.Stop();

        LOG << "local size: " << local_size;
        LOG << "global size: " << global_size;

        // Calculate and remember the ranks we search for.  In our case, we
        // search for ranks that split the data into equal parts.
        std::vector<size_t> target_ranks(p - 1);

        for (size_t r = 0; r < p - 1; r++) {
            target_ranks[r] = (global_size / p) * (r + 1);
            // Modify all ranks 0..(globalSize % p), in case global_size is not
            // divisible by p.
            if (r < global_size % p)
                target_ranks[r] += 1;
        }

        if (debug) {
            LOG << "target_ranks: " << common::VecToStr(target_ranks);

            stats_.comm_timer_.Start();
            assert(context_.net.Broadcast(target_ranks) == target_ranks);
            stats_.comm_timer_.Stop();
        }

        // buffer for the global ranks of selected pivots
        std::vector<size_t> global_ranks(p - 1);

        // Search range bounds.
        std::vector<ArrayNumInputsSizeT> left(p - 1), width(p - 1);

        // Auxillary arrays.
        std::vector<Pivot> pivots(p - 1);
        std::vector<ArrayNumInputsSizeT> local_ranks(p - 1);

        // Initialize all lefts with 0 and all widths with size of their
        // respective file.
        for (size_t r = 0; r < p - 1; r++) {
            for (size_t q = 0; q < kNumInputs; q++) {
                width[r][q] = files_[q]->num_items();
            }
        }

        bool finished = false;
        stats_.balancing_timer_.Start();

        // Iterate until we find a pivot which is within the prescribed balance
        // tolerance
        while (!finished) {

            LOG << "iteration: " << stats_.iterations_;
            LOG0 << "left: " << VecVecToStr(left);
            LOG0 << "width: " << VecVecToStr(width);

            if (debug) {
                for (size_t q = 0; q < kNumInputs; q++) {
                    std::ostringstream oss;
                    for (size_t i = 0; i < p - 1; ++i) {
                        if (i != 0) oss << " # ";
                        oss << '[' << left[i][q] << ',' << left[i][q] + width[i][q] << ')';
                    }
                    LOG1 << "left/right[" << q << "]: " << oss.str();
                }
            }

            // Find pivots.
            stats_.pivot_selection_timer_.Start();
            SelectPivots(left, width, pivots);
            stats_.pivot_selection_timer_.Stop();

            LOG << "final pivots: " << VToStr(pivots);

            // Get global ranks and shrink ranges.
            stats_.search_step_timer_.Start();
            GetGlobalRanks(pivots, global_ranks, local_ranks, left, width);

            LOG << "global_ranks: " << common::VecToStr(global_ranks);
            LOG << "local_ranks: " << VecVecToStr(local_ranks);

            SearchStep(global_ranks, local_ranks, target_ranks, left, width);

            if (debug) {
                for (size_t q = 0; q < kNumInputs; q++) {
                    std::ostringstream oss;
                    for (size_t i = 0; i < p - 1; ++i) {
                        if (i != 0) oss << " # ";
                        oss << '[' << left[i][q] << ',' << left[i][q] + width[i][q] << ')';
                    }
                    LOG1 << "left/right[" << q << "]: " << oss.str();
                }
            }

            // We check for accuracy of kNumInputs + 1
            finished = true;
            for (size_t i = 0; i < p - 1; i++) {
                size_t a = global_ranks[i], b = target_ranks[i];
                if (common::abs_diff(a, b) > kNumInputs + 1) {
                    finished = false;
                    break;
                }
            }

            stats_.search_step_timer_.Stop();
            stats_.iterations_++;
        }
        stats_.balancing_timer_.Stop();

        LOG << "Finished after " << stats_.iterations_ << " iterations";

        LOG << "Creating channels";

        // Initialize channels for distributing data.
        for (size_t j = 0; j < kNumInputs; j++)
            streams_[j] = context_.GetNewCatStream(this);

        stats_.scatter_timer_.Start();

        LOG << "Scattering.";

        // For each file, initialize an array of offsets according to the
        // splitters we found. Then call Scatter to distribute the data.

        std::vector<size_t> tx_items(p);
        for (size_t j = 0; j < kNumInputs; j++) {

            std::vector<size_t> offsets(p + 1, 0);

            for (size_t r = 0; r < p - 1; r++)
                offsets[r + 1] = local_ranks[r][j];

            offsets[p] = files_[j]->num_items();

            LOG << "Scatter from file " << j << " to other workers: "
                << common::VecToStr(offsets);

            for (size_t r = 0; r < p; ++r) {
                tx_items[r] += offsets[r + 1] - offsets[r];
            }

            streams_[j]->template Scatter<ValueType>(
                *files_[j], offsets, /* consume */ true);
        }

        LOG << "tx_items: " << common::VecToStr(tx_items);

        // calculate total items on each worker after Scatter
        tx_items = context_.net.AllReduce(
            tx_items, common::ComponentSum<std::vector<size_t> >());
        if (context_.my_rank() == 0)
            LOG1 << "Merge(): total_items: " << common::VecToStr(tx_items);

        stats_.scatter_timer_.Stop();
    }
};

/*!
 * Merge is a DOp, which merges any number of sorted DIAs to a single sorted
 * DIA.  All input DIAs must be sorted conforming to the given comparator.  The
 * type of the output DIA will be the type of this DIA.
 *
 * The merge operation balances all input data, so that each worker will have an
 * equal number of elements when the merge completes.
 *
 * \tparam Comparator Comparator to specify the order of input and output.
 *
 * \param comparator Comparator to specify the order of input and output.
 *
 * \param first_dia first DIA
 * \param dias DIAs, which is merged with this DIA.
 *
 * \ingroup dia_dops
 */
template <typename Comparator, typename FirstDIA, typename ... DIAs>
auto Merge(const Comparator& comparator,
           const FirstDIA& first_dia, const DIAs& ... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    using ValueType = typename FirstDIA::ValueType;

    using CompareResult =
              typename common::FunctionTraits<Comparator>::result_type;

    using MergeNode = api::MergeNode<
              ValueType, Comparator, 1 + sizeof ... (DIAs)>;

    // Assert comparator types.
    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<Comparator>::template arg<0>
            >::value,
        "Comparator has the wrong input type in argument 0");

    static_assert(
        std::is_convertible<
            ValueType,
            typename common::FunctionTraits<Comparator>::template arg<1>
            >::value,
        "Comparator has the wrong input type in argument 1");

    // Assert meaningful return type of comperator.
    static_assert(
        std::is_convertible<
            CompareResult,
            bool
            >::value,
        "Comparator must return bool");

    auto merge_node =
        tlx::make_counting<MergeNode>(comparator, first_dia, dias ...);

    return DIA<ValueType>(merge_node);
}

template <typename ValueType, typename Stack>
template <typename Comparator, typename SecondDIA>
auto DIA<ValueType, Stack>::Merge(
    const SecondDIA& second_dia, const Comparator& comparator) const {
    return api::Merge(comparator, *this, second_dia);
}

} // namespace api

//! imported from api namespace
using api::Merge;

} // namespace thrill

#endif // !THRILL_API_MERGE_HEADER

/******************************************************************************/

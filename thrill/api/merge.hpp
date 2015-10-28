/*******************************************************************************
 * thrill/api/merge.hpp
 *
 * DIANode for a merge operation. Performs the actual merge operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MERGE_HEADER
#define THRILL_API_MERGE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/meta.hpp>
#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/losertree.hpp>
#include <thrill/data/dyn_block_reader.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * merge_local holds functions internally used by merge.
 */
namespace merge_local {

//! Set this variable to true to enable generation and output of merge stats.
static const bool stats_enabled = false;

using StatsTimer = common::StatsTimer<stats_enabled>;

/*!
 * MergeStatsBase holds timers for measuring merge performance.
 */
class MergeStatsBase
{
public:
    //! A Timer accumulating all time spent in File operations.
    StatsTimer file_op_timer_;
    //! A Timer accumulating all time spent while actually merging.
    StatsTimer merge_timer_;
    //! A Timer accumulating all time spent while re-balancing the data.
    StatsTimer balancing_timer_;
    //! A Timer accumulating all time spent for selecting the global pivot elements.
    StatsTimer pivot_selection_timer_;
    //! A Timer accumulating all time spent in global search steps.
    StatsTimer search_step_timer_;
    //! A Timer accumulating all time spent communicating.
    StatsTimer comm_timer_;
    //! A Timer accumulating all time spent calling the scatter method of the data subsystem.
    StatsTimer scatter_timer_;
    //! The count of all elements processed on this host.
    size_t result_size_ = 0;
    //! The count of search iterations needed for balancing.
    size_t iterations_ = 0;
};

/*!
 * MergeStats is an implementation of MergeStatsBase, that supports accumulating
 * the output and printing it to the standard out stream.
 */
class MergeStats : public MergeStatsBase
{
public:
    void PrintToSQLPlotTool(const std::string& label, size_t p, size_t value) {
        static const bool debug = true;

        LOG << "RESULT " << "operation=" << label << " time=" << value
            << " workers=" << p << " result_size_=" << result_size_;
    }

    void Print(Context& ctx) {
        if (stats_enabled) {

            size_t p = ctx.num_workers();

            size_t merge = ctx.AllReduce(merge_timer_.Milliseconds()) / p;
            size_t balance = ctx.AllReduce(balancing_timer_.Milliseconds()) / p;
            size_t pivot_selection = ctx.AllReduce(pivot_selection_timer_.Milliseconds()) / p;
            size_t search_step = ctx.AllReduce(search_step_timer_.Milliseconds()) / p;
            size_t file_op = ctx.AllReduce(file_op_timer_.Milliseconds()) / p;
            size_t comm = ctx.AllReduce(comm_timer_.Milliseconds()) / p;
            size_t scatter = ctx.AllReduce(scatter_timer_.Milliseconds()) / p;
            result_size_ = ctx.AllReduce(result_size_);

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

} // namespace merge_local

/*!
 * Implementation of Thrill's merge. This merge implementation balances all data
 * before merging, so each worker has the same amount of data when merge
 * finishes.
 *
 * \tparam ValueType The type of the first and second input DIA
 * \tparam Comparator The comparator defining input and output order.
 * \tparam ParentDIA0 The type of the first input DIA
 * \tparam ParentDIAs The types of the other input DIAs
 */
template <typename ValueType, typename Comparator,
          typename ParentDIA0,
          typename ... ParentDIAs>
class MergeNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    //! Instance of merge statistics
    merge_local::MergeStats stats_;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 1 + sizeof ... (ParentDIAs);

    static_assert(num_inputs_ >= 2, "Merge requires at least two inputs.");

public:
    MergeNode(const Comparator& comparator,
              StatsNode* stats_node,
              const ParentDIA0& parent0,
              const ParentDIAs& ... parents)
        : DOpNode<ValueType>(parent0.ctx(),
                             { parent0.node(), parents.node() ... }, stats_node),
          comparator_(comparator)
    {
        // allocate files.
        for (size_t i = 0; i < num_inputs_; ++i)
            files_[i] = context_.GetFilePtr();

        for (size_t i = 0; i < num_inputs_; ++i)
            writers_[i] = files_[i]->GetWriter();

        common::VarCallForeachIndex(
            RegisterParent(this), parent0, parents ...);
    }

    void StopPreOp(size_t id) final {
        writers_[id].Close();
    }

    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {
        size_t result_count = 0;
        static const bool debug = false;

        LOG << "Entering Main OP";

        stats_.merge_timer_.Start();

        using Reader = data::BufferedBlockReader<
                  ValueType, data::CatBlockSource<data::DynBlockSource> >;

        // get buffered inbound readers from all Channels
        std::vector<Reader> readers;
        for (size_t i = 0; i < num_inputs_; i++) {
            readers.emplace_back(std::move(streams_[i]->GetCatBlockSource(consume)));
        }
        // init the looser-tree
        using LoserTreeType = core::LoserTreePointer<true, ValueType, Comparator>;

        LoserTreeType lt(num_inputs_, comparator_);

        // Arbitrary element (copied!)
        std::unique_ptr<ValueType> zero;

        size_t completed = 0;

        // Find abritary elem.
        for (size_t i = 0; i < num_inputs_; i++) {
            if (readers[i].HasValue()) {
                zero = std::make_unique<ValueType>(readers[i].Value());
                break;
            }
        }

        if (zero) { //If so, we only have empty channels.
            // Insert abritray element for each empty reader.
            for (size_t i = 0; i < num_inputs_; i++) {
                if (!readers[i].HasValue()) {
                    lt.insert_start(*zero, i, true);
                    completed++;
                }
                else {
                    lt.insert_start(readers[i].Value(), i, false);
                }
            }
            lt.init();

            while (completed < num_inputs_) {

                size_t min = lt.get_min_source();

                auto& reader = readers[min];
                assert(reader.HasValue());

                this->PushItem(reader.Value());

                reader.Next();

                if (reader.HasValue()) {
                    lt.delete_min_insert(reader.Value(), false);
                }
                else {
                    lt.delete_min_insert(*zero, true);
                    completed++;
                }

                result_count++;
            }
        }

        stats_.merge_timer_.Stop();

        sLOG << "Merge: result_count" << result_count;

        stats_.result_size_ = result_count;
        stats_.Print(context_);
    }

    void Dispose() final { }

private:
    //! Merge comparator
    Comparator comparator_;

    //! Random generator for pivot selection.
    std::default_random_engine rng_ { std::random_device { } () };

    //! Files for intermediate storage
    std::array<data::FilePtr, num_inputs_> files_;

    //! Writers to intermediate files
    std::array<data::File::Writer, num_inputs_> writers_;

    //! Array of inbound CatStreams
    std::array<data::CatStreamPtr, num_inputs_> streams_;

    struct Pivot {
        ValueType value;
        size_t    tie_idx;
        size_t    segment_len;
    };

    //! Count of items on this worker.
    size_t data_size_;
    //! Count of items on all prev workers.
    size_t prefix_size_;

    //! Logging helper to print vectors.
    template <typename T>
    std::string VToStr(const std::vector<T>& data) {
        std::stringstream ss;

        for (const T& elem : data)
            ss << elem << " ";

        return ss.str();
    }

    //! Logging helper to print vectors of vectors of size_t
    std::string VToStr(const std::vector<std::vector<size_t> >& data) {
        std::stringstream ss;

        for (const std::vector<size_t>& elem : data) {
            ss << VToStr(elem) << " ## ";
        }

        return ss.str();
    }

    //! Logging helper to print vectors of vectors of pivots.
    std::string VToStr(const std::vector<Pivot>& data) {
        std::stringstream ss;

        for (const Pivot& elem : data)
            ss << "(" << elem.value << ", itie: " << elem.tie_idx << ", len: " << elem.segment_len << ") ";

        return ss.str();
    }

    //! Helper method that adds two size_t Vector. This is used
    //! for global reduce operations.
    static std::vector<size_t> AddSizeTVectors
        (const std::vector<size_t>& a, const std::vector<size_t>& b) {
        assert(a.size() == b.size());
        std::vector<size_t> res(a.size());
        for (size_t i = 0; i < a.size(); i++) {
            res[i] = a[i] + b[i];
        }
        return res;
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
                                 writer->PutItem(input);
                             };

            // close the function stacks with our pre ops and register it at
            // parent nodes for output
            auto lop_chain = parent.stack().push(pre_op_fn).emit();

            parent.node()->RegisterChild(lop_chain, merge_node_->type());
        }

    protected:
        MergeNode* merge_node_;
    };

    /*!
     * Selects random global pivots for all splitter searches based on all
     * worker's search ranges.
     *
     * \param pivots The output pivots.
     *
     * \param left The left bounds of all search ranges for all files.  The
     * first index identifies the splitter, the second index identifies the
     * file.
     *
     * \param width The width of all search ranges for all files.  The first
     * index identifies the splitter, the second index identifies the file.
     */
    // dim 1: Different splitters, dim 2: different files
    void SelectPivots(
        std::vector<Pivot>& pivots,
        const std::vector<std::vector<size_t> >& left,
        const std::vector<std::vector<size_t> >& width) {

        // Select a random pivot for the largest range we have
        // For each splitter.
        for (size_t s = 0; s < width.size(); s++) {
            size_t mp = 0;

            // Search for the largest range.
            for (size_t p = 1; p < width[s].size(); p++) {
                if (width[s][p] > width[s][mp]) {
                    mp = p;
                }
            }

            // We can leave pivotElem uninitialized.  If it is not initialized
            // below, then an other worker's pivot will be taken for this range,
            // since our range is zero.
            ValueType pivot_elem = ValueType();
            size_t pivot_idx = left[s][mp];

            if (width[s][mp] > 0) {
                pivot_idx = left[s][mp] + (rng_() % width[s][mp]);
                stats_.file_op_timer_.Start();
                pivot_elem = files_[mp]->template GetItemAt<ValueType>(pivot_idx);
                stats_.file_op_timer_.Stop();
            }

            pivots[s] = Pivot {
                pivot_elem,
                pivot_idx,
                width[s][mp]
            };
        }

        LOG << "Local Pivots " << VToStr(pivots);

        // Distribute pivots globally.

        // Reduce function that returns the pivot originating from the biggest
        // range.  That removes some nasty corner cases, like selecting the same
        // pivot over and over again from a tiny range.
        auto reduce_pivots = [](const Pivot& a, const Pivot& b) {
                                 if (a.segment_len > b.segment_len) {
                                     return a;
                                 }
                                 else {
                                     return b;
                                 }
                             };

        stats_.comm_timer_.Start();

        // Reduce vectors of pivots globally to select the pivots from the
        // largest ranges.
        pivots = context_.AllReduce(
            pivots,
            [reduce_pivots]
                (const std::vector<Pivot>& a, const std::vector<Pivot>& b) {
                assert(a.size() == b.size());
                std::vector<Pivot> res(a.size());
                for (size_t i = 0; i < a.size(); i++) {
                    res[i] = reduce_pivots(a[i], b[i]);
                }
                return res;
            });
        stats_.comm_timer_.Stop();
    }

    /*!
     * Calculates the global ranks of the given pivots.
     * Additionally retruns the local ranks so we can use them in the next step.
     *
     * \param pivots The pivots.
     *
     * \param ranks The global ranks of the pivots. This is an output parameter.
     *
     * \params local_ranks The local ranks. The first index corresponds to the
     * splitter, the second one to the file. This is an output parameter.
     */
    void GetGlobalRanks(
        const std::vector<Pivot>& pivots,
        std::vector<size_t>& ranks,
        std::vector<std::vector<size_t> >& local_ranks) {

        // Simply get the rank of each pivot in each file.
        // Sum the ranks up locally.
        for (size_t s = 0; s < pivots.size(); s++) {
            size_t rank = 0;
            for (size_t i = 0; i < num_inputs_; i++) {
                stats_.file_op_timer_.Start();
                size_t idx = files_[i]->GetIndexOf(pivots[s].value, pivots[s].tie_idx, comparator_);
                stats_.file_op_timer_.Stop();

                rank += idx;

                local_ranks[s][i] = idx;
            }
            ranks[s] = rank;
        }

        stats_.comm_timer_.Start();
        // Sum up ranks globally.
        ranks = context_.AllReduce(ranks, &AddSizeTVectors);
        stats_.comm_timer_.Stop();
    }

    /*!
     * Shrinks the search ranges accoring to the global ranks of the pivots.
     *
     * \param ranks The global ranks of all pivots.
     * \param local_ranks The local ranks of each pivot in each file.
     * \param target_ranks The desired ranks of the splitters we are looking for.
     * \param left The left bounds of all search ranges for all files.
     *             The first index identifies the splitter, the second index identifies the file.
     *             This parameter will be modified.
     * \param width The width of all search ranges for all files.
     *              The first index identifies the splitter, the second index identifies the file.
     *             This parameter will be modified.
     *
     * TODO: This implementation
     * suffers from an off-by-one error when there is only
     * a single global range left per splitter. The binary search never
     * terminates because the rank never gets zero. The result is
     * the sloppy termination condition in MainOp and a worst-case
     * balancing error of p * m, with p equals number of workers, m number of files.
     */
    void SearchStep(
        const std::vector<size_t>& ranks,
        std::vector<std::vector<size_t> >& local_ranks,
        const std::vector<size_t>& target_ranks,
        std::vector<std::vector<size_t> >& left,
        std::vector<std::vector<size_t> >& width) {

        // This is basically a binary search for each
        // splitter and each file.
        for (size_t s = 0; s < width.size(); s++) {

            for (size_t p = 0; p < width[s].size(); p++) {

                if (width[s][p] == 0)
                    continue;

                size_t idx = local_ranks[s][p];
                size_t old_width = width[s][p];

                if (ranks[s] <= target_ranks[s]) {
                    width[s][p] -= idx - left[s][p];
                    left[s][p] = idx;
                }
                else if (ranks[s] > target_ranks[s]) {
                    width[s][p] = idx - left[s][p];
                }

                if (debug) {
                    die_unless(old_width >= width[s][p]);
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

        LOG << "Splitting to " << p << " workers";

        // Count of all local elements.
        data_size_ = 0;

        for (size_t i = 0; i < files_.size(); i++) {
            data_size_ += files_[i]->num_items();
        }

        // Count of all global elements.
        stats_.comm_timer_.Start();
        size_t global_size = context_.AllReduce(data_size_);
        stats_.comm_timer_.Stop();

        LOG << "Global size: " << global_size;

        // Calculate and remember the ranks we search for.  In our case, we
        // search for ranks that split the data into equal parts.
        std::vector<size_t> target_ranks(p - 1);
        std::vector<size_t> global_ranks(p - 1);

        for (size_t r = 0; r < p - 1; r++) {
            target_ranks[r] = (global_size / p) * (r + 1);
        }

        // Modify all ranks 0..(globalSize % p), in case global_size is not
        // divisible by p.
        for (size_t r = 0; r < global_size % p; r++) {
            target_ranks[r] += 1;
        }

        if (debug) {
            for (size_t r = 0; r < p - 1; r++) {
                LOG << "Search Rank " << r << ": " << target_ranks[r];

                stats_.comm_timer_.Start();
                assert(context_.Broadcast(target_ranks[r]) == target_ranks[r]);
                stats_.comm_timer_.Stop();
            }
        }

        // Search range bounds.
        std::vector<std::vector<size_t> > left(p - 1);
        std::vector<std::vector<size_t> > width(p - 1);

        // Auxillary arrays.
        std::vector<Pivot> pivots(p - 1);
        std::vector<std::vector<size_t> > local_ranks(p - 1);

        // Initialize all lefts with 0 and all widths with size of their
        // respective file.
        for (size_t r = 0; r < p - 1; r++) {
            left[r] = std::vector<size_t>(num_inputs_);
            width[r] = std::vector<size_t>(num_inputs_);
            local_ranks[r] = std::vector<size_t>(num_inputs_);
            std::fill(left[r].begin(), left[r].end(), 0);

            for (size_t q = 0; q < num_inputs_; q++) {
                width[r][q] = files_[q]->num_items();
            }
        }

        bool finished = false;
        stats_.balancing_timer_.Start();

        // TODO: Iterate until all splitters are found.
        // Theoretically, the condition
        //
        // while(global_ranks != target_ranks)
        //
        // could be used here. If the binary-search error
        // mentioned in SearchStep is fixed.
        while (!finished) {

            LOG << "left: " << VToStr(left);
            LOG << "width: " << VToStr(width);

            // Find pivots.
            stats_.pivot_selection_timer_.Start();
            SelectPivots(pivots, left, width);
            stats_.pivot_selection_timer_.Stop();

            LOG << "Final Pivots " << VToStr(pivots);

            // Get global ranks and shrink ranges.
            stats_.search_step_timer_.Start();
            GetGlobalRanks(pivots, global_ranks, local_ranks);
            SearchStep(global_ranks, local_ranks, target_ranks, left, width);

            // Check if all our ranges have at most size one.
            // TODO: This can potentially be omitted. See comment
            // above.
            finished = true;

            // TODO: We check for accuracy of num_inputs_ + 1
            // There is a off-by one error in the last search step.
            // We need special treatment of search ranges with width 1, when the pivot
            // originates from our host.
            // An error of num_inputs_ + 1 is the worst case.
            for (size_t i = 0; i < p - 1; i++) {
                size_t a = global_ranks[i];
                size_t b = target_ranks[i];
                if ((a > b && a - b > num_inputs_ + 1) || (b > a && b - a > num_inputs_ + 1)) {
                    finished = false;
                    break;
                }
            }

            LOG << "srank: " << VToStr(target_ranks);
            LOG << "grank: " << VToStr(global_ranks);

            stats_.search_step_timer_.Stop();
            stats_.iterations_++;
        }
        stats_.balancing_timer_.Stop();

        LOG << "Creating channels";

        // Initialize channels for distributing data.
        for (size_t j = 0; j < num_inputs_; j++) {
            streams_[j] = context_.GetNewCatStream();
        }

        stats_.scatter_timer_.Start();

        LOG << "Scattering.";

        // For each file, initialize an array of offsets according
        // to the splitters we found. Then call scatter to distribute the data.
        for (size_t j = 0; j < num_inputs_; j++) {

            std::vector<size_t> offsets(p);

            for (size_t r = 0; r < p - 1; r++) {
                offsets[r] = left[r][j];
            }

            offsets[p - 1] = files_[j]->num_items();

            for (size_t i = 0; i < p; i++) {
                LOG << "Offset " << i << " for file " << j << ": " << VToStr(offsets);
            }

            streams_[j]->template Scatter<ValueType>(*files_[j], offsets);
        }
        stats_.scatter_timer_.Stop();
    }
};

template <typename ValueType, typename Stack>
template <typename Comparator, typename ... DIAs>
auto DIA<ValueType, Stack>::Merge(
    const Comparator &comparator, const DIAs &... dias) const {

    using VarForeachExpander = int[];

    AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    using CompareResult
              = typename FunctionTraits<Comparator>::result_type;

    using MergeResultNode
              = MergeNode<ValueType, Comparator, DIA<ValueType, Stack>, DIAs ...>;

    // Assert comperator types.
    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<Comparator>::template arg<0>
            >::value,
        "Comparator has the wrong input type in argument 1");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<Comparator>::template arg<1>
            >::value,
        "Comparator has the wrong input type in argument 2");

    // Assert meaningful return type of comperator.
    static_assert(
        std::is_convertible<
            bool,
            CompareResult
            >::value,
        "Comparator must return bool");

    // Create merge node.
    StatsNode* stats_node = AddChildStatsNode("Merge", DIANodeType::DOP);
    (void)VarForeachExpander {
        (dias.AppendChildStatsNode(stats_node), 0) ...
    };

    auto merge_node
        = std::make_shared<MergeResultNode>(comparator, stats_node, *this, dias ...);

    return DIA<ValueType>(merge_node, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_MERGE_HEADER

/******************************************************************************/

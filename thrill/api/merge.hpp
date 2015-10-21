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
#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/core/losertree.hpp>
#include <thrill/data/dyn_block_reader.hpp>
#include <thrill/data/file.hpp>
#include <thrill/common/meta.hpp>

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

/*!
 * MergeStatsBase holds timers for measuring merge performance.
 */
class MergeStatsBase
{
public:
    //! A Timer accumulating all time spent in File operations.
    thrill::common::StatsTimer<stats_enabled> FileOpTimer;
    //! A Timer accumulating all time spent while actually merging.
    thrill::common::StatsTimer<stats_enabled> MergeTimer;
    //! A Timer accumulating all time spent while re-balancing the data.
    thrill::common::StatsTimer<stats_enabled> BalancingTimer;
    //! A Timer accumulating all time spent for selecting the global pivot elements.
    thrill::common::StatsTimer<stats_enabled> PivotSelectionTimer;
    //! A Timer accumulating all time spent in global search steps.
    thrill::common::StatsTimer<stats_enabled> SearchStepTimer;
    //! A Timer accumulating all time spent communicating.
    thrill::common::StatsTimer<stats_enabled> CommTimer;
    //! A Timer accumulating all time spent calling the scatter method of the data subsystem.
    thrill::common::StatsTimer<stats_enabled> ScatterTimer;
    //! The count of all elements processed on this host.
    size_t result_size = 0;
    //! The count of search iterations needed for balancing.
    size_t iterations = 0;
};

/*!
 * MergeStats is an implementation of MergeStatsBase,
 * that supports accumulating the output and printing it
 * to the standard out stream.
 */
class MergeStats : public MergeStatsBase
{
public:
    void PrintToSQLPlotTool(std::string label, size_t p, size_t value) {
        static const bool debug = true;

        LOG << "RESULT " << "operation=" << label << " time=" << value << " workers=" << p << " result_size=" << result_size;
    }

    void Print(Context& ctx) {
        if (stats_enabled) {

            net::FlowControlChannel& flow = ctx.flow_control_channel();
            size_t p = ctx.num_workers();

            size_t merge = flow.AllReduce(MergeTimer.Milliseconds()) / p;
            size_t balance = flow.AllReduce(BalancingTimer.Milliseconds()) / p;
            size_t pivotSelection = flow.AllReduce(PivotSelectionTimer.Milliseconds()) / p;
            size_t searchStep = flow.AllReduce(SearchStepTimer.Milliseconds()) / p;
            size_t fileOp = flow.AllReduce(FileOpTimer.Milliseconds()) / p;
            size_t comm = flow.AllReduce(CommTimer.Milliseconds()) / p;
            size_t scatter = flow.AllReduce(ScatterTimer.Milliseconds()) / p;
            result_size = flow.AllReduce(result_size);

            if (ctx.my_rank() == 0) {
                PrintToSQLPlotTool("merge", p, merge);
                PrintToSQLPlotTool("balance", p, balance);
                PrintToSQLPlotTool("pivotSelection", p, pivotSelection);
                PrintToSQLPlotTool("searchStep", p, searchStep);
                PrintToSQLPlotTool("fileOp", p, fileOp);
                PrintToSQLPlotTool("communication", p, comm);
                PrintToSQLPlotTool("scatter", p, scatter);
                PrintToSQLPlotTool("iterations", p, iterations);
            }
        }
    }
};

} // namespace merge_local

/*!
 * Implementation of Thrill's merge. This merge implementation
 * balances all data before merging, so each worker has
 * the same amount of data when merge finishes.
 *
 * \tparam ValueType The type of the first and second input DIA
 * \tparam ParentDiaRef0 The type of the first input DIA
 * \tparam ParentDiaRef1 The type of the second input DIA
 * \tparam Comparator The comparator defining input and output order.
 */
template <typename ValueType,
          typename Comparator, size_t num_inputs_,
          typename FirstParentDIAType,
          typename ... ParentDIAType>
class MergeNode : public DOpNode<ValueType>
{
    static_assert(num_inputs_ >= 2, "Merge requires at least two inputs.");

    static const bool debug = false;

    //! Instance of merge statistics
    merge_local::MergeStats stats;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    MergeNode(Comparator comparator,
              StatsNode* stats_node,
              const FirstParentDIAType& parent0,
              const ParentDIAType& ... parents)
        : DOpNode<ValueType>(parent0.ctx(),
                            { parent0.node(), parents.node() ... }, stats_node),
          comparator_(comparator)
    {
        // allocate files.
        for (size_t i = 0; i < num_inputs_; ++i)
            files_[i] = context_.GetFilePtr();

        for (size_t i = 0; i < num_inputs_; ++i)
            writers_[i] = files_[i]->GetWriterPtr();

         common::VarCallForeachIndex(
             RegisterParent(this), parent0, parents ...);
    }

    /*!
     * Actually executes the merge operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {
        size_t result_count = 0;
        static const bool debug = false;

        LOG << "Entering Main OP";

        stats.MergeTimer.Start();

        using Reader = data::BufferedBlockReader<ValueType, data::CatBlockSource<data::DynBlockSource> >;

        const size_t k = num_inputs_;

        // get buffered inbound readers from all Channels
        std::vector<Reader> readers;
        for (size_t i = 0; i < k; i++) {
            readers.emplace_back(std::move(streams_[i]->GetCatBlockSource(consume)));
        }
        //Init the looser-tree
        typedef core::LoserTreePointer<
                true,
                ValueType,
                std::function<bool(const ValueType &a, const ValueType &b)>>
            LoserTreeType; //ADVICE(tb), select correct type.

        LoserTreeType lt(k, [this]
                (const ValueType &a, const ValueType &b) -> bool {
                    return comparator_(a, b);
                }
        );

        //Abritary element (copied!)
        ValueType *zero = NULL;

        size_t completed = 0;

        //Find abritary elem.
        for(size_t i = 0; i < k; i++) {
            if(readers[i].HasValue()) {
                 zero = (ValueType*)malloc(sizeof(ValueType));
                 *zero = readers[i].Value();
                 break;
            }
        }

        if(zero != NULL) { //If so, we only have empty channels.
            //Insert abritray element for each empty reader.
            for(size_t i = 0; i < k; i++) {
                if(!readers[i].HasValue()) {
                    lt.insert_start(*zero, i, true);
                    completed++;
                } else {
                    lt.insert_start(readers[i].Value(), i, false);
                }
            }
            lt.init();

            while(completed < k) {

                size_t min = lt.get_min_source();

                auto &reader = readers[min];
                assert(reader.HasValue());

                this->PushItem(reader.Value());

                reader.Next();

                if(reader.HasValue()) {
                    lt.delete_min_insert(reader.Value(), false);
                } else {
                    lt.delete_min_insert(*zero, true);
                    completed++;
                }

                result_count++;
            }

            free(zero);
        }

        stats.MergeTimer.Stop();

        sLOG << "Merge: result_count" << result_count;

        stats.result_size = result_count;
        stats.Print(context_);
    }

    void Dispose() final { }

private:
    //! Merge comparator
    Comparator comparator_;

    size_t my_rank_;
    std::mt19937 ran;

    //! Files for intermediate storage
    std::array<data::FilePtr, num_inputs_> files_;

    //! Writers to intermediate files
    std::array<std::shared_ptr<data::File::Writer>, num_inputs_> writers_;

    //! Array of inbound CatStreams
    std::array<data::CatStreamPtr, num_inputs_> streams_;

    struct Pivot {
        ValueType value;
        size_t    tie_idx;
        size_t    segment_len;
    };

    size_t dataSize;   //Count of items on this worker.
    size_t prefixSize; //Count of items on all prev workers.

    //! Logging helper to print vectors.
    template <typename T>
    std::string VToStr(const std::vector<T>& data) {
        std::stringstream ss;

        for (T elem : data)
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

    //! Register Parent PreOp Hooks, instantiated and called for each Merge parent
    class RegisterParent
    {
    public:
        RegisterParent(MergeNode* merge_node) : merge_node_(merge_node) { }

        template <typename Index, typename Parent>
        void operator () (const Index&, Parent& parent) {

            // construct lambda with only the writer in the closure
            data::File::Writer* writer = merge_node_->writers_[Index::index].get();
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
     * Selects random global pivots for all splitter searches based on all worker's search ranges.
     *
     * \param pivots The output pivots.
     *
     * \param left The left bounds of all search ranges for all files.
     *             The first index identifies the splitter, the second index identifies the file.
     *
     * \param width The width of all search ranges for all files.
     *              The first index identifies the splitter, the second index identifies the file.
     *
     * \param flowControl The flow control channel used for global communication.
     */
    // dim 1: Different splitters, dim 2: different files
    void SelectPivots(
            std::vector<Pivot>& pivots,
            const std::vector<std::vector<size_t> >& left,
            const std::vector<std::vector<size_t> >& width,
            net::FlowControlChannel& flowControl) {

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

            // We can leave pivotElem uninitialzed.
            // If it is not initialized below, then
            // an other worker's pivot will be taken for
            // this range, since our range is zero.
            ValueType pivotElem = pivotElem;
            size_t pivotIdx = left[s][mp];

            if (width[s][mp] > 0) {
                pivotIdx = left[s][mp] + (ran() % width[s][mp]);
                stats.FileOpTimer.Start();
                pivotElem = files_[mp]->template GetItemAt<ValueType>(pivotIdx);
                stats.FileOpTimer.Stop();
            }

            pivots[s] = Pivot {
                pivotElem,
                pivotIdx,
                width[s][mp]
            };
        }

        LOG << "Local Pivots " << VToStr(pivots);

        // Distribute pivots globally.

        // Reduce function that returns the pivot originating from the biggest range.
        // That removes some nasty corner cases, like selecting the same pivot over and
        // over again from a tiny range.
        auto reducePivots = [this](const Pivot a, const Pivot b) {
                                if (a.segment_len > b.segment_len) {
                                    return a;
                                }
                                else {
                                    return b;
                                }
                            };

        stats.CommTimer.Start();

        // Reduce vectors of pivots globally to select the pivots from the
        // largest ranges.
        pivots = flowControl.AllReduce(pivots,
                                       [reducePivots]
                                           (const std::vector<Pivot>& a, const std::vector<Pivot>& b) {
                                           assert(a.size() == b.size());
                                           std::vector<Pivot> res(a.size());
                                           for (size_t i = 0; i < a.size(); i++) {
                                               res[i] = reducePivots(a[i], b[i]);
                                           }
                                           return res;
                                       });
        stats.CommTimer.Stop();
    }

    /*!
     * Calculates the global ranks of the given pivots.
     * Additionally retruns the local ranks so we can use them in the next step.
     *
     * \param pivots The pivots.
     * \param ranks The global ranks of the pivots. This is an output parameter.
     * \params localRanks The local ranks. The first index corresponds to the splitter,
     *                    the second one to the file. This is an output parameter.
     *
     * \flowControl The FlowControlChannel to do global communication.
     */
    void GetGlobalRanks(
            const std::vector<Pivot>& pivots,
            std::vector<size_t>& ranks,
            std::vector<std::vector<size_t> >& localRanks,
            net::FlowControlChannel& flowControl) {

        // Simply get the rank of each pivot in each file.
        // Sum the ranks up locally.
        for (size_t s = 0; s < pivots.size(); s++) {
            size_t rank = 0;
            for (size_t i = 0; i < num_inputs_; i++) {
                stats.FileOpTimer.Start();
                size_t idx = files_[i]->GetIndexOf(pivots[s].value, pivots[s].tie_idx, comparator_);
                stats.FileOpTimer.Stop();

                rank += idx;

                localRanks[s][i] = idx;
            }
            ranks[s] = rank;
        }

        stats.CommTimer.Start();
        // Sum up ranks globally.
        ranks = flowControl.AllReduce(ranks, &AddSizeTVectors);
        stats.CommTimer.Stop();
    }

    /*!
     * Shrinks the search ranges accoring to the global ranks of the pivots.
     *
     * \param ranks The global ranks of all pivots.
     * \param localRanks The local ranks of each pivot in each file.
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
    void SearchStep(const std::vector<size_t>& ranks, std::vector<std::vector<size_t> >& localRanks, const std::vector<size_t>& target_ranks, std::vector<std::vector<size_t> >& left, std::vector<std::vector<size_t> >& width) {

        // This is basically a binary search for each
        // splitter and each file.
        for (size_t s = 0; s < width.size(); s++) {

            for (size_t p = 0; p < width[s].size(); p++) {

                if (width[s][p] == 0)
                    continue;

                size_t idx = localRanks[s][p];
                size_t oldWidth = width[s][p];

                if (ranks[s] <= target_ranks[s]) {
                    width[s][p] -= idx - left[s][p];
                    left[s][p] = idx;
                }
                else if (ranks[s] > target_ranks[s]) {
                    width[s][p] = idx - left[s][p];
                }

                if (debug) {
                    assert(oldWidth >= width[s][p]);
                }
            }
        }
    }

    /*!
     * Receives elements from other workers and re-balance
     * them, so each worker has the same amount after merging.
     */
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i]->Close();
        }
        net::FlowControlChannel& flowControl = context_.flow_control_channel();


        // Setup Environment for merging
        my_rank_ = context_.my_rank();     //Local rank.
        size_t p = context_.num_workers(); //Count of all workers (and count of target partitions)

        LOG << "Splitting to " << p << " workers";

        // Count of all local elements.
        dataSize = 0;

        for (size_t i = 0; i < files_.size(); i++) {
            dataSize += files_[i]->num_items();
        }

        // Count of all global elements.
        stats.CommTimer.Start();
        size_t globalSize = flowControl.AllReduce(dataSize);
        stats.CommTimer.Stop();

        LOG << "Global size: " << globalSize;

        // Calculate and remember the ranks we search for.
        // In our case, we search for ranks that split the data into equal parts.
        std::vector<size_t> targetRanks(p - 1);
        std::vector<size_t> globalRanks(p - 1);

        for (size_t r = 0; r < p - 1; r++) {
            targetRanks[r] = (globalSize / p) * (r + 1);
        }

        // Modify all ranks 0..(globalSize % p), in case globalSize is
        // not divisible by p.
        for (size_t r = 0; r < globalSize % p; r++) {
            targetRanks[r] += 1;
        }

        if (debug) {
            for (size_t r = 0; r < p - 1; r++) {
                LOG << "Search Rank " << r << ": " << targetRanks[r];

                stats.CommTimer.Start();
                size_t res = flowControl.Broadcast(targetRanks[r]);
                stats.CommTimer.Stop();
                assert(res == targetRanks[r]);
            }
        }

        // Search range bounds.
        std::vector<std::vector<size_t> > left(p - 1);
        std::vector<std::vector<size_t> > width(p - 1);

        // Auxillary arrays.
        std::vector<Pivot> pivots(p - 1);
        std::vector<std::vector<size_t> > localRanks(p - 1);

        // Initialize all lefts with 0 and all
        // widths with size of their respective file.
        for (size_t r = 0; r < p - 1; r++) {
            left[r] = std::vector<size_t>(num_inputs_);
            width[r] = std::vector<size_t>(num_inputs_);
            localRanks[r] = std::vector<size_t>(num_inputs_);
            std::fill(left[r].begin(), left[r].end(), 0);

            for (size_t q = 0; q < num_inputs_; q++) {
                width[r][q] = files_[q]->num_items();
            }
        }

        bool finished = false;
        stats.BalancingTimer.Start();

        // TODO: Iterate until all splitters are found.
        // Theoretically, the condition
        //
        // while(globalRanks != targetRanks)
        //
        // could be used here. If the binary-search error
        // mentioned in SearchStep is fixed.
        while (!finished) {

            LOG << "left: " << VToStr(left);
            LOG << "width: " << VToStr(width);

            // Find pivots.
            stats.PivotSelectionTimer.Start();
            SelectPivots(pivots, left, width, flowControl);
            stats.PivotSelectionTimer.Stop();

            LOG << "Final Pivots " << VToStr(pivots);

            // Get global ranks and shrink ranges.
            stats.SearchStepTimer.Start();
            GetGlobalRanks(pivots, globalRanks, localRanks, flowControl);
            SearchStep(globalRanks, localRanks, targetRanks, left, width);

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
                size_t a = globalRanks[i];
                size_t b = targetRanks[i];
                if ((a > b && a - b > num_inputs_ + 1) || (b > a && b - a > num_inputs_ + 1)) {
                    finished = false;
                    break;
                }
            }

            LOG << "srank: " << VToStr(targetRanks);
            LOG << "grank: " << VToStr(globalRanks);

            stats.SearchStepTimer.Stop();
            stats.iterations++;
        }
        stats.BalancingTimer.Stop();

        LOG << "Creating channels";

        // Initialize channels for distributing data.
        for (size_t j = 0; j < num_inputs_; j++) {
            streams_[j] = context_.GetNewCatStream();
        }

        stats.ScatterTimer.Start();

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
        stats.ScatterTimer.Stop();
    }
};

/*!
 * Implementation of corresponding method in dia.hpp
 */
template <typename ValueType, typename Stack>
template <typename Comparator, typename ... DIAs>
auto DIA<ValueType, Stack>::Merge(const Comparator &comparator, const DIAs &... dias) const {

    using VarForeachExpander = int[];
    const size_t num_inputs = sizeof...(DIAs);

    AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    using CompareResult
              = typename FunctionTraits<Comparator>::result_type;

    using MergeResultNode
              = MergeNode<ValueType, Comparator, num_inputs + 1, DIA<ValueType, Stack>, DIAs ...>;

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

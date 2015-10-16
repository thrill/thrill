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

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

namespace merge_local {

static const bool stats_enabled = false;

class MergeStatsBase
{
public:
    thrill::common::StatsTimer<stats_enabled> FileOpTimer;
    thrill::common::StatsTimer<stats_enabled> MergeTimer;
    thrill::common::StatsTimer<stats_enabled> BalancingTimer;
    thrill::common::StatsTimer<stats_enabled> PivotSelectionTimer;
    thrill::common::StatsTimer<stats_enabled> SearchStepTimer;
    thrill::common::StatsTimer<stats_enabled> CommTimer;
    thrill::common::StatsTimer<stats_enabled> ScatterTimer;
    size_t result_size = 0;
    size_t iterations = 0;
};

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

template <typename ValueType,
          typename Comparator, size_t num_inputs_, 
          typename FirstParentDIAType,
          typename ... ParentDIAType>
class MergeNode : public DOpNode<ValueType>
{
    static_assert(num_inputs_ >= 2, "Merge requires at least two inputs.");

    static const bool debug = false;

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
        common::VarCallForeachIndex([this](auto i, auto parent) {
            files_[i] = context_.GetFilePtr();
            writers_[i] = files_[i]->GetWriterPtr();
        
            auto pre_op_fn = [=](const ValueType& input) {
                              (*writers_[i])(input);
                          };
            
            auto lop_chain = parent.stack().push(pre_op_fn).emit();
            // close the function stacks with our pre ops and register it at parent
            // nodes for output
            parent.node()->RegisterChild(lop_chain, this->type());
        }, parent0, parents ...);
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
    std::array<std::shared_ptr<data::File>, num_inputs_> files_;

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

    template <typename T>
    std::string VToStr(const std::vector<T>& data) {
        std::stringstream ss;

        for (T elem : data)
            ss << elem << " ";

        return ss.str();
    }

    std::string VToStr(const std::vector<std::vector<size_t> >& data) {
        std::stringstream ss;

        for (const std::vector<size_t>& elem : data) {
            ss << VToStr(elem) << " ## ";
        }

        return ss.str();
    }

    std::string VToStr(const std::vector<Pivot>& data) {
        std::stringstream ss;

        for (const Pivot& elem : data)
            ss << "(" << elem.value << ", itie: " << elem.tie_idx << ", len: " << elem.segment_len << ") ";

        return ss.str();
    }

    static std::vector<size_t> AddSizeTVectors
        (const std::vector<size_t>& a, const std::vector<size_t>& b) {
        assert(a.size() == b.size());
        std::vector<size_t> res(a.size());
        for (size_t i = 0; i < a.size(); i++) {
            res[i] = a[i] + b[i];
        }
        return res;
    }

    // Globally selects pivots based on the given left/right
    // dim 1: Different splitters, dim 2: different files
    void SelectPivots(std::vector<Pivot>& pivots, const std::vector<std::vector<size_t> >& left, const std::vector<std::vector<size_t> >& width, net::FlowControlChannel& flowControl) {

        // Select the best pivot we have from our ranges.

        for (size_t s = 0; s < width.size(); s++) {
            size_t mp = 0; //biggest range

            for (size_t p = 1; p < width[s].size(); p++) {
                if (width[s][p] > width[s][mp]) {
                    mp = p;
                }
            }

            // TODO(ej) get default val from somewhere.
            ValueType pivotElem = 0;
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

        // Return pivot from biggest range (makes sure that we actually split)
        auto reducePivots = [this](const Pivot a, const Pivot b) {
                                if (a.segment_len > b.segment_len) {
                                    return a;
                                }
                                else {
                                    return b;
                                }
                            };

        stats.CommTimer.Start();
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

    void GetGlobalRanks(const std::vector<Pivot>& pivots, std::vector<size_t>& ranks, std::vector<std::vector<size_t> >& localRanks, net::FlowControlChannel& flowControl) {
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
        ranks = flowControl.AllReduce(ranks, &AddSizeTVectors);
        stats.CommTimer.Stop();
    }

    void SearchStep(const std::vector<Pivot>& pivots, const std::vector<size_t>& ranks, std::vector<std::vector<size_t> >& localRanks, const std::vector<size_t>& target_ranks, std::vector<std::vector<size_t> >& left, std::vector<std::vector<size_t> >& width) {
        for (size_t s = 0; s < pivots.size(); s++) {

            for (size_t p = 0; p < width[s].size(); p++) {

                if (width[s][p] == 0)
                    continue;

                size_t idx = localRanks[s][p];
                size_t oldWidth = width[s][p];

                LOG << "idx: " << idx << " tie_idx: " << pivots[s].tie_idx;

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

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i]->Close();
        }
        net::FlowControlChannel& flowControl = context_.flow_control_channel();

        // Partitioning happens here.
        stats.BalancingTimer.Start();

        // Environment
        my_rank_ = context_.my_rank();     //Local rank.
        size_t p = context_.num_workers(); //Count of all workers (and count of target partitions)

        LOG << "Splitting to " << p << " workers";

        // Partitions in rank over all local collections.
        dataSize = 0;

        for (size_t i = 0; i < files_.size(); i++) {
            dataSize += files_[i]->num_items();
        }

        // Global size off all data.
        stats.CommTimer.Start();
        size_t globalSize = flowControl.AllReduce(dataSize);
        stats.CommTimer.Stop();

        LOG << "Global size: " << globalSize;

        // Rank we search for
        std::vector<size_t> targetRanks(p - 1);
        std::vector<size_t> globalRanks(p - 1);

        for (size_t r = 0; r < p - 1; r++) {
            targetRanks[r] = (globalSize / p) * (r + 1);
        }
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

        // Partition borders. Let there by binary search.
        std::vector<std::vector<size_t> > left(p - 1);
        std::vector<std::vector<size_t> > width(p - 1);

        // Auxillary Arrays
        std::vector<Pivot> pivots(p - 1);
        std::vector<std::vector<size_t> > localRanks(p - 1);

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

        // Partition loop
        // while(globalRanks != targetRanks) {
        while (!finished) {

            LOG << "left: " << VToStr(left);
            LOG << "width: " << VToStr(width);

            stats.PivotSelectionTimer.Start();
            SelectPivots(pivots, left, width, flowControl);
            stats.PivotSelectionTimer.Stop();

            LOG << "Final Pivots " << VToStr(pivots);

            stats.SearchStepTimer.Start();

            GetGlobalRanks(pivots, globalRanks, localRanks, flowControl);
            SearchStep(pivots, globalRanks, localRanks, targetRanks, left, width);

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

        LOG << "Creating channels";

        // Init channels and offsets.
        for (size_t j = 0; j < num_inputs_; j++) {
            streams_[j] = context_.GetNewCatStream();
        }

        stats.BalancingTimer.Stop();
        stats.ScatterTimer.Start();

        LOG << "Scattering.";

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

    static_assert(
        std::is_convertible<
            bool,
            CompareResult
            >::value,
        "Comparator must return bool");

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

//! \}
#endif // !THRILL_API_MERGE_HEADER

/******************************************************************************/

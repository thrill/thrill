/*******************************************************************************
 * thrill/api/merge.hpp
 *
 * DIANode for a merge operation. Performs the actual merge operation
 *
 * Part of Project Thrill.
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
#include <thrill/data/file.hpp>
#include <thrill/data/dyn_block_reader.hpp>
#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <vector>
#include <random>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{


namespace merge_local {
    
    static const bool stats_enabled = false;

    class MergeStatsBase {
        public:
        thrill::common::StatsTimer<stats_enabled> IndexOfTimer;
        thrill::common::StatsTimer<stats_enabled> GetAtIndexTimer;
        thrill::common::StatsTimer<stats_enabled> MergeTimer;
        thrill::common::StatsTimer<stats_enabled> BalancingTimer;
        thrill::common::StatsTimer<stats_enabled> PivotSelectionTimer;
        thrill::common::StatsTimer<stats_enabled> PivotLocationTimer;
        thrill::common::StatsTimer<stats_enabled> CommTimer;
        size_t result_size = 0;
        size_t iterations = 0;
    };

    
    class MergeStats : public MergeStatsBase {
    public:

        void PrintToSQLPlotTool(std::string label, size_t p, size_t value) {
            static const bool debug = true;

            LOG << "RESULT " << label << "=" << value << " workers=" << p << " result_size=" << result_size; 
        }

        void Print(Context &ctx) { 
            if(stats_enabled) {

                net::FlowControlChannel &flow = ctx.flow_control_channel();
                size_t p = ctx.num_workers();

                size_t merge = flow.AllReduce(MergeTimer.Microseconds()) / p;
                size_t balance = flow.AllReduce(BalancingTimer.Microseconds()) / p;
                size_t pivotSelection = flow.AllReduce(PivotSelectionTimer.Microseconds()) / p;
                size_t pivotLocation = flow.AllReduce(PivotLocationTimer.Microseconds()) / p;
                size_t indexOf = flow.AllReduce(IndexOfTimer.Microseconds()) / p;
                size_t getAtIndex = flow.AllReduce(GetAtIndexTimer.Microseconds()) / p;
                size_t comm = flow.AllReduce(CommTimer.Microseconds()) / p;
                result_size = flow.AllReduce(result_size);

                if(ctx.my_rank() == 0) {
                    PrintToSQLPlotTool("merge", p, merge); 
                    PrintToSQLPlotTool("balance", p, balance); 
                    PrintToSQLPlotTool("pivotSelection", p, pivotSelection); 
                    PrintToSQLPlotTool("pivotLocation", p, pivotLocation); 
                    PrintToSQLPlotTool("getIndexOf", p, indexOf); 
                    PrintToSQLPlotTool("getAtIndex", p, getAtIndex); 
                    PrintToSQLPlotTool("communication", p, comm); 
                    PrintToSQLPlotTool("iterations", p, iterations); 
                }
            }
        }
    };
};

template <typename ValueType,
          typename ParentDIARef0, typename ParentDIARef1,
          typename Comparator>
class TwoMergeNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    merge_local::MergeStats stats;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    TwoMergeNode(const ParentDIARef0& parent0,
                 const ParentDIARef1& parent1,
                 Comparator comparator,
                 StatsNode* stats_node)
        : DOpNode<ValueType>(parent0.ctx(),
                             { parent0.node(), parent1.node() }, stats_node),
          comparator_(comparator)
    {
        // Hook PreOp(s)
        auto pre_op0_fn = [=](const ValueType& input) {
                              writers_[0](input);
                          };
        auto pre_op1_fn = [=](const ValueType& input) {
                              writers_[1](input);
                          };

        // close the function stacks with our pre ops and register it at parent
        // nodes for output
        auto lop_chain0 = parent0.stack().push(pre_op0_fn).emit();
        auto lop_chain1 = parent1.stack().push(pre_op1_fn).emit();

        parent0.node()->RegisterChild(lop_chain0, this->type());
        parent1.node()->RegisterChild(lop_chain1, this->type());
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

        typedef data::BufferedBlockReader<ValueType, data::CatBlockSource<data::DynBlockSource>> Reader; 

        // get buffered inbound readers from all Channels
        std::vector<Reader> readers;
        for(size_t i = 0; i < streams_.size(); i++) {
            readers.emplace_back(std::move(streams_[i]->GetCatBlockSource(consume)));
        }

        while(true) {

            auto &a = readers[0];
            auto &b = readers[1];

            if(a.HasValue() && b.HasValue()) {
                if(comparator_(b.Value(), a.Value())) {
                    this->PushItem(b.Value());
                    b.Next();
                } else {
                    this->PushItem(a.Value());
                    a.Next();
                }
            } else if(b.HasValue()) {
                this->PushItem(b.Value());
                b.Next();
            } else if(a.HasValue()) {
                this->PushItem(a.Value());
                a.Next();
            } else {
                break;
            }

            result_count++;
        }
        
        stats.MergeTimer.Stop();

        sLOG << "Merge: result_count" << result_count;

        stats.result_size = result_count;
        stats.Print(context_);
    }

    void Dispose() final { }

    /*!
     * Creates empty stack.
     */
    auto ProduceStack() {
        // Hook PostOp
        return FunctionStack<ValueType>();
    }

private:
    //! Merge comparator
    Comparator comparator_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 2;

    size_t my_rank_;
    std::mt19937 ran;
 

    //! Files for intermediate storage
    std::array<data::File, num_inputs_> files_ {
        { context_.GetFile(), context_.GetFile() }
    };

    //! Writers to intermediate files
    std::array<data::File::Writer, num_inputs_> writers_  {
        { files_[0].GetWriter(), files_[1].GetWriter() }
    };

    //! Array of inbound CatStreams
    std::array<data::CatStreamPtr, num_inputs_> streams_;

    struct Pivot {
        ValueType value;
        size_t tie_idx; 
        size_t segment_len;
    };

    size_t dataSize; //Count of items on this worker.
    size_t prefixSize; //Count of items on all prev workers.

    template <typename T>
    std::string VToStr(const std::vector<T> &data) {
        std::stringstream ss;
        
        for(T elem : data)
            ss << elem << " ";

        return ss.str();
    }
    
    std::string VToStr(const std::vector<std::vector<size_t>> &data) {
        std::stringstream ss;
        
        for(const std::vector<size_t> &elem : data) {
            ss << VToStr(elem) << " ## ";
        }

        return ss.str();
    }
    
    std::string VToStr(const std::vector<Pivot> &data) {
        std::stringstream ss;
        
        for(const Pivot &elem : data)
            ss << "(" << elem.value << ", itie: " << elem.tie_idx << ", len: " << elem.segment_len << ") ";

        return ss.str();
    }
    
    static std::vector<size_t> AddSizeTVectors
        (const std::vector<size_t> &a, const std::vector<size_t> &b) {
            assert(a.size() == b.size());
            std::vector<size_t> res(a.size());
            for(size_t i = 0; i < a.size(); i++) {
               res[i] = a[i] + b[i]; 
            }
            return res;
        };


    //Globally selects pivots based on the given left/right
    //dim 1: Different splitters, dim 2: different files
    void SelectPivots(std::vector<Pivot> &pivots, const std::vector<std::vector<size_t>> &left, const std::vector<std::vector<size_t>> &width, net::FlowControlChannel &flowControl) {

        //Select the best pivot we have from our ranges. 

        for(size_t s = 0; s < width.size(); s++) {
            size_t mp = 0; //biggest range

            for(size_t p = 1; p < width[s].size(); p++) {
                if(width[s][p] > width[s][mp]) {
                    mp = p;
                }
            }
            
            //TODO(ej) get default val from somewhere. 
            ValueType pivotElem = 0;
            size_t pivotIdx = left[s][mp]; 

            if(width[s][mp] > 0) {
                pivotIdx = left[s][mp] + (ran() % width[s][mp]);
                pivotElem = files_[mp].template GetItemAt<ValueType>(pivotIdx);
            }

            pivots[s] = Pivot{
                pivotElem, 
                pivotIdx, 
                width[s][mp]
            };
        } 

        LOG << "Local Pivots " << VToStr(pivots);
        //Distribute pivots globally. 
        
        //Return pivot from biggest range (makes sure that we actually split)
        auto reducePivots = [this] (const Pivot a, const Pivot b) { 
                if(a.segment_len > b.segment_len) {
                    return a; 
                } else {
                    return b;
                }}; 

        //stats.CommTimer.Start();
        pivots = flowControl.AllReduce(pivots, 
            [reducePivots]
            (const std::vector<Pivot> &a, const std::vector<Pivot> &b) {
                assert(a.size() == b.size());
                std::vector<Pivot> res(a.size());
                for(size_t i = 0; i < a.size(); i++) {
                   res[i] = reducePivots(a[i], b[i]); 
                }
                return res;
            }); 
        //stats.CommTimer.Stop();

    }

    void GetGlobalRanks(const std::vector<Pivot> &pivots, std::vector<size_t> &ranks, std::vector<std::vector<size_t>> &localRanks, net::FlowControlChannel &flowControl) {
        for(size_t s = 0; s < pivots.size(); s++) {
            size_t rank = 0;
            for(size_t i = 0; i < num_inputs_; i++) {
                size_t idx = files_[i].GetIndexOf(pivots[s].value, pivots[s].tie_idx, comparator_);
                rank += idx;

                localRanks[s][i] = idx;
            }
            ranks[s] = rank;
        }
           
        ranks = flowControl.AllReduce(ranks, &AddSizeTVectors);
    }
    
    void SearchStep(const std::vector<Pivot> &pivots, const std::vector<size_t> &ranks, std::vector<std::vector<size_t>> &localRanks, const std::vector<size_t> &target_ranks, std::vector<std::vector<size_t>> &left, std::vector<std::vector<size_t>> &width) {
        for(size_t s = 0; s < pivots.size(); s++) {

            for(size_t p = 0; p < width[s].size(); p++) {

                if(width[s][p] == 0)
                    continue;

                size_t idx = localRanks[s][p];
                size_t oldWidth = width[s][p];

                LOG << "idx: " << idx << " tie_idx: " << pivots[s].tie_idx;

                if(ranks[s] <= target_ranks[s]) {
                    width[s][p] -= idx - left[s][p];
                    left[s][p] = idx;
                } else if(ranks[s] > target_ranks[s]) {
                    width[s][p] = idx - left[s][p];
                } 

                if(debug) {
                    assert(oldWidth >= width[s][p]);
                }
            }
        }
    }

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i].Close();
        }
        net::FlowControlChannel& flowControl = context_.flow_control_channel();

        //Partitioning happens here.
        stats.BalancingTimer.Start();
        
        //Environment
        my_rank_ = context_.my_rank(); //Local rank. 
        size_t p = context_.num_workers(); //Count of all workers (and count of target partitions)
        size_t zero_t = 0;

        LOG << "Splitting to " << p << " workers";

        //Partitions in rank over all local collections.
        dataSize = 0;

        for(size_t i = 0; i < files_.size(); i++) {
            dataSize += files_[i].num_items();
        };

        //Global size off all data.
        stats.CommTimer.Start();
        size_t globalSize = flowControl.AllReduce(dataSize);
        stats.CommTimer.Stop();

        LOG << "Global size: " << globalSize;

        //Rank we search for
        std::vector<size_t> targetRanks(p - 1);
        std::vector<size_t> globalRanks(p - 1);

        for(size_t r = 0; r < p - 1; r++) {
            targetRanks[r] = (globalSize / p) * (r + 1);
        }
        for(size_t r = 0; r < globalSize % p; r++) {
            targetRanks[r] += 1;
        }

        if(debug) {
            for(size_t r = 0; r < p - 1; r++) { 
                LOG << "Search Rank " << r << ": " << targetRanks[r];

                stats.CommTimer.Start();
                size_t res = flowControl.Broadcast(targetRanks[r]);
                stats.CommTimer.Stop();
                assert(res == targetRanks[r]);
            }
        }

        //Partition borders. Let there by binary search. 
        std::vector<std::vector<size_t>> left(p - 1);
        std::vector<std::vector<size_t>> width(p - 1);
        
        //Auxillary Arrays
        std::vector<Pivot> pivots(p - 1);
        std::vector<std::vector<size_t>> localRanks(p - 1);

        for(size_t r = 0; r < p - 1; r++) {
            left[r] = std::vector<size_t>(num_inputs_);
            width[r] = std::vector<size_t>(num_inputs_);
            localRanks[r] = std::vector<size_t>(num_inputs_);
            std::fill(left[r].begin(), left[r].end(), 0);

            for(size_t q = 0; q < num_inputs_; q++) {
                width[r][q] = files_[q].num_items();
            }
        }

        bool finished = false;

        //Partition loop 
        //while(globalRanks != targetRanks) {
        while(!finished) {
            stats.PivotSelectionTimer.Start();

            LOG << "left: " << VToStr(left);
            LOG << "width: " << VToStr(width);

            SelectPivots(pivots, left, width, flowControl);

            LOG << "Final Pivots " << VToStr(pivots);
           
            stats.PivotSelectionTimer.Stop();
            stats.PivotLocationTimer.Start();

            GetGlobalRanks(pivots, globalRanks, localRanks, flowControl);
            SearchStep(pivots, globalRanks, localRanks, targetRanks, left, width);

            finished = true;

            for(size_t i = 0; i < p - 1; i++) {
                size_t a = globalRanks[i];
                size_t b = targetRanks[i];
                if((a > b && a - b > 1) || (b > a && b - a > 1)) {
                    finished = false;
                    break;
                }
            }

            LOG << "srank: " << VToStr(targetRanks);
            LOG << "grank: " << VToStr(globalRanks);



            stats.PivotLocationTimer.Stop();
            stats.iterations++;
        }

        LOG << "Creating channels";
        
        //Init channels and offsets.
        for(size_t j = 0; j < num_inputs_; j++) {
            streams_[j] = context_.GetNewCatStream();
        }

        stats.BalancingTimer.Stop();

        LOG << "Scattering.";
        
        for(size_t j = 0; j < num_inputs_; j++) {

            std::vector<size_t> offsets(p);

            for(size_t r = 0; r < p - 1; r++) {
                offsets[r] = left[r][j];
            }

            offsets[p - 1] = files_[j].num_items();

            for(size_t i = 0; i < p; i++) {
                LOG << "Offset " << i << " for file " << j << ": " << VToStr(offsets);
            }

            streams_[j]->template Scatter<ValueType>(files_[j], offsets);
        }
    }
};

template <typename ValueType, typename Stack>
template <typename SecondDIA, typename Comparator>
auto DIA<ValueType, Stack>::Merge(
    SecondDIA second_dia, const Comparator &comparator) const {
    
    assert(IsValid());
    assert(second_dia.IsValid());

    using CompareResult
              = typename FunctionTraits<Comparator>::result_type;

    using MergeResultNode
              = TwoMergeNode<ValueType, DIA, SecondDIA, Comparator>;
    
    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            ValueType
            >::value,
        "DIA 1 and DIA 0 have different types");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<Comparator>::template arg<0>
            >::value,
        "Comparator has the wrong input type in DIA 0");

    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            typename FunctionTraits<Comparator>::template arg<1>
            >::value,
        "Comparator has the wrong input type in DIA 1");

    static_assert(
        std::is_convertible<
            bool,
            CompareResult
            >::value,
        "Comparator must return bool");

    StatsNode* stats_node = AddChildStatsNode("Merge", DIANodeType::DOP);
    second_dia.AppendChildStatsNode(stats_node);
    auto merge_node
        = std::make_shared<MergeResultNode>(*this,
                                            second_dia,
                                            comparator,
                                            stats_node);

    auto merge_stack = merge_node->ProduceStack();

    return DIA<ValueType, decltype(merge_stack)>(
        merge_node,
        merge_stack,
        { stats_node });
}


//! \}

} // namespace api
} // namespace thrill

//! \}
#endif // !THRILL_API_MERGE_HEADER

/******************************************************************************/

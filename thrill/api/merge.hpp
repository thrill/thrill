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
    
    static const bool stats_enabled = true;

    class MergeStatsBase {
        public:
        thrill::common::StatsTimer<stats_enabled> IndexOfTimer;
        thrill::common::StatsTimer<stats_enabled> GetAtIndexTimer;
        thrill::common::StatsTimer<stats_enabled> MergeTimer;
        thrill::common::StatsTimer<stats_enabled> BalancingTimer;
        thrill::common::StatsTimer<stats_enabled> PivotSelectionTimer;
        thrill::common::StatsTimer<stats_enabled> PivotLocationTimer;
        thrill::common::StatsTimer<stats_enabled> OffsetCalculationTimer;
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
                size_t offsetCalculation = flow.AllReduce(OffsetCalculationTimer.Microseconds()) / p;
                size_t indexOf = flow.AllReduce(IndexOfTimer.Microseconds()) / p;
                size_t getAtIndex = flow.AllReduce(GetAtIndexTimer.Microseconds()) / p;
                size_t comm = flow.AllReduce(CommTimer.Microseconds()) / p;
                result_size = flow.AllReduce(result_size);

                if(ctx.my_rank() == 0) {
                    PrintToSQLPlotTool("merge", p, merge); 
                    PrintToSQLPlotTool("balance", p, balance); 
                    PrintToSQLPlotTool("pivotSelection", p, pivotSelection); 
                    PrintToSQLPlotTool("pivotLocation", p, pivotLocation); 
                    PrintToSQLPlotTool("offsetCalculation", p, offsetCalculation); 
                    PrintToSQLPlotTool("getIndexOf", p, indexOf); 
                    PrintToSQLPlotTool("getAtIndex", p, getAtIndex); 
                    PrintToSQLPlotTool("communication", p, comm); 
                    PrintToSQLPlotTool("iterations", p, iterations); 
                }
            }
        }
    };

    template <typename ItemType, typename Comperator, size_t num_items>
    static size_t IndexOf(ItemType element, size_t tie, const std::array<data::File, num_items> &files, Comperator comperator, common::StatsTimer<stats_enabled> *timer = NULL) {
        static const bool debug = false;

        START_TIMER(timer);

        //Get the index of a given element, or the first
        //Greater one - 1 
        size_t lidx = 0;
        size_t hidx = 0;

        LOG << "Looking for element " << element << " with tie " << tie;

        for(size_t i = 0; i < num_items; i++) {
            auto reader = files[i].GetKeepReader();
            lidx += files[i].GetIndexOf(element, 0, comperator);
            hidx += files[i].GetIndexOf(element, std::numeric_limits<size_t>::max(), comperator);
        }

        LOG << "hidx: " << hidx << ", lidx: " << lidx << ", tie: " << tie; 

        STOP_TIMER(timer);

        if(tie > hidx) {
            return hidx;
        } else if(tie < lidx) {
            return lidx;
        } else {
            return tie;
        }
    }
};

template <typename ValueType,
          typename ParentDIARef0, typename ParentDIARef1,
          typename Comperator>
class TwoMergeNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    merge_local::MergeStats stats;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    TwoMergeNode(const ParentDIARef0& parent0,
                 const ParentDIARef1& parent1,
                 Comperator comperator,
                 StatsNode* stats_node)
        : DOpNode<ValueType>(parent0.ctx(),
                             { parent0.node(), parent1.node() }, stats_node),
          comperator_(comperator)
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
                if(comperator_(b.Value(), a.Value())) {
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
    //! Merge comperator
    Comperator comperator_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 2;

    //! TODO
    size_t result_size_;

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
        size_t tie_host;
        size_t tie_idx; 
        size_t segment_len;
    };

    Pivot CreatePivot(ValueType v, size_t i) {
        Pivot p;
        p.first = v;
        p.second = i;
        return p;
    }
    

    size_t dataSize; //Count of items on this worker.
    size_t prefixSize; //Count of items on all prev workers.

    template <typename T>
    std::string VToStr(std::vector<T> data) {
        std::stringstream ss;
        
        for(T elem : data)
            ss << elem << " ";

        return ss.str();
    }
    
    std::string VToStr(std::vector<Pivot> data) {
        std::stringstream ss;
        
        for(Pivot elem : data)
            ss << "(" << elem.valid << ", " << elem.first << ", " << elem.second << ") ";

        return ss.str();
    }

    //Globally selects pivots based on the given left/right
    //dim 1: Different splitters, dim 2: different files
    void SelectPivots(std::vector<Pivot> &pivots, const std::vector<std::vector<size_t>> &left, const std::vector<std::vector<size_t>> &width, FlowControlChannel &flowControl) {

        //Select the best pivot we have from our ranges. 

        for(size_t s = 0; s < width.size()) {
            size_t mp = 0; //biggest range

            for(size_t p = 1; p < width[s].size(); p++) {
                if(width[s][p] > width[s][mp]) {
                    p = mp;
                }
            }
  
            size_t pivotIdx = (left[s][mp] + width[s][mp]) / 2;

            //Todo: Not sure about tying here. 
            pivots[s] = Pivot{files[mp].GetAt(pivotIdx), my_rank_, pivotIdx, width[s][mp]};
        } 

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

    void GetGlobalRanks(const std::vector<Pivot> &pivots, std::vector<size_t> &ranks) {
        //TODO(ej): We can actually optimize here by saving the rank of the pivots in files for later.
        for(size_t s = 0; s < pivots.size(); s++) {
           size_t tie = pivots[s].tie_idx;
           if(pivots[s].tie_host < my_rank)
               tie = 0;
           else if(pivots[s].tie_host > my_rank) 
               tie = std::max<size_t>;
           ranks[s] = merge_local::GetIndexOf(pivots[s].pivot, tie, files_, comperator_); 
        }
           
        ranks = flowControl.AllReduce(ranks, addSizeTVectors);
    }
    
    void SearchStep(const std::vector<Pivot> &pivots, const std::vector<size_t> &ranks, const std::vector<size_t> &target_ranks, std::vector<std::vector<size_t>> &left, std::vector<std::vector<size_t>> &width) {
        for(size_t s = 0; s < pivots.size(); s++) {

            for(size_t p = 0; p < width[s][p].size(); p++) {
                size_t idx = files_[p].IndexOf(pivots[s]); //TODO 

                if(ranks[s] < target_ranks[s]) {
                    width[s][p] = idx - left[s][p];
                    left[s][p] = idx;
                } else {
                    width[s][p] = left[s][p] - idx;
                }
            }
        }
    }

    //TODO: Rewrite this method with the helpers above. 

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i].Close();
        }
        net::FlowControlChannel& flowControl = context_.flow_control_channel();

        //Partitioning happens here.
        stats.BalancingTimer.Start();
        
        //Environment
        size_t me = context_.my_rank(); //Local rank. 
        size_t p = context_.num_workers(); //Count of all workers (and count of target partitions)
        size_t zero_t = 0;

        LOG << "Splitting to " << p << " workers";

        //Partitions in rank over all local collections.
        std::vector<size_t> partitions(p - 1);
        std::mt19937 rand(0); //Give const seed. May choose this one over the net.

        //Overall count of local items.
        dataSize = 0;

        for(size_t i = 0; i < files_.size(); i++) {
            dataSize += files_[i].num_items();
        };

        //Global size off aaaalll data.
        stats.CommTimer.Start();
        size_t globalSize = flowControl.AllReduce(dataSize);
        stats.CommTimer.Stop();

        LOG << "Global size: " << globalSize;

        //Rank we search for
        std::vector<size_t> srank(p - 1);

        for(size_t r = 0; r < p - 1; r++) {
            srank[r] = (globalSize / p) * (r + 1);
        }
        for(size_t r = 0; r < globalSize % p; r++) {
            srank[r] += 1;
        }

        if(debug) {
            for(size_t r = 0; r < p - 1; r++) { 
                LOG << "Search Rank " << r << ": " << srank[r];

                stats.CommTimer.Start();
                size_t res = flowControl.Broadcast(srank[r]);
                stats.CommTimer.Stop();
                assert(res == srank[r]);
            }
        }

        //Partition borders. Let there by binary search. 
        std::vector<size_t> left(p - 1);
        std::vector<size_t> width(p - 1);
        std::vector<size_t> zero_v(p - 1);

        std::fill(left.begin(), left.end(), 0);
        std::fill(zero_v.begin(), zero_v.end(), 0);
        std::fill(width.begin(), width.end(), dataSize);
        
        stats.CommTimer.Start();
        prefixSize = flowControl.PrefixSum(dataSize, zero_t, std::plus<size_t>(), false); 
        stats.CommTimer.Stop();

        LOG << "Data count left of me: " << prefixSize;

        //Auxillary Arrays
        std::vector<size_t> widthscan(p - 1);
        std::vector<size_t> widthsum(p - 1);
        std::vector<size_t> pivotrank(p - 1);
        std::vector<size_t> split(p - 1);
        std::vector<Pivot> pivots(p - 1);
        std::vector<size_t> splitsum(p - 1);

        Pivot zero = CreatePivot(merge_local::GetAt<ValueType, Comperator, num_inputs_>(0, files_, comperator_, &stats.GetAtIndexTimer), 0);
        zero.valid = false;

        auto addSizeTVectors = [] 
            (const std::vector<size_t> &a, const std::vector<size_t> &b) {
                assert(a.size() == b.size());
                std::vector<size_t> res(a.size());
                for(size_t i = 0; i < a.size(); i++) {
                   res[i] = a[i] + b[i]; 
                }
                return res;
            };

        //Partition loop 
        
        while(1) {
            stats.PivotSelectionTimer.Start();
            stats.CommTimer.Start();
            widthscan = flowControl.PrefixSum(width, zero_v, addSizeTVectors, false);
            widthsum = flowControl.AllReduce(width, addSizeTVectors);
            stats.CommTimer.Stop();
             
            size_t done = 0;

            LOG << "left: " << VToStr(left);
            LOG << "width: " << VToStr(width);
            LOG << "srank: " << VToStr(srank);
            LOG << "widthsum: " << VToStr(widthsum);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] <= 1) { //Yes, this is cheating, but I have no idea why it gets unstable for very low widthsum[r]. 9 is randomly choosen by fair dice roll.  
                    partitions[r] = left[r];
                    done++;
                }
            }

            if(done == p - 1) {
                LOG << "Finished";
                stats.PivotSelectionTimer.Stop();
                break;
            } else {
                LOG << "Continue";
            }
            
            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] > 1) {
                    pivotrank[r] = rand() % widthsum[r];
                } else {
                    pivotrank[r] = 0;
                }
            }
            
            LOG << "Pivotranks: " << VToStr(pivotrank);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] > 1 &&
                   widthscan[r] <= pivotrank[r] &&
                   pivotrank[r] < widthscan[r] + width[r]) {
                   
                   size_t localRank = left[r] + pivotrank[r] - widthscan[r]; 
                   LOG << "Selecting local rank " << localRank << " zero pivot " << r;
                   ValueType pivotElement = merge_local::GetAt<ValueType, Comperator, num_inputs_>(localRank, files_, comperator_, &stats.GetAtIndexTimer);

                   if(debug) {
                        assert(merge_local::IndexOf(pivotElement, localRank, files_, comperator_, &stats.IndexOfTimer) == localRank);
                   }
                    
                   Pivot pivot = CreatePivot(pivotElement, localRank + prefixSize);

                   pivots[r] = pivot;
                } else {
                   LOG << "Selecting zero pivot " << r;
                   pivots[r] = zero;
                }
            }

           LOG << "Pivots: " << VToStr(pivots);
           
           auto reducePivots = [this] (const Pivot a, const Pivot b) { 
                    if(!a.valid) {
                        return b; 
                    } else if(!b.valid) {
                        return a;
                    } else if(comperator_(b.first, a.first)) {
                        return a; 
                    } else if(comperator_(a.first, b.first)) {
                        return b;
                    } else if(a.second > b.second) {
                        return a;
                    } else {
                        return b;
                    }}; //Return maximal pivot (Is this  OK?)

            stats.CommTimer.Start();
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
            stats.CommTimer.Stop();

            LOG << "Final Pivots: " << VToStr(pivots);

            stats.PivotSelectionTimer.Stop();
            stats.PivotLocationTimer.Start();

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] <= 1) {
                    split[r] = 0;
                } else {
                    split[r] = merge_local::IndexOf(pivots[r].first, pivots[r].second - prefixSize, files_, comperator_, &stats.IndexOfTimer) - left[r];
                }
            }

            stats.CommTimer.Start();
            splitsum = flowControl.AllReduce(split, addSizeTVectors);
            stats.CommTimer.Stop();

            LOG << "Splitters: " << VToStr(split);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] < 1) continue;
                
                if(debug) {
                    assert(width[r] - split[r] <= width[r]);
                }

                if(splitsum[r] < srank[r])  {
                    left[r] += split[r];
                    width[r] -= split[r];
                    srank[r] -= splitsum[r];
                } else {
                    if(debug) {
                        assert(width[r] >= split[r]);
                    }
                    width[r] = split[r];
                }
            } 
            stats.PivotLocationTimer.Stop();
            stats.iterations++;
        }

        //Cool, parts contains the global splitters now. But we need
        //To convert them to file-local parts. 

        std::vector<std::vector<size_t>> offsets(num_inputs_);

        LOG << "Creating channels";
        
        //Init channels and offsets.
        for(size_t j = 0; j < num_inputs_; j++) {
            streams_[j] = context_.GetNewCatStream();
            offsets[j] = std::vector<size_t>(p);
            offsets[j][p - 1] = files_[j].num_items();
        }

        LOG << "Calculating offsets.";

        stats.OffsetCalculationTimer.Start();

        //TODO(ej) - this is superfluiouuiuius. We can probably
        //Extract the ranks per file from the above loop. 
        for(size_t i = 0; i < p - 1; i++) {
            size_t globalSplitter = partitions[i];
            static const bool debug = false;
            LOG << "Global Splitter " << i << ": " << globalSplitter;
            if(globalSplitter < dataSize) {
                //We have to find file-local splitters.
                ValueType pivot = merge_local::GetAt<ValueType, Comperator, num_inputs_>(partitions[i], files_, comperator_, &stats.GetAtIndexTimer);

                size_t prefixSum = 0;

                for(size_t j = 0; j < num_inputs_; j++) {
                    offsets[j][i] = files_[j].GetIndexOf(pivot, partitions[i] - prefixSum, comperator_);
                    prefixSum += files_[j].num_items();
                }
            } else {
                //Ok, splitter beyond all sizes. 
                for(size_t j = 0; j < num_inputs_; j++) {
                    offsets[j][i] = files_[j].num_items();
                }
            }
        }

        stats.OffsetCalculationTimer.Stop();
        stats.BalancingTimer.Stop();

        LOG << "Scattering.";
        
        for(size_t j = 0; j < num_inputs_; j++) {
            static const bool debug = false;
            for(size_t i = 0; i < p; i++) {
                LOG << "Offset " << i << " for file " << j << ": " << offsets[j][i];
            }

            streams_[j]->template Scatter<ValueType>(files_[j], offsets[j]);
        }
    }
};

template <typename ValueType, typename Stack>
template <typename SecondDIA, typename Comperator>
auto DIA<ValueType, Stack>::Merge(
    SecondDIA second_dia, const Comperator &comperator) const {
    
    assert(IsValid());
    assert(second_dia.IsValid());

    using CompareResult
              = typename FunctionTraits<Comperator>::result_type;

    using MergeResultNode
              = TwoMergeNode<ValueType, DIA, SecondDIA, Comperator>;
    
    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            ValueType
            >::value,
        "DIA 1 and DIA 0 have different types");

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<Comperator>::template arg<0>
            >::value,
        "Comperator has the wrong input type in DIA 0");

    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            typename FunctionTraits<Comperator>::template arg<1>
            >::value,
        "Comperator has the wrong input type in DIA 1");

    static_assert(
        std::is_convertible<
            bool,
            CompareResult
            >::value,
        "Comperator must return bool");

    StatsNode* stats_node = AddChildStatsNode("Merge", DIANodeType::DOP);
    second_dia.AppendChildStatsNode(stats_node);
    auto merge_node
        = std::make_shared<MergeResultNode>(*this,
                                            second_dia,
                                            comperator,
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

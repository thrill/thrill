/*******************************************************************************
 * thrill/api/merge.hpp
 *
 * DIANode for a merge operation. Performs the actual merge operation
 *
 * Part of Project thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MERGE_HEADER
#define THRILL_API_MERGE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/buffered_block_reader.hpp>
#include <thrill/net/collective_communication.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <vector>
#include <random>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

//! Todo(ej) Merge has to identical types. 
//! todo(ej) todo(tb) Can probably subclass a lot here.

template <typename ValueType,
          typename ParentDIARef0, typename ParentDIARef1,
          typename Comperator>
class TwoMergeNode : public DOpNode<ValueType>
{
    static const bool debug = true;

    using Super = DOpNode<ValueType>;
    using Super::context_;

public:
    TwoMergeNode(const ParentDIARef0& parent0,
               const ParentDIARef1& parent1,
               Comperator comperator,
               StatsNode* stats_node)
        : DOpNode<ValueType>(parent0.ctx(), { parent0.node(), parent1.node() }, "MergeNode", stats_node),
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

    ~TwoMergeNode() { }

    /*!
     * Actually executes the merge operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() final {
        MainOp();
    }

    void PushData() override {

        size_t result_count = 0;

        typedef data::BufferedBlockReader<ValueType, data::ConcatBlockSource<data::CachingBlockQueueSource>> Reader; 

        // get buffered inbound readers from all Channels
        std::vector<Reader> readers;
        for(size_t i = 0; i < channels_.size(); i++) {
            readers.emplace_back(std::move(channels_[i]->OpenCachingReaderSource()));
        }

        while(true) {

            int biggest = -1;

            for (size_t i = 0; i < readers.size(); i++) {
                if(readers[i].HasValue()) {
                    if(biggest == -1 || comperator_(readers[i].Value(), readers[biggest].Value())) {
                       biggest = (int)i; 
                    }
                }
            }

            if(biggest == -1) {
                //We finished.
                break;
            }

            auto &reader = readers[biggest];

            for (auto func : DIANode<ValueType>::callbacks_) {
                func(reader.Value());
            }

            reader.Next();

            result_count++;
        }

        sLOG << "Merge: result_count" << result_count;
    }

    void Dispose() final { }

    /*!
     * Creates empty stack.
     */
    auto ProduceStack() {
        // Hook PostOp
        return FunctionStack<ValueType>();
    }

    /*!
     * Returns "[MergeNode]" as a string.
     * \return "[MergeNode]"
     */
    std::string ToString() final {
        return "[MergeNode]";
    }

private:
    //! Merge comperator
    Comperator comperator_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 2;

    //! Files for intermediate storage
    std::array<data::File, num_inputs_> files_ {
        { context_.GetFile(), context_.GetFile() }
    };

    //! Writers to intermediate files
    std::array<data::File::Writer, num_inputs_> writers_  {
        { files_[0].GetWriter(), files_[1].GetWriter() }
    };

    //! Array of inbound Channels
    std::array<data::ChannelPtr, num_inputs_> channels_;

    struct Pivot {
        ValueType first;
        size_t second;

        bool operator <(const Pivot& y) const {
            return std::tie(first, second) < std::tie(y.first, y.second);
        }
    };

    Pivot CreatePivot(ValueType v, size_t i) {
        Pivot p;
        p.first = v;
        p.second = i;
        return p;
    }
    

    ValueType GetAt(size_t rank) {
        //Select am element based on its rank from 
        //all collections. 
        return (ValueType)rank;
    }
    size_t IndexOf(Pivot element) { 
        //Get the index of a given element, or the first
        //Greater one. 
        return (size_t)element.first;  
    }

    
    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i].Close();
        }
        net::FlowControlChannel& flowControl = context_.flow_control_channel();

        //Partitioning happens here.
        
        //Environment
        size_t me = context_.my_rank(); //Local rank. 
        size_t p = context_.num_workers(); //Count of all workers (and count of target partitions)

        //Partitions in rank over all local collections.
        std::vector<size_t> partitions(p - 1);
        std::mt19937 rand(0); //Give const seed. May choose this one over the net.

        //Overall count of local items.
        size_t dataSize = 0;

        for(size_t i = 0; i < files_.size(); i++) {
            dataSize += files_[i].NumItems();
        };

        //Care! This does only work if dataSize is the same
        //on each worker. 
        size_t targetSize = dataSize;

        //Partition borders. Let there by binary search. 
        std::vector<size_t> left(p - 1);
        std::vector<size_t> width(p - 1);
        size_t prefixSize; //Count of all items before this worker.

        std::fill(left.begin(), left.end(), 0);
        std::fill(width.begin(), width.end(), dataSize);
       
        prefixSize = flowControl.PrefixSum(dataSize, std::plus<ValueType>(), false); 

        //Rank we search for
        std::vector<size_t> srank(p - 1);

        for(size_t r = 0; r  < p - 1; r++) {
            srank[r] = (r + 1) * targetSize;
        }

        //Auxillary Arrays
        std::vector<size_t> widthscan(p - 1);
        std::vector<size_t> widthsum(p - 1);
        std::vector<size_t> pivotrank(p - 1);
        std::vector<size_t> split(p - 1);
        std::vector<Pivot> pivots;
        pivots.reserve(p - 1);
        std::vector<size_t> splitsum(p - 1);

        //Partition loop 
        
        while(1) {
            flowControl.ArrayPrefixSum(width, widthscan, std::plus<ValueType>(), false);
            flowControl.ArrayAllReduce(width, widthsum, std::plus<ValueType>());
             
            size_t done = 0;

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] <= p) { //Not sure about this condition. It's been modified. 
                    partitions[r] = left[r];
                    done++;
                }
            }

            if(done == p - 1) break;
            
            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] > 1) {
                    pivotrank[r] = rand() % widthsum[r];
                } else {
                    pivotrank[r] = 0;
                }
            }

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] > 1 &&
                   widthscan[r] <= pivotrank[r] &&
                   pivotrank[r] < widthscan[r] + width[r]) {
                   
                   size_t localRank = left[r] + pivotrank[r] - widthscan[r]; 
                   ValueType pivotElement = GetAt(localRank);
                    
                   Pivot pivot = CreatePivot(pivotElement, localRank + prefixSize);

                   pivots[r] = pivot;
                } else {
                   pivots[r] = CreatePivot(GetAt(0), 0);
                }
            }

            flowControl.ArrayAllReduce(pivots, pivots, [] (const Pivot a, const Pivot b) { return b < a ? a : b; }); //Return maximal pivot (Is this  OK?);

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] <= 1) {
                    split[r] = 0;
                } else {
                    split[r] = IndexOf(pivots[r]) - left[r];
                }
            }

            flowControl.ArrayAllReduce(split, splitsum, std::plus<ValueType>());

            for(size_t r = 0; r < p - 1; r++) {
                if(widthsum[r] < 1) continue;
                
                if(widthsum[r] < srank[r])  {
                    left[r] += split[r];
                    width[r] -= split[r];
                    srank[r] -= splitsum[r];
                } else {
                    width[r] = split[r];
                }
            } 
        }

        //Cool, parts contains the global splittage now. 
        
        //For now, do "trivial" scattering. 
        channels_[0] = context_.GetNewChannel();
        channels_[1] = context_.GetNewChannel();

        std::vector<size_t> offset1(context_.num_workers(), 0);  
        std::vector<size_t> offset2(context_.num_workers(), 0);  
        
        size_t sizes[] = { files_[0].NumItems(), files_[1].NumItems() };
        for (size_t i = me; i != offset1.size(); ++i) {
            offset1[i] = sizes[0];
            offset2[i] = sizes[1];
        }
    
        channels_[0]->template Scatter<ValueType>(files_[0], offset1);
        channels_[1]->template Scatter<ValueType>(files_[1], offset2);
   }
};

template <typename ValueType, typename Stack>
template <typename Comperator, typename SecondDIA>
auto DIARef<ValueType, Stack>::Merge(
    SecondDIA second_dia, const Comperator &comperator) const {
    assert(IsValid());
    assert(second_dia.IsValid());

    using CompareResult
              = typename FunctionTraits<Comperator>::result_type;

    using MergeResultNode
              = TwoMergeNode<ValueType, DIARef, SecondDIA, Comperator>;
    
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
        "Comperator has the wrong return type");

    StatsNode* stats_node = AddChildStatsNode("Merge", NodeType::DOP);
    second_dia.AppendChildStatsNode(stats_node);
    auto merge_node
        = std::make_shared<MergeResultNode>(*this,
                                            second_dia,
                                            comperator,
                                            stats_node);

    auto merge_stack = merge_node->ProduceStack();

    return DIARef<ValueType, decltype(merge_stack)>(
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

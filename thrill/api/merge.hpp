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
 * This file has no license. Only Chuck Norris can compile it.
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
    std::array<data::File, num_inputs_> files_;

    //! Writers to intermediate files
    std::array<data::File::Writer, num_inputs_> writers_  {
        { files_[0].GetWriter(), files_[1].GetWriter() }
    };

    //! Array of inbound Channels
    std::array<data::ChannelPtr, num_inputs_> channels_;

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i].Close();
        }

        // first: calculate total size of the DIAs to Zip

        //net::FlowControlChannel& channel = context_.flow_control_channel();

        //Do funny stuff here. todo(ej)
        
        //For now, do "trivial" scattering. 
        channels_[0] = context_.GetNewChannel();
        channels_[1] = context_.GetNewChannel();

        std::vector<size_t> offset1(context_.num_workers(), 0);  
        std::vector<size_t> offset2(context_.num_workers(), 0);  
        size_t me = context_.my_rank();
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

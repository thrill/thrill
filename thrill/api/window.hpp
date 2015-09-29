/*******************************************************************************
 * thrill/api/window.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_WINDOW_HEADER
#define THRILL_API_WINDOW_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/ring_buffer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/flow_control_manager.hpp>

#include <algorithm>
#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIA, typename WindowFunction>
class WindowNode final : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    using Input = typename ParentDIA::ValueType;

    //! RingBuffer used and passed to user-defined function.
    using RingBuffer = common::RingBuffer<Input>;

public:
    WindowNode(const ParentDIA& parent,
               size_t window_size,
               const WindowFunction& window_function,
               StatsNode* stats_node)
        : DOpNode<ValueType>(parent.ctx(), { parent.node() }, stats_node),
          window_size_(window_size),
          window_function_(window_function)
    {
        // Hook PreOp(s)
        auto pre_op_fn = [=](const Input& input) {
                             PreOp(input);
                         };

        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    //! Executes the window operation.
    void Execute() final {
        MainOp();
    }

    void PushData(bool consume) final {
        data::File::Reader reader = file_.GetReader(consume);

        RingBuffer window = window_;
        // this may wrap around, but that is okay. -tb
        size_t rank = first_rank_ - (window_size_ - 1);

        sLOG << "WindowNode::PushData()"
             << "window.size()" << window.size()
             << "rank" << rank
             << "file_.num_items" << file_.num_items();

        for (size_t i = 0; i < file_.num_items(); ++i, ++rank) {
            // append an item.
            window.emplace_back(reader.Next<Input>());

            // only issue full window frames
            if (window.size() != window_size_) continue;

            // call window user-defined function
            this->PushItem(window_function_(rank, window));

            // return to window size - 1
            if (window.size() >= window_size_ - 1)
                window.pop_front();
        }
    }

    void Dispose() final { }

    FunctionStack<ValueType> ProduceStack() {
        return FunctionStack<ValueType>();
    }

private:
    //! Size of the window
    size_t window_size_;
    //! The window function which is applied to two elements.
    WindowFunction window_function_;

    //! cache the last k - 1 items for transmission
    RingBuffer window_ { window_size_ };

    //! Local data file
    data::File file_ { context_.GetFile() };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ = file_.GetWriter();

    //! PreOp: keep last k - 1 items (local window) and store items.
    void PreOp(const Input& input) {

        if (window_.size() >= window_size_ - 1)
            window_.pop_front();
        window_.push_back(input);

        writer_(input);
    }

    //! rank of our first element in file_
    size_t first_rank_;

    void MainOp() {
        writer_.Close();

        // get rank of our first element
        first_rank_ = context_.ExPrefixSum(file_.num_items());

        // copy our last elements into a vector
        std::vector<Input> my_last;
        my_last.reserve(window_size_ - 1);

        assert(window_.size() < window_size_);
        while (!window_.empty()) {
            my_last.emplace_back(window_.front());
            window_.pop_front();
        }

        // collective operation: get k - 1 predecessors
        std::vector<Input> pre =
            context_.flow_control_channel().Predecessor(window_size_ - 1, my_last);

        sLOG << "Window::MainOp()"
             << "first_rank_" << first_rank_
             << "window_size_" << window_size_
             << "pre.size()" << pre.size();

        assert(pre.size() == std::min(window_size_ - 1, first_rank_));

        // put k - 1 predecessors back into window_
        for (size_t i = 0; i < pre.size(); ++i)
            window_.push_back(pre[i]);
    }
};

template <typename ValueType, typename Stack>
template <typename WindowFunction>
auto DIA<ValueType, Stack>::Window(
    size_t window_size, const WindowFunction &window_function) const {
    assert(IsValid());

    using Result
              = typename FunctionTraits<WindowFunction>::result_type;

    using WindowResultNode
              = WindowNode<Result, DIA, WindowFunction>;

    // static_assert(
    //     std::is_convertible<
    //         ValueType,
    //         typename FunctionTraits<WindowFunction>::template arg<0>
    //         >::value,
    //     "WindowFunction has the wrong input type");

    // static_assert(
    //     std::is_convertible<
    //         ValueType,
    //         typename FunctionTraits<WindowFunction>::template arg<1> >::value,
    //     "WindowFunction has the wrong input type");

    // static_assert(
    //     std::is_convertible<
    //         typename FunctionTraits<WindowFunction>::result_type,
    //         ValueType>::value,
    //     "WindowFunction has the wrong input type");

    StatsNode* stats_node = AddChildStatsNode("Window", DIANodeType::DOP);
    auto shared_node
        = std::make_shared<WindowResultNode>(
        *this, window_size, window_function, stats_node);

    auto window_stack = shared_node->ProduceStack();

    return DIA<Result, decltype(window_stack)>(
        shared_node, window_stack, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_WINDOW_HEADER

/******************************************************************************/

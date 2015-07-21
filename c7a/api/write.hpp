/*******************************************************************************
 * c7a/api/write.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_WRITE_HEADER
#define C7A_API_WRITE_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/core/stage_builder.hpp>

#include <fstream>
#include <string>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentStack>
class WriteNode : public ActionNode
{
    static const bool debug = false;

public:
    using Super = ActionNode;
    using Super::result_file_;
    using Super::context_;

    using ParentInput = typename ParentStack::Input;

    WriteNode(Context& ctx,
              const std::shared_ptr<DIANode<ParentInput> >& parent,
              const ParentStack& parent_stack,
              const std::string& path_out)
        : ActionNode(ctx, { parent }, "Write"),
          path_out_(path_out),
          file_(path_out_),
          emit_(file_),
          parent_(parent)
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [=](ValueType input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        lop_chain_ = parent_stack.push(pre_op_fn).emit();
        parent_->RegisterChild(lop_chain_);
    }

    void PreOp(ValueType input) {
        emit_(input);
    }

    virtual ~WriteNode() {
        parent_->UnregisterChild(lop_chain_);
    }

    //! Closes the output file
    void Execute() override {
        this->StartExecutionTimer();
        sLOG << "closing file" << path_out_;
        emit_.Close();
        this->StopExecutionTimer();
    }

    /*!
     * Returns "[WriteNode]" and its id as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() override {
        return "[WriteNode] Id:" + result_file_.ToString();
    }

protected:
    //! OutputLineEmitter let's you write to files. Each element is written
    //! using ostream.
    class OutputLineEmitter
    {
    public:
        explicit OutputLineEmitter(std::ofstream& file)
            : out_(file) { }

        //! write item out using ostream formatting / serialization.
        void operator () (const ValueType& v) {
            out_ << v;
        }

        //! Flushes and closes the block (cannot be undone)
        //! No further emitt operations can be done afterwards.
        void Close() {
            assert(!closed_);
            closed_ = true;
            out_.close();
        }

        //! Writes the data to the target without closing the emitter
        void Flush() {
            out_.flush();
        }

    private:
        //! output stream
        std::ofstream& out_;

        //! whether the output stream is closed.
        bool closed_ = false;
    };

private:
    //! Path of the output file.
    std::string path_out_;

    //! File to write to
    std::ofstream file_;

    //! Emitter to file
    OutputLineEmitter emit_;

    std::shared_ptr<DIANode<ParentInput> > parent_;
    common::delegate<void(ParentInput)> lop_chain_;
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteToFileSystem(
    const std::string& filepath) const {

    using WriteResultNode = WriteNode<ValueType, Stack>;

    auto shared_node =
        std::make_shared<WriteResultNode>(node_->context(),
                                          node_,
                                          stack_,
                                          filepath);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_WRITE_HEADER

/******************************************************************************/

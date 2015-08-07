/*******************************************************************************
 * c7a/api/write.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_WRITE_SINGLE_FILE_HEADER
#define C7A_API_WRITE_SINGLE_FILE_HEADER

#include <c7a/api/action_node.hpp>
#include <c7a/api/dia.hpp>
#include <c7a/core/stage_builder.hpp>
#include <c7a/data/file.hpp>

#include <fstream>
#include <string>

namespace c7a {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename ParentDIARef>
class WriteLinesNode : public ActionNode
{
    static const bool debug = false;

public:
    using Super = ActionNode;
    using Super::result_file_;
    using Super::context_;

    WriteLinesNode(const ParentDIARef& parent,
				   const std::string& path_out,
				   StatsNode* stats_node)
        : ActionNode(parent.ctx(), { parent.node() },
                     "WriteSingleFile", stats_node),
          path_out_(path_out),
          file_(path_out_),
          writer_(result_file_.GetWriter())
    {
        sLOG << "Creating write node.";

        auto pre_op_fn = [=](const ValueType& input) {
                             PreOp(input);
                         };
        // close the function stack with our pre op and register it at parent
        // node for output
        auto lop_chain = parent.stack().push(pre_op_fn).emit();
        parent.node()->RegisterChild(lop_chain);
        
    }

    void PreOp(const ValueType& input) {
        writer_(input);
		size_ += input.size() + 1;
    }

    //! Closes the output file
    void Execute() override {
        writer_.Close();

		//(Portable) allocation of output file, setting individual file pointers. 
        size_t prefix_elem = context_.flow_control_channel().PrefixSum(size_, std::plus<size_t>(), false);
		if (context_.my_rank() == context_.num_workers() - 1) {
			file_.seekp(prefix_elem + size_ - 1);
			file_.put('\0');
		}		
		file_.seekp(prefix_elem);		
		context_.flow_control_channel().Await();

        data::File::Reader reader = result_file_.GetReader();
            
        for (size_t i = 0; i < result_file_.NumItems(); ++i) {
            file_ << reader.Next<ValueType>() << "\n";
        }

		file_.close();
    }

    void Dispose() override { }

    /*!
     * Returns "[WriteNode]" and its id as a string.
     * \return "[WriteNode]"
     */
    std::string ToString() override {
        return "[WriteNode] Id:" + result_file_.ToString();
    }

private:
    //! Path of the output file.
    std::string path_out_;

    //! File to write to
    std::ofstream file_;

	//! Local file size
	size_t size_ = 0;

    //! File writer used.
    data::File::Writer writer_;
};

template <typename ValueType, typename Stack>
void DIARef<ValueType, Stack>::WriteLines(
    const std::string& filepath) const {

	static_assert(std::is_same<ValueType, std::string>::value,
				  "WriteLines needs an std::string as input parameter");

    using WriteResultNode = WriteLinesNode<ValueType, DIARef>;

    StatsNode* stats_node = AddChildStatsNode("Write", NodeType::ACTION);
    auto shared_node =
        std::make_shared<WriteResultNode>(*this,
                                          filepath,
                                          stats_node);

    core::StageBuilder().RunScope(shared_node.get());
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_WRITE_HEADER

/******************************************************************************/

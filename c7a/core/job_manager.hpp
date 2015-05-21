/*******************************************************************************
 * c7a/core/job_manager.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef C7A_CORE_JOB_MANAGER_HEADER
#define C7A_CORE_JOB_MANAGER_HEADER

#include "c7a/data/data_manager.hpp"

namespace c7a {
namespace core {
class JobManager
{
public:
    JobManager() : net_dispatcher_(), cmp_(net_dispatcher_), data_manager_(cmp_)
    {
        //TODO: Run Dispatcher in own thread
    }

    data::DataManager & get_data_manager()
    {
        return data_manager_;
    }

    //When a DIA calls HasNext() on the data manager but HasNext() returns false and IsClosed() returns false too, then the DIA needs to wait. Therefore, this function is needed
    void WaitOnData()
    {
        //TODO: Call on wait and as soon as data manager has data, it needs to signal the DIA to continue
        //Use Condition Variables to do this.
    }

private:
    net::NetDispatcher net_dispatcher_;
    net::ChannelMultiplexer cmp_;
    data::DataManager data_manager_;
};
}  // namespace core
}  // namespace c7a

#endif // !C7A_CORE_JOB_MANAGER_HEADER

/******************************************************************************/

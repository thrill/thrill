/*******************************************************************************
 * Perform All-Reduce in the hypercube for the workers.
 *
 * Author: Robert Hangu
 ******************************************************************************/

#include <iostream>
#include <thread>
#include <vector>
#include <sstream>

#include <c7a/net/net-group.hpp>
#include <c7a/net/flow_control_channel.hpp>

#if THIS_BELONGS_INTO_TESTS

// Number of workers in the network
int worker_count;

void CommunicationOfOneThread(c7a::NetGroup* net) {
    // For each dimension of the hypercube, exchange data between workers with
    // different bits at position d

    int local_value = net->MyRank();
    net->AllReduce<int>(local_value, c7a::SumOp<int>());
//    ASSSERT_EQ(local_value = net->Size() * (net->Size() - 1) / 2);
}

int main() {
    worker_count = 8;

    c7a::NetGroup::ExecuteLocalMock(worker_count, CommunicationOfOneThread);

    return 0;
}

#endif // THIS_BELONGS_INTO_TESTS

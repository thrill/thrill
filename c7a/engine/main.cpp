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

    int local_value = net -> MyRank();
    for (int d = 1; d < worker_count; d <<= 1) {
        // Send local_value to worker with id id ^ d
        if ((net -> MyRank() ^ d) < worker_count){
            net -> GetConnection(net -> MyRank() ^ d).Send(local_value);
            std::cout << "LOCAL: Worker " << net -> MyRank() << ": Sending " << local_value
                      << " to worker " << (net -> MyRank() ^ d) << "\n";
        }

        // Receive local_value from worker with id id ^ d
        int recv_data;
        if ((net -> MyRank() ^ d) < worker_count) {
            net -> GetConnection(net -> MyRank() ^ d).Receive(&recv_data);
            local_value += recv_data;
            std::cout << "LOCAL: Worker " << net -> MyRank() << ": Received " << recv_data
                      << " from worker " << (net -> MyRank() ^ d)
                      << " local_value = " << local_value << "\n";
        }
    }
    
    std::cout << "LOCAL: local_value after all reduce " << local_value << "\n";
}

int main() {
    worker_count = 8;

    c7a::NetGroup::ExecuteLocalMock(worker_count, CommunicationOfOneThread);

    return 0;
}

#endif // THIS_BELONGS_INTO_TESTS

/*
 * Perform All-Reduce in the hypercube for the workers.
 * Author: Robert Hangu
 */
//#include "controller.hpp"
#include "mock-network.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <sstream>

#if THIS_BELONGS_INTO_TESTS

std::vector<std::thread> threads;

// Number of workers in the network
int worker_count;

void worker(c7a::MockNetwork& net, size_t id, int local_value) {
    c7a::MockSelect client(net, id);

    // For each dimension of the hypercube, exchange data between workers with
    // different bits at position d
    for (int d = 1; d <= worker_count; d <<= 1) {
        // Convert the local int to string
        std::ostringstream local_value_string;
        local_value_string << local_value;
        // Send local_value to worker with id id ^ d
        client.sendToWorkerString(id ^ d, local_value_string.str());

        // Receive local_value from worker with id id ^ d
        size_t from_whom;
        std::string recv_data;
        client.receiveFromAnyString(&from_whom, &recv_data);
        std::cout << "Worker " << id << ": Received " << recv_data
                  << " from worker " << from_whom << "\n";
    }
}

int main() {
    c7a::MockNetwork net;

    for (int i = 0; i < 8; ++i)
        threads.push_back(std::thread(worker, std::ref(net), i, 2 * (i + 1)));

    for (int i = 0; i < threads.size(); ++i)
        threads[i].join();

    return 0;
}

#endif // THIS_BELONGS_INTO_TESTS

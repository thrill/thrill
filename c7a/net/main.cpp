/*******************************************************************************
 * c7a/net/
 *
 */
#include <c7a/net/net-group.hpp>

#include <cstdlib>

using namespace c7a::net;

bool debug = true;

int main(int argc, char *argv[])
{
    int num_workers = atoi(argv[1]);
    NetGroup::ExecuteLocalMock(
        num_workers, [](NetGroup* net) {
            size_t local_value = net->MyRank();
        //    net->ReduceToRoot(local_value);
            net->Broadcast(local_value);
            }
        );

    return 0;
}

#include <sstream>
#include "../c7a/engine/worker.hpp"
#include "../c7a/engine/mock-network.hpp"

static const int num_workers = 5;

std::vector<int> remove(std::vector<int> v, int i) {
    std::vector<int> newv(v);
    auto it = std::find(newv.begin(), newv.end(), i);
    if (it != newv.end())
        newv.erase(it);
    return newv;
}

std::string toStr(std::vector<int> v)
{
    std::string s;
    for (auto x : v)
    {
        s += static_cast<char>(x);
    }
    return s;
}

int main()
{
    c7a::engine::MockNetwork net;

    std::vector<std::string> words = {"word0", "word1", "word2", "word4", "word4", "word4"};

    c7a::engine::Worker w0(0, 5, net);
//    c7a::engine::Worker w1(1, 5, net);
//    c7a::engine::Worker w2(2, 5, net);
//    c7a::engine::Worker w3(3, 5, net);
//    c7a::engine::Worker w4(4, 5, net);
    std::thread t0([&]{ w0.reduce<std::string, int>(words); });
//    std::thread t1([&]{ w1.reduce<std::string, int>(words); });
//    std::thread t2([&]{ w2.reduce<std::string, int>(words); });
//    std::thread t3([&]{ w3.reduce<std::string, int>(words); });
//    std::thread t4([&]{ w4.reduce<std::string, int>(words); });
    t0.join();
//    t1.join();
//    t2.join();
//    t3.join();
//    t4.join();

    /*std::vector<c7a::engine::Worker> workerObjs;
    std::vector<std::thread*> threads;

    for (int n=0; n<num_workers; n++) {
        c7a::engine::Worker worker(n, num_workers, net);
        workerObjs.push_back(worker);

        threads[n] = new std::thread([&]{
            worker.reduce<std::string, int>(words);
        });

        std::thread *t = threads[n];
        t->join();

        std::cout << "reduce 1" << std::endl;
    }

    //for (auto& th : threads) { th->join(); };*/

    return 0;
}
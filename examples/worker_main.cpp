#include <sstream>
#include "../c7a/engine/worker.hpp"
#include "../c7a/engine/mock-network.hpp"

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
    std::vector<int> workers = {0, 1, 2, 3, 4};
    std::vector<std::string> words = {"word0", "word1", "word2", "word3", "word4", "word4"};

    c7a::engine::Worker w0(workers.at(0), remove(workers, workers.at(0)), net);
    c7a::engine::Worker w1(workers.at(1), remove(workers, workers.at(1)), net);
    c7a::engine::Worker w2(workers.at(2), remove(workers, workers.at(2)), net);
    c7a::engine::Worker w3(workers.at(3), remove(workers, workers.at(3)), net);
    c7a::engine::Worker w4(workers.at(4), remove(workers, workers.at(4)), net);
    std::thread t0([&]{ w0.reduce<std::string, int>(words); });
    std::thread t1([&]{ w1.reduce<std::string, int>(words); });
    std::thread t2([&]{ w2.reduce<std::string, int>(words); });
    std::thread t3([&]{ w3.reduce<std::string, int>(words); });
    std::thread t4([&]{ w4.reduce<std::string, int>(words); });
    t0.join();
    t1.join();
    t2.join();
    t3.join();
    t4.join();

    /*
    std::vector<c7a::engine::Worker> workerObjs;
    std::vector<std::thread*> threads;

    for (int n=0; n<workers.size(); n++) {
        std::cout << workers.at(n) << std::endl;
        workerObjs.push_back(c7a::engine::Worker(workers.at(n), remove(workers, workers.at(n)), net));
        workerObjs.at(n).print();

        std::cout << "reduce" << std::endl;
        threads[n] = new std::thread([&]{
            std::cout << "reduce in" << std::endl;
            workerObjs.at(n).print();
            //workerObjs.at(n).reduce<std::string, int>(words);
        });
    }

    for (auto& th : threads) { th->join(); };*/

    return 0;
}
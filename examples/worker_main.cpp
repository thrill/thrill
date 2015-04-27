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

    // declare reduce function
    std::function<int (int, int)> f_reduce = [] (const int val1, const int val2) ->int { return val1 + val2; };

    c7a::engine::Worker w0(0, 5, net);
    for (auto word : words) {
        w0.reduce<std::string, int>(std::make_pair(word, 1), f_reduce);
    }
    w0.flush<std::string, int>(f_reduce);
    w0.receive<std::string, int>(f_reduce);

    c7a::engine::Worker w1(1, 5, net);
    for (auto word : words) {
        w1.reduce<std::string, int>(std::make_pair(word, 1), f_reduce);
    }
    w1.flush<std::string, int>(f_reduce);
    w1.receive<std::string, int>(f_reduce);

    c7a::engine::Worker w2(2, 5, net);
    for (auto word : words) {
        w2.reduce<std::string, int>(std::make_pair(word, 1), f_reduce);
    }
    w2.flush<std::string, int>(f_reduce);
    w2.receive<std::string, int>(f_reduce);

    c7a::engine::Worker w3(3, 5, net);
    for (auto word : words) {
        w3.reduce<std::string, int>(std::make_pair(word, 1), f_reduce);
    }
    w3.flush<std::string, int>(f_reduce);
    w3.receive<std::string, int>(f_reduce);

    c7a::engine::Worker w4(4, 5, net);
    for (auto word : words) {
        w4.reduce<std::string, int>(std::make_pair(word, 1), f_reduce);
    }
    w4.flush<std::string, int>(f_reduce);
    w4.receive<std::string, int>(f_reduce);


//    std::thread t5([&]{
//        w0.receive<std::string, int>(f_reduce);
//    });
//    t5.join();
//    std::thread t6([&]{
//        w1.receive<std::string, int>(f_reduce);
//    });
//    t6.join();
//    std::thread t7([&]{
//        w2.receive<std::string, int>(f_reduce);
//    });
//    t7.join();
//    std::thread t8([&]{
//        w3.receive<std::string, int>(f_reduce);
//    });
//    t8.join();
//    std::thread t9([&]{
//        w4.receive<std::string, int>(f_reduce);
//    });
//    t9.join();

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
//
// Created by Matthias Stumpp on 25/04/15.
//

#ifndef C7A_DUMMY_COMMUNICATOR_HPP
#define C7A_DUMMY_COMMUNICATOR_HPP

namespace c7a {
namespace engine {

class DummyCommLayer {
public:

    template<typename T>
    void sendToWorker(int id, T data) {

    }

    template<typename T>
    void receiveCallback(int id, ) {

    }

private:

};

}
}

#endif //C7A_DUMMY_COMMUNICATOR_HPP
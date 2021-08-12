#include <boost/interprocess/sync/named_mutex.hpp>
#include <cstring>
#include <iostream>
#include <stdlib.h>
#include <string>
#include <cstdlib> //std::system
#include "thread"
#include <signal.h>

#include "utility.hpp"

using namespace boost::interprocess;

volatile sig_atomic_t stopped = 0;

void my_handler(int s){
    stopped = 1;
}

// Consumer, receive the request and send back the reply
int main(int argc, char *argv[])
{
    signal(SIGINT, my_handler);

    int size = std::stoi(argv[1]);
    // this will throw error if memory not exists
    managed_shared_memory segment(open_only, "MySharedMemory");

    //Find the message queue using the name
    shm::message_queue* request_queue = segment.find<shm::message_queue>("request_queue").first;
    shm::message_queue* reply_queue = segment.find<shm::message_queue>("reply_queue").first;

    char* reply = static_cast<char*>(segment.allocate(size*sizeof(char)));
    void* buffer = malloc(size*sizeof(char));

    while (!stopped) {
        offset_ptr<char> ptr;
        while(!request_queue->pop(ptr) && !stopped) {
            // pop failed, retry
        }
        // read the request
        memcpy(buffer, ptr.get(), size);
        // write the reply
        memset(reply, '*', size);
        while (!reply_queue->push(reply) && !stopped) {
            // push failed, retry
        };
    }
    segment.deallocate(reply);
    free(buffer);
    std::cout << "Consumer finished.\n";
}
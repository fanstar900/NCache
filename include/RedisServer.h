#ifndef REDIS_SERVER_H
#define REDIS_SERVER_H

#include <string>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <unordered_set>

class RedisServer {
public:
    explicit RedisServer(int port);

    // Accept clients until shutdown() is called. Returns false if the socket
    // could not be set up; returns true after a clean shutdown once every
    // client thread has finished.
    bool run();

    // Thread-safe: stop accepting, disconnect clients, make run() return.
    // Must not be called from a signal handler (main uses sigwait instead).
    void shutdown();

private:
    void handleClient(int client_socket);

    int port;
    std::atomic<int> server_socket;
    std::atomic<bool> running;

    std::mutex clients_mutex;
    std::condition_variable clients_cv;
    std::unordered_set<int> client_sockets;
    size_t active_clients = 0;
};

#endif

#include "../include/RedisServer.h"
#include "../include/RedisDatabase.h"
#include <iostream>
#include <thread>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <string>
#include <signal.h>
#include <pthread.h>

static const char* DUMP_FILE = "dump.my_rdb";

int main(int argc, char* argv[]) {
    int port = 6379; // default
    if (argc >= 2) {
        try {
            size_t idx;
            port = std::stoi(argv[1], &idx);
            if (argv[1][idx] != '\0' || port < 1 || port > 65535) throw std::out_of_range("port");
        } catch (const std::exception&) {
            std::cerr << "Invalid port: " << argv[1] << "\n";
            return 1;
        }
    }

    // Block SIGINT/SIGTERM in every thread; a dedicated thread receives them
    // with sigwait(), so shutdown logic runs in normal (non-signal) context.
    sigset_t sigs;
    sigemptyset(&sigs);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &sigs, nullptr);
    signal(SIGPIPE, SIG_IGN);

    if (RedisDatabase::getInstance().load(DUMP_FILE))
        std::cout << "Database Loaded From " << DUMP_FILE << "\n";
    else
        std::cout << "No dump found or load failed; starting with an empty database.\n";

    RedisServer server(port);

    // Background persistance: dump the database every 300 seconds. (5 * 60 seconds save database)
    std::mutex stopMutex;
    std::condition_variable stopCv;
    bool stopping = false;
    std::thread persistanceThread([&]() {
        std::unique_lock<std::mutex> lock(stopMutex);
        while (!stopCv.wait_for(lock, std::chrono::seconds(300), [&] { return stopping; })) {
            if (!RedisDatabase::getInstance().dump(DUMP_FILE))
                std::cerr << "Error Dumping Database\n";
            else
                std::cout << "Database Dumped to " << DUMP_FILE << "\n";
        }
    });

    std::atomic<bool> serverDone{false};
    std::thread signalThread([&]() {
        int sig = 0;
        if (sigwait(&sigs, &sig) == 0 && !serverDone) {
            std::cout << "\nCaught signal " << sig << ", shutting down...\n";
            server.shutdown();
        }
    });

    bool ok = server.run();

    // If run() ended on its own (startup or accept failure), wake the signal thread.
    serverDone = true;
    pthread_kill(signalThread.native_handle(), SIGTERM);
    signalThread.join();

    {
        std::lock_guard<std::mutex> lock(stopMutex);
        stopping = true;
    }
    stopCv.notify_all();
    persistanceThread.join();

    if (!ok)
        return 1;

    // All clients are gone: persist the final state.
    if (RedisDatabase::getInstance().dump(DUMP_FILE))
        std::cout << "Database Dumped to " << DUMP_FILE << "\n";
    else
        std::cerr << "Error dumping database\n";
    std::cout << "Server Shutdown Complete!\n";
    return 0;
}

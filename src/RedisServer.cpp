#include "../include/RedisServer.h"
#include "../include/RedisCommandHandler.h"

#include <iostream>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <vector>
#include <thread>
#include <cerrno>

// Upper bound on unparsed bytes buffered per client (protects memory).
static const size_t MAX_CLIENT_BUFFER = 1024ULL * 1024 * 1024;

RedisServer::RedisServer(int port) : port(port), server_socket(-1), running(true) {}

void RedisServer::shutdown() {
    if (!running.exchange(false))
        return; // already shutting down

    // shutdown() on the listening socket wakes up the blocked accept() call.
    int fd = server_socket.load();
    if (fd != -1)
        ::shutdown(fd, SHUT_RDWR);

    // Wake every client thread blocked in recv().
    std::lock_guard<std::mutex> lock(clients_mutex);
    for (int c : client_sockets)
        ::shutdown(c, SHUT_RDWR);
}

// Send the whole buffer, handling partial writes. MSG_NOSIGNAL prevents a
// disconnected client from killing the process with SIGPIPE.
static bool sendAll(int fd, const std::string& data) {
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        sent += n;
    }
    return true;
}

void RedisServer::handleClient(int client_socket) {
    RedisCommandHandler cmdHandler;
    std::string buffer;       // bytes received but not yet parsed
    std::vector<std::string> tokens;
    std::string error;
    char chunk[16 * 1024];

    while (running) {
        ssize_t bytes = recv(client_socket, chunk, sizeof(chunk), 0);
        if (bytes < 0 && errno == EINTR) continue;
        if (bytes <= 0) break;
        buffer.append(chunk, bytes);

        // A single recv() may contain part of a command or several pipelined
        // commands, so parse as many complete commands as are available.
        std::string responses;
        size_t pos = 0;
        bool closeConn = false;
        while (true) {
            auto status = RedisCommandHandler::parseCommand(buffer, pos, tokens, error);
            if (status == RedisCommandHandler::ParseStatus::Incomplete) break;
            if (status == RedisCommandHandler::ParseStatus::Error) {
                responses += error;   // like Redis: reply with the error, then close
                closeConn = true;
                break;
            }
            if (!tokens.empty())
                responses += cmdHandler.processCommand(tokens);
        }
        buffer.erase(0, pos);

        if (!responses.empty() && !sendAll(client_socket, responses)) break;
        if (closeConn) break;
        if (buffer.size() > MAX_CLIENT_BUFFER) {
            sendAll(client_socket, "-ERR Protocol error: request too large\r\n");
            break;
        }
    }

    close(client_socket);
    std::lock_guard<std::mutex> lock(clients_mutex);
    client_sockets.erase(client_socket);
    if (--active_clients == 0)
        clients_cv.notify_all();
}

bool RedisServer::run() {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        std::cerr << "Error Creating Server Socket\n";
        return false;
    }

    int opt = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(fd, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cerr << "Error Binding Server Socket\n";
        close(fd);
        return false;
    }

    if (listen(fd, 128) < 0) {
        std::cerr << "Error Listening On Server Socket\n";
        close(fd);
        return false;
    }

    server_socket = fd;
    std::cout << "Redis Server Listening On Port " << port << "\n";

    // If shutdown() ran before server_socket was set, `running` is already false.
    while (running) {
        int client_socket = accept(fd, nullptr, nullptr);
        if (client_socket < 0) {
            if (!running) break;
            if (errno == EINTR || errno == ECONNABORTED) continue;
            std::cerr << "Error Accepting Client Connection\n";
            if (errno == EMFILE || errno == ENFILE) {
                // Out of file descriptors: back off instead of spinning.
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            break;
        }

        {
            std::lock_guard<std::mutex> lock(clients_mutex);
            if (!running) { close(client_socket); break; }
            client_sockets.insert(client_socket);
            ++active_clients;
        }
        // Threads are detached; active_clients tracks them so shutdown can wait.
        std::thread(&RedisServer::handleClient, this, client_socket).detach();
    }

    running = false;
    server_socket = -1;
    close(fd);

    // Disconnect remaining clients and wait for their threads to finish.
    std::unique_lock<std::mutex> lock(clients_mutex);
    for (int c : client_sockets)
        ::shutdown(c, SHUT_RDWR);
    clients_cv.wait(lock, [this] { return active_clients == 0; });
    return true;
}

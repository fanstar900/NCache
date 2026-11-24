#ifndef REDIS_COMMAND_HANDLER_H
#define REDIS_COMMAND_HANDLER_H

#include <string>
#include <vector>
#include <cstddef>

class RedisCommandHandler {
public:
    enum class ParseStatus { Complete, Incomplete, Error };

    RedisCommandHandler();

    // Try to parse one command from `buffer` starting at `pos`.
    // Complete:   `tokens` holds the command and `pos` is advanced past it.
    // Incomplete: more bytes are needed; `pos` is unchanged.
    // Error:      protocol error; `error` holds a RESP error reply.
    static ParseStatus parseCommand(const std::string& buffer, size_t& pos,
                                    std::vector<std::string>& tokens, std::string& error);

    // Execute a parsed command and return a RESP-formatted response.
    std::string processCommand(const std::vector<std::string>& tokens);

    // Convenience: parse and execute every command contained in `commandLine`.
    std::string processCommand(const std::string& commandLine);
};

#endif

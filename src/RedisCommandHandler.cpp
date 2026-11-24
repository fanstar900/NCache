#include "../include/RedisCommandHandler.h"
#include "../include/RedisDatabase.h"

#include <vector>
#include <sstream>
#include <algorithm>
#include <exception>
#include <cctype>
#include <climits>

// RESP parser:
// *2\r\n$4\r\nPING\r\n$4\r\nTEST\r\n
// *2 -> array has 2 elements
// $4 -> next string has 4 characters
// PING
// TEST

static const long long MAX_BULK_LEN = 512LL * 1024 * 1024;   // Redis' proto-max-bulk-len
static const long long MAX_ARRAY_LEN = 1024LL * 1024;
static const size_t MAX_INLINE_LEN = 64 * 1024;

// Parse a signed integer from buf[start, end). Returns false on any non-digit.
static bool parseInteger(const std::string& buf, size_t start, size_t end, long long& out) {
    if (start >= end) return false;
    bool neg = false;
    size_t i = start;
    if (buf[i] == '-') { neg = true; ++i; }
    if (i >= end) return false;
    long long val = 0;
    for (; i < end; ++i) {
        if (!std::isdigit(static_cast<unsigned char>(buf[i]))) return false;
        if (val > (LLONG_MAX - 9) / 10) return false;
        val = val * 10 + (buf[i] - '0');
    }
    out = neg ? -val : val;
    return true;
}

RedisCommandHandler::ParseStatus RedisCommandHandler::parseCommand(
        const std::string& buf, size_t& pos, std::vector<std::string>& tokens, std::string& error) {
    tokens.clear();
    if (pos >= buf.size()) return ParseStatus::Incomplete;

    // If it doesnt start with '*', treat it as an inline command split by whitespace.
    if (buf[pos] != '*') {
        size_t nl = buf.find('\n', pos);
        if (nl == std::string::npos) {
            if (buf.size() - pos > MAX_INLINE_LEN) {
                error = "-ERR Protocol error: too big inline request\r\n";
                return ParseStatus::Error;
            }
            return ParseStatus::Incomplete;
        }
        std::istringstream iss(buf.substr(pos, nl - pos));
        std::string token;
        while (iss >> token)
            tokens.push_back(token);
        pos = nl + 1;
        return ParseStatus::Complete;   // may be empty (blank line) - caller skips it
    }

    size_t p = pos + 1; // skip '*'
    // crlf = Carriage Return (\r), Line Feed (\n)
    size_t crlf = buf.find("\r\n", p);
    if (crlf == std::string::npos) return ParseStatus::Incomplete;

    long long numElements;
    if (!parseInteger(buf, p, crlf, numElements) || numElements > MAX_ARRAY_LEN) {
        error = "-ERR Protocol error: invalid multibulk length\r\n";
        return ParseStatus::Error;
    }
    p = crlf + 2;

    std::vector<std::string> out;
    for (long long i = 0; i < numElements; i++) {
        if (p >= buf.size()) return ParseStatus::Incomplete;
        if (buf[p] != '$') {
            error = "-ERR Protocol error: expected '$'\r\n";
            return ParseStatus::Error;
        }
        p++; // skip '$'

        crlf = buf.find("\r\n", p);
        if (crlf == std::string::npos) return ParseStatus::Incomplete;
        long long len;
        if (!parseInteger(buf, p, crlf, len) || len < 0 || len > MAX_BULK_LEN) {
            error = "-ERR Protocol error: invalid bulk length\r\n";
            return ParseStatus::Error;
        }
        p = crlf + 2;

        if (p + len + 2 > buf.size()) return ParseStatus::Incomplete;
        if (buf[p + len] != '\r' || buf[p + len + 1] != '\n') {
            error = "-ERR Protocol error: bulk string not terminated by CRLF\r\n";
            return ParseStatus::Error;
        }
        out.push_back(buf.substr(p, len));
        p += len + 2; // skip token and CRLF
    }
    tokens = std::move(out);
    pos = p;
    return ParseStatus::Complete;
}

//----------------------
// Reply helpers
//----------------------
static std::string bulk(const std::string& s) {
    return "$" + std::to_string(s.size()) + "\r\n" + s + "\r\n";
}

static std::string integer(long long n) {
    return ":" + std::to_string(n) + "\r\n";
}

static std::string array(const std::vector<std::string>& items) {
    std::ostringstream oss;
    oss << "*" << items.size() << "\r\n";
    for (const auto& item : items)
        oss << "$" << item.size() << "\r\n" << item << "\r\n";
    return oss.str();
}

static std::string wrongArgs(const std::string& cmd) {
    return "-ERR wrong number of arguments for '" + cmd + "' command\r\n";
}

// Parse an int argument the way Redis does: the whole token must be a number.
static bool toInt(const std::string& s, int& out) {
    long long v;
    if (!parseInteger(s, 0, s.size(), v) || v < INT_MIN || v > INT_MAX) return false;
    out = static_cast<int>(v);
    return true;
}

static const char* NOT_INTEGER = "-ERR value is not an integer or out of range\r\n";

//----------------------
// Common Commands
//----------------------
static std::string handlePing(const std::vector<std::string>& tokens, RedisDatabase& /*db*/) {
    if (tokens.size() >= 2)
        return bulk(tokens[1]);
    return "+PONG\r\n";
}

static std::string handleEcho(const std::vector<std::string>& tokens, RedisDatabase& /*db*/) {
    if (tokens.size() != 2)
        return wrongArgs("echo");
    return bulk(tokens[1]);
}

static std::string handleFlushAll(const std::vector<std::string>& /*tokens*/, RedisDatabase& db) {
    db.flushAll();
    return "+OK\r\n";
}

//----------------------
// Key/Value Operations
//----------------------
static std::string handleSet(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 3)
        return tokens.size() < 3 ? wrongArgs("set") : "-ERR syntax error\r\n";
    db.set(tokens[1], tokens[2]);
    return "+OK\r\n";
}

static std::string handleGet(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("get");
    std::string value;
    if (db.get(tokens[1], value))
        return bulk(value);
    return "$-1\r\n";
}

static std::string handleKeys(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("keys");
    return array(db.keys(tokens[1]));
}

static std::string handleType(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("type");
    return "+" + db.type(tokens[1]) + "\r\n";
}

static std::string handleDel(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 2)
        return wrongArgs("del");
    long long removed = 0;
    for (size_t i = 1; i < tokens.size(); ++i)
        if (db.del(tokens[i])) ++removed;
    return integer(removed);
}

static std::string handleExpire(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 3)
        return wrongArgs("expire");
    long long seconds;
    if (!parseInteger(tokens[2], 0, tokens[2].size(), seconds))
        return NOT_INTEGER;
    return integer(db.expire(tokens[1], seconds) ? 1 : 0);
}

static std::string handleRename(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 3)
        return wrongArgs("rename");
    if (db.rename(tokens[1], tokens[2]))
        return "+OK\r\n";
    return "-ERR no such key\r\n";
}

// List Opreations
static std::string handleLget(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("lget");
    return array(db.lget(tokens[1]));
}

static std::string handleLlen(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("llen");
    return integer(db.llen(tokens[1]));
}

static std::string handleLpush(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return wrongArgs("lpush");
    std::vector<std::string> values(tokens.begin() + 2, tokens.end());
    return integer(db.lpush(tokens[1], values));
}

static std::string handleRpush(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return wrongArgs("rpush");
    std::vector<std::string> values(tokens.begin() + 2, tokens.end());
    return integer(db.rpush(tokens[1], values));
}

static std::string handleLpop(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("lpop");
    std::string val;
    if (db.lpop(tokens[1], val))
        return bulk(val);
    return "$-1\r\n";
}

static std::string handleRpop(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("rpop");
    std::string val;
    if (db.rpop(tokens[1], val))
        return bulk(val);
    return "$-1\r\n";
}

static std::string handleLrem(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 4)
        return wrongArgs("lrem");
    int count;
    if (!toInt(tokens[2], count))
        return NOT_INTEGER;
    return integer(db.lrem(tokens[1], count, tokens[3]));
}

static std::string handleLindex(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 3)
        return wrongArgs("lindex");
    int index;
    if (!toInt(tokens[2], index))
        return NOT_INTEGER;
    std::string value;
    if (db.lindex(tokens[1], index, value))
        return bulk(value);
    return "$-1\r\n";
}

static std::string handleLset(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 4)
        return wrongArgs("lset");
    int index;
    if (!toInt(tokens[2], index))
        return NOT_INTEGER;
    switch (db.lset(tokens[1], index, tokens[3])) {
        case RedisDatabase::LsetResult::Ok:        return "+OK\r\n";
        case RedisDatabase::LsetResult::NoSuchKey: return "-ERR no such key\r\n";
        default:                                   return "-ERR index out of range\r\n";
    }
}

// Hash Operations
static std::vector<std::pair<std::string, std::string>> fieldPairs(const std::vector<std::string>& tokens) {
    std::vector<std::pair<std::string, std::string>> fieldValues;
    for (size_t i = 2; i + 1 < tokens.size(); i += 2)
        fieldValues.emplace_back(tokens[i], tokens[i + 1]);
    return fieldValues;
}

static std::string handleHset(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 4 || (tokens.size() % 2) == 1)
        return wrongArgs("hset");
    return integer(db.hmset(tokens[1], fieldPairs(tokens)));
}

static std::string handleHget(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 3)
        return wrongArgs("hget");
    std::string value;
    if (db.hget(tokens[1], tokens[2], value))
        return bulk(value);
    return "$-1\r\n";
}

static std::string handleHexists(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 3)
        return wrongArgs("hexists");
    return integer(db.hexists(tokens[1], tokens[2]) ? 1 : 0);
}

static std::string handleHdel(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 3)
        return wrongArgs("hdel");
    std::vector<std::string> fields(tokens.begin() + 2, tokens.end());
    return integer(db.hdel(tokens[1], fields));
}

static std::string handleHgetall(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("hgetall");
    auto hash = db.hgetall(tokens[1]);
    std::vector<std::string> flat;
    flat.reserve(hash.size() * 2);
    for (const auto& pair : hash) {
        flat.push_back(pair.first);
        flat.push_back(pair.second);
    }
    return array(flat);
}

static std::string handleHkeys(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("hkeys");
    return array(db.hkeys(tokens[1]));
}

static std::string handleHvals(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("hvals");
    return array(db.hvals(tokens[1]));
}

static std::string handleHlen(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() != 2)
        return wrongArgs("hlen");
    return integer(db.hlen(tokens[1]));
}

static std::string handleHmset(const std::vector<std::string>& tokens, RedisDatabase& db) {
    if (tokens.size() < 4 || (tokens.size() % 2) == 1)
        return wrongArgs("hmset");
    db.hmset(tokens[1], fieldPairs(tokens));
    return "+OK\r\n";
}

RedisCommandHandler::RedisCommandHandler() {}

std::string RedisCommandHandler::processCommand(const std::vector<std::string>& tokens) {
    if (tokens.empty()) return "-ERR empty command\r\n";

    std::string cmd = tokens[0];
    std::transform(cmd.begin(), cmd.end(), cmd.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    RedisDatabase& db = RedisDatabase::getInstance();

    try {
        // Common Commands
        if (cmd == "PING")          return handlePing(tokens, db);
        if (cmd == "ECHO")          return handleEcho(tokens, db);
        if (cmd == "FLUSHALL")      return handleFlushAll(tokens, db);
        // Key/Value Operations
        if (cmd == "SET")           return handleSet(tokens, db);
        if (cmd == "GET")           return handleGet(tokens, db);
        if (cmd == "KEYS")          return handleKeys(tokens, db);
        if (cmd == "TYPE")          return handleType(tokens, db);
        if (cmd == "DEL" || cmd == "UNLINK") return handleDel(tokens, db);
        if (cmd == "EXPIRE")        return handleExpire(tokens, db);
        if (cmd == "RENAME")        return handleRename(tokens, db);
        // List Operations
        if (cmd == "LGET")          return handleLget(tokens, db);
        if (cmd == "LLEN")          return handleLlen(tokens, db);
        if (cmd == "LPUSH")         return handleLpush(tokens, db);
        if (cmd == "RPUSH")         return handleRpush(tokens, db);
        if (cmd == "LPOP")          return handleLpop(tokens, db);
        if (cmd == "RPOP")          return handleRpop(tokens, db);
        if (cmd == "LREM")          return handleLrem(tokens, db);
        if (cmd == "LINDEX")        return handleLindex(tokens, db);
        if (cmd == "LSET")          return handleLset(tokens, db);
        // Hash Operations
        if (cmd == "HSET")          return handleHset(tokens, db);
        if (cmd == "HGET")          return handleHget(tokens, db);
        if (cmd == "HEXISTS")       return handleHexists(tokens, db);
        if (cmd == "HDEL")          return handleHdel(tokens, db);
        if (cmd == "HGETALL")       return handleHgetall(tokens, db);
        if (cmd == "HKEYS")         return handleHkeys(tokens, db);
        if (cmd == "HVALS")         return handleHvals(tokens, db);
        if (cmd == "HLEN")          return handleHlen(tokens, db);
        if (cmd == "HMSET")         return handleHmset(tokens, db);
    } catch (const WrongTypeError& e) {
        return std::string("-") + e.what() + "\r\n";
    } catch (const std::exception& e) {
        return std::string("-ERR ") + e.what() + "\r\n";
    }
    return "-ERR unknown command '" + tokens[0] + "'\r\n";
}

std::string RedisCommandHandler::processCommand(const std::string& commandLine) {
    std::string out, error;
    std::vector<std::string> tokens;
    size_t pos = 0;
    while (pos < commandLine.size()) {
        auto status = parseCommand(commandLine, pos, tokens, error);
        if (status == ParseStatus::Complete) {
            if (!tokens.empty()) out += processCommand(tokens);
        } else if (status == ParseStatus::Error) {
            out += error;
            break;
        } else {
            out += "-ERR Protocol error: incomplete command\r\n";
            break;
        }
    }
    return out;
}

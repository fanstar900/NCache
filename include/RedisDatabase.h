#ifndef REDIS_DATABASE_H
#define REDIS_DATABASE_H

#include <string>
#include <mutex>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <stdexcept>
#include <sys/types.h>

// Thrown when a command is used against a key holding a different data type.
class WrongTypeError : public std::runtime_error {
public:
    WrongTypeError()
        : std::runtime_error("WRONGTYPE Operation against a key holding the wrong kind of value") {}
};

class RedisDatabase {
public:
    // Get the singleton instance
    static RedisDatabase& getInstance();

    // Common Comands
    bool flushAll();

    // Key/Value Operations
    void set(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string& value);
    std::vector<std::string> keys(const std::string& pattern = "*");
    std::string type(const std::string& key);
    bool del(const std::string& key);
    bool expire(const std::string& key, long long seconds);
    bool rename(const std::string& oldKey, const std::string& newKey);

    // List Opreations
    std::vector<std::string> lget(const std::string& key);
    ssize_t llen(const std::string& key);
    // Push all values atomically; returns the new list length.
    ssize_t lpush(const std::string& key, const std::vector<std::string>& values);
    ssize_t rpush(const std::string& key, const std::vector<std::string>& values);
    bool lpop(const std::string& key, std::string& value);
    bool rpop(const std::string& key, std::string& value);
    int lrem(const std::string& key, int count, const std::string& value);
    bool lindex(const std::string& key, int index, std::string& value);
    enum class LsetResult { Ok, NoSuchKey, OutOfRange };
    LsetResult lset(const std::string& key, int index, const std::string& value);

    // Hash Operations
    bool hset(const std::string& key, const std::string& field, const std::string& value);
    bool hget(const std::string& key, const std::string& field, std::string& value);
    bool hexists(const std::string& key, const std::string& field);
    // Removes the given fields; returns how many existed.
    int hdel(const std::string& key, const std::vector<std::string>& fields);
    std::unordered_map<std::string, std::string> hgetall(const std::string& key);
    std::vector<std::string> hkeys(const std::string& key);
    std::vector<std::string> hvals(const std::string& key);
    ssize_t hlen(const std::string& key);
    // Sets all pairs atomically; returns how many fields were newly created.
    int hmset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fieldValues);

    // Persistance: Dump / load the database from a file.
    bool dump(const std::string& filename);
    bool load(const std::string& filename);

private:
    RedisDatabase() = default;
    ~RedisDatabase() = default;
    RedisDatabase(const RedisDatabase&) = delete;
    RedisDatabase& operator=(const RedisDatabase&) = delete;

    // Helpers below expect db_mutex to be held by the caller.
    void purgeExpired();
    std::string typeLocked(const std::string& key) const;
    void checkType(const std::string& key, const char* expected) const;
    void eraseKeyLocked(const std::string& key);

    std::mutex db_mutex;
    std::unordered_map<std::string, std::string> kv_store;
    std::unordered_map<std::string, std::vector<std::string>> list_store;
    std::unordered_map<std::string, std::unordered_map<std::string, std::string>> hash_store;

    std::unordered_map<std::string, std::chrono::steady_clock::time_point> expiry_map;
};

#endif

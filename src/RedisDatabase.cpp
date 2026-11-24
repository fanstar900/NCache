#include "../include/RedisDatabase.h"

#include <fstream>
#include <sstream>
#include <algorithm>
#include <iterator>
#include <cstdio>
#include <fnmatch.h>

// Singleton accessor
RedisDatabase& RedisDatabase::getInstance() {
    static RedisDatabase instance;
    return instance;
}

//----------------------
// Internal helpers (db_mutex must be held)
//----------------------
std::string RedisDatabase::typeLocked(const std::string& key) const {
    if (kv_store.count(key)) return "string";
    if (list_store.count(key)) return "list";
    if (hash_store.count(key)) return "hash";
    return "none";
}

void RedisDatabase::checkType(const std::string& key, const char* expected) const {
    std::string t = typeLocked(key);
    if (t != "none" && t != expected)
        throw WrongTypeError();
}

void RedisDatabase::eraseKeyLocked(const std::string& key) {
    kv_store.erase(key);
    list_store.erase(key);
    hash_store.erase(key);
    expiry_map.erase(key);
}

void RedisDatabase::purgeExpired() {
    auto now = std::chrono::steady_clock::now();
    for (auto it = expiry_map.begin(); it != expiry_map.end(); ) {
        if (now >= it->second) {
            // Remove from all stores
            kv_store.erase(it->first);
            list_store.erase(it->first);
            hash_store.erase(it->first);
            it = expiry_map.erase(it);
        } else {
            ++it;
        }
    }
}

// Common Comands
bool RedisDatabase::flushAll() {
    std::lock_guard<std::mutex> lock(db_mutex);
    kv_store.clear();
    list_store.clear();
    hash_store.clear();
    expiry_map.clear();
    return true;
}

// Key/Value Operations
void RedisDatabase::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    // SET overwrites any existing value of any type and clears its TTL.
    eraseKeyLocked(key);
    kv_store[key] = value;
}

bool RedisDatabase::get(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "string");
    auto it = kv_store.find(key);
    if (it != kv_store.end()) {
        value = it->second;
        return true;
    }
    return false;
}

std::vector<std::string> RedisDatabase::keys(const std::string& pattern) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    std::vector<std::string> result;
    auto add = [&](const std::string& k) {
        if (fnmatch(pattern.c_str(), k.c_str(), 0) == 0)
            result.push_back(k);
    };
    for (const auto& pair : kv_store) add(pair.first);
    for (const auto& pair : list_store) add(pair.first);
    for (const auto& pair : hash_store) add(pair.first);
    return result;
}

std::string RedisDatabase::type(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    return typeLocked(key);
}

bool RedisDatabase::del(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    bool erased = false;
    erased |= kv_store.erase(key) > 0;
    erased |= list_store.erase(key) > 0;
    erased |= hash_store.erase(key) > 0;
    expiry_map.erase(key);
    return erased;
}

bool RedisDatabase::expire(const std::string& key, long long seconds) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    if (typeLocked(key) == "none")
        return false;

    if (seconds <= 0) {
        // A non-positive TTL deletes the key immediately (Redis behaviour).
        eraseKeyLocked(key);
        return true;
    }
    expiry_map[key] = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    return true;
}

bool RedisDatabase::rename(const std::string& oldKey, const std::string& newKey) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    std::string t = typeLocked(oldKey);
    if (t == "none")
        return false;
    if (oldKey == newKey)
        return true;

    // Whatever newKey held before (of any type, with any TTL) is overwritten.
    eraseKeyLocked(newKey);

    if (t == "string") {
        kv_store[newKey] = std::move(kv_store[oldKey]);
        kv_store.erase(oldKey);
    } else if (t == "list") {
        list_store[newKey] = std::move(list_store[oldKey]);
        list_store.erase(oldKey);
    } else {
        hash_store[newKey] = std::move(hash_store[oldKey]);
        hash_store.erase(oldKey);
    }

    auto itExpire = expiry_map.find(oldKey);
    if (itExpire != expiry_map.end()) {
        expiry_map[newKey] = itExpire->second;
        expiry_map.erase(itExpire);
    }
    return true;
}

// List Opreations
std::vector<std::string> RedisDatabase::lget(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    auto it = list_store.find(key);
    if (it != list_store.end())
        return it->second;
    return {};
}

ssize_t RedisDatabase::llen(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    auto it = list_store.find(key);
    if (it != list_store.end())
        return it->second.size();
    return 0;
}

ssize_t RedisDatabase::lpush(const std::string& key, const std::vector<std::string>& values) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    auto& lst = list_store[key];
    // LPUSH k a b c  ->  c b a ...
    lst.insert(lst.begin(), values.rbegin(), values.rend());
    return lst.size();
}

ssize_t RedisDatabase::rpush(const std::string& key, const std::vector<std::string>& values) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    auto& lst = list_store[key];
    lst.insert(lst.end(), values.begin(), values.end());
    return lst.size();
}

bool RedisDatabase::lpop(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    auto it = list_store.find(key);
    if (it == list_store.end() || it->second.empty())
        return false;
    value = it->second.front();
    it->second.erase(it->second.begin());
    if (it->second.empty())
        eraseKeyLocked(key);
    return true;
}

bool RedisDatabase::rpop(const std::string& key, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    auto it = list_store.find(key);
    if (it == list_store.end() || it->second.empty())
        return false;
    value = it->second.back();
    it->second.pop_back();
    if (it->second.empty())
        eraseKeyLocked(key);
    return true;
}

int RedisDatabase::lrem(const std::string& key, int count, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    int removed = 0;
    auto it = list_store.find(key);
    if (it == list_store.end())
        return 0;

    auto& lst = it->second;

    if (count == 0) {
        // Remove all occurances
        auto new_end = std::remove(lst.begin(), lst.end(), value);
        removed = std::distance(new_end, lst.end());
        lst.erase(new_end, lst.end());
    } else if (count > 0) {
        // Remove from head to tail
        for (auto iter = lst.begin(); iter != lst.end() && removed < count; ) {
            if (*iter == value) {
                iter = lst.erase(iter);
                ++removed;
            } else {
                ++iter;
            }
        }
    } else {
        // Remove from tail to head (count is negative)
        long long limit = -static_cast<long long>(count);
        for (auto riter = lst.rbegin(); riter != lst.rend() && removed < limit; ) {
            if (*riter == value) {
                auto fwdIter = std::prev(riter.base());
                fwdIter = lst.erase(fwdIter);
                ++removed;
                riter = std::reverse_iterator<std::vector<std::string>::iterator>(fwdIter);
            } else {
                ++riter;
            }
        }
    }
    if (lst.empty())
        eraseKeyLocked(key);
    return removed;
}

bool RedisDatabase::lindex(const std::string& key, int index, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    auto it = list_store.find(key);
    if (it == list_store.end())
        return false;

    const auto& lst = it->second;
    long long idx = index;
    if (idx < 0)
        idx += static_cast<long long>(lst.size());
    if (idx < 0 || idx >= static_cast<long long>(lst.size()))
        return false;

    value = lst[idx];
    return true;
}

RedisDatabase::LsetResult RedisDatabase::lset(const std::string& key, int index, const std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "list");
    auto it = list_store.find(key);
    if (it == list_store.end())
        return LsetResult::NoSuchKey;

    auto& lst = it->second;
    long long idx = index;
    if (idx < 0)
        idx += static_cast<long long>(lst.size());
    if (idx < 0 || idx >= static_cast<long long>(lst.size()))
        return LsetResult::OutOfRange;

    lst[idx] = value;
    return LsetResult::Ok;
}

// Hash Operations
bool RedisDatabase::hset(const std::string& key, const std::string& field, const std::string& value) {
    return hmset(key, {{field, value}}) == 1;
}

bool RedisDatabase::hget(const std::string& key, const std::string& field, std::string& value) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "hash");
    auto it = hash_store.find(key);
    if (it != hash_store.end()) {
        auto f = it->second.find(field);
        if (f != it->second.end()) {
            value = f->second;
            return true;
        }
    }
    return false;
}

bool RedisDatabase::hexists(const std::string& key, const std::string& field) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "hash");
    auto it = hash_store.find(key);
    if (it != hash_store.end())
        return it->second.find(field) != it->second.end();
    return false;
}

int RedisDatabase::hdel(const std::string& key, const std::vector<std::string>& fields) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "hash");
    auto it = hash_store.find(key);
    if (it == hash_store.end())
        return 0;
    int removed = 0;
    for (const auto& f : fields)
        removed += it->second.erase(f);
    if (it->second.empty())
        eraseKeyLocked(key);
    return removed;
}

std::unordered_map<std::string, std::string> RedisDatabase::hgetall(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "hash");
    auto it = hash_store.find(key);
    if (it != hash_store.end())
        return it->second;
    return {};
}

std::vector<std::string> RedisDatabase::hkeys(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "hash");
    std::vector<std::string> fields;
    auto it = hash_store.find(key);
    if (it != hash_store.end()) {
        for (const auto& pair: it->second)
            fields.push_back(pair.first);
    }
    return fields;
}

std::vector<std::string> RedisDatabase::hvals(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "hash");
    std::vector<std::string> values;
    auto it = hash_store.find(key);
    if (it != hash_store.end()) {
        for (const auto& pair: it->second)
            values.push_back(pair.second);
    }
    return values;
}

ssize_t RedisDatabase::hlen(const std::string& key) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "hash");
    auto it = hash_store.find(key);
    return (it != hash_store.end()) ? it->second.size() : 0;
}

int RedisDatabase::hmset(const std::string& key, const std::vector<std::pair<std::string, std::string>>& fieldValues) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();
    checkType(key, "hash");
    auto& hash = hash_store[key];
    int created = 0;
    for (const auto& pair: fieldValues) {
        auto res = hash.insert_or_assign(pair.first, pair.second);
        if (res.second) ++created;
    }
    return created;
}

/*
Binary-safe text persistance. Every string is written as <length>:<bytes>,
so keys and values may contain spaces, colons or newlines.

Memory -> File - dump()
File -> Memory - load()

Header: MYRDB1
K <key> <value>                         = Key Value
L <key> <count> <item>...               = List
H <key> <count> <field> <value>...      = Hash
E <key> <unix-epoch-ms>                 = Expiry (absolute wall-clock time)
*/
static const char* DUMP_HEADER = "MYRDB1";

static void writeStr(std::ostream& os, const std::string& s) {
    os << s.size() << ':' << s;
}

static bool readStr(std::istream& is, std::string& out) {
    size_t len;
    if (!(is >> len)) return false;
    if (is.get() != ':') return false;
    out.resize(len);
    if (len > 0 && !is.read(&out[0], len)) return false;
    return true;
}

bool RedisDatabase::dump(const std::string& filename) {
    std::lock_guard<std::mutex> lock(db_mutex);
    purgeExpired();

    // Write to a temp file and rename, so a crash mid-dump never corrupts the old dump.
    std::string tmp = filename + ".tmp";
    {
        std::ofstream ofs(tmp, std::ios::binary | std::ios::trunc);
        if (!ofs) return false;

        ofs << DUMP_HEADER << "\n";
        for (const auto& kv: kv_store) {
            ofs << "K ";
            writeStr(ofs, kv.first); ofs << ' ';
            writeStr(ofs, kv.second); ofs << "\n";
        }
        for (const auto& kv : list_store) {
            ofs << "L ";
            writeStr(ofs, kv.first);
            ofs << ' ' << kv.second.size();
            for (const auto& item : kv.second) {
                ofs << ' ';
                writeStr(ofs, item);
            }
            ofs << "\n";
        }
        for (const auto& kv : hash_store) {
            ofs << "H ";
            writeStr(ofs, kv.first);
            ofs << ' ' << kv.second.size();
            for (const auto& field_val : kv.second) {
                ofs << ' ';
                writeStr(ofs, field_val.first); ofs << ' ';
                writeStr(ofs, field_val.second);
            }
            ofs << "\n";
        }
        // steady_clock is meaningless across restarts, so store absolute wall-clock time.
        auto steadyNow = std::chrono::steady_clock::now();
        auto sysNow = std::chrono::system_clock::now();
        for (const auto& e : expiry_map) {
            auto at = sysNow + std::chrono::duration_cast<std::chrono::system_clock::duration>(e.second - steadyNow);
            long long ms = std::chrono::duration_cast<std::chrono::milliseconds>(at.time_since_epoch()).count();
            ofs << "E ";
            writeStr(ofs, e.first);
            ofs << ' ' << ms << "\n";
        }
        ofs.flush();
        if (!ofs) return false;
    }
    return std::rename(tmp.c_str(), filename.c_str()) == 0;
}

bool RedisDatabase::load(const std::string& filename) {
    std::ifstream ifs(filename, std::ios::binary);
    if (!ifs) return false;

    std::string header;
    if (!std::getline(ifs, header) || header != DUMP_HEADER)
        return false;

    // Parse into temporaries so a corrupt file leaves the database untouched.
    decltype(kv_store) kv;
    decltype(list_store) lists;
    decltype(hash_store) hashes;
    decltype(expiry_map) expiries;

    auto steadyNow = std::chrono::steady_clock::now();
    auto sysNow = std::chrono::system_clock::now();

    char type;
    while (ifs >> type) {
        std::string key;
        if (!readStr(ifs, key)) return false;
        if (type == 'K') {
            std::string value;
            if (!readStr(ifs, value)) return false;
            kv[key] = std::move(value);
        } else if (type == 'L') {
            size_t n;
            if (!(ifs >> n)) return false;
            std::vector<std::string> list;
            list.reserve(std::min<size_t>(n, 1 << 20));
            for (size_t i = 0; i < n; ++i) {
                std::string item;
                if (!readStr(ifs, item)) return false;
                list.push_back(std::move(item));
            }
            lists[key] = std::move(list);
        } else if (type == 'H') {
            size_t n;
            if (!(ifs >> n)) return false;
            std::unordered_map<std::string, std::string> hash;
            for (size_t i = 0; i < n; ++i) {
                std::string field, value;
                if (!readStr(ifs, field) || !readStr(ifs, value)) return false;
                hash[std::move(field)] = std::move(value);
            }
            hashes[key] = std::move(hash);
        } else if (type == 'E') {
            long long ms;
            if (!(ifs >> ms)) return false;
            std::chrono::system_clock::time_point at{std::chrono::milliseconds(ms)};
            expiries[key] = steadyNow + std::chrono::duration_cast<std::chrono::steady_clock::duration>(at - sysNow);
        } else {
            return false;
        }
    }

    std::lock_guard<std::mutex> lock(db_mutex);
    kv_store = std::move(kv);
    list_store = std::move(lists);
    hash_store = std::move(hashes);
    expiry_map = std::move(expiries);
    purgeExpired();
    return true;
}

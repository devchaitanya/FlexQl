#pragma once
#include <list>
#include <unordered_map>
#include <unordered_set>
#include <string>
#include <mutex>
#include <optional>

// Thread-safe LRU cache with per-table invalidation support.
// K must be std::string (or any hashable type); V is the cached value.
template <typename K, typename V>
class LRUCache {
public:
    explicit LRUCache(size_t capacity) : capacity_(capacity) {}

    // Returns nullptr if not cached.
    const V* get(const K& key) {
        std::lock_guard lock(mtx_);
        auto it = map_.find(key);
        if (it == map_.end()) return nullptr;
        // Move to front (most recently used)
        order_.splice(order_.begin(), order_, it->second.list_it);
        return &it->second.value;
    }

    // Insert/update entry; tag with table name for invalidation.
    void put(const K& key, const V& value, const std::string& table_tag = "") {
        std::lock_guard lock(mtx_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            // Update in place, move to front
            it->second.value   = value;
            it->second.tag     = table_tag;
            order_.splice(order_.begin(), order_, it->second.list_it);
            return;
        }
        // Evict LRU if at capacity
        if (map_.size() >= capacity_) {
            const K& lru_key = order_.back();
            map_.erase(lru_key);
            order_.pop_back();
        }
        order_.push_front(key);
        map_[key] = {value, table_tag, order_.begin()};
    }

    // Evict all entries whose tag starts with `prefix` (table name).
    void invalidate_prefix(const std::string& prefix) {
        std::lock_guard lock(mtx_);
        std::vector<K> to_erase;
        for (auto& [k, e] : map_) {
            if (e.tag == prefix || k.rfind(prefix + "|", 0) == 0)
                to_erase.push_back(k);
        }
        for (const auto& k : to_erase) {
            auto it = map_.find(k);
            if (it != map_.end()) {
                order_.erase(it->second.list_it);
                map_.erase(it);
            }
        }
    }

    void clear() {
        std::lock_guard lock(mtx_);
        map_.clear();
        order_.clear();
    }

    size_t size() const {
        std::lock_guard lock(mtx_);
        return map_.size();
    }

private:
    struct Entry {
        V                              value;
        std::string                    tag;
        typename std::list<K>::iterator list_it;
    };

    size_t              capacity_;
    std::list<K>        order_;
    std::unordered_map<K, Entry> map_;
    mutable std::mutex  mtx_;
};

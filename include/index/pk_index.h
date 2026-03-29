#pragma once
#include <string_view>
#include <vector>
#include <cstddef>
#include <cstdint>
#include <string>

// ── PrimaryIndex ──────────────────────────────────────────────────────────────
// Open-addressing flat hash map: string_view key → row index.
// Keys are string_views into the Table's StringArena (stable pointers).
// Uses FNV-64 hash + linear probing, max load factor 0.6.
// Not thread-safe; callers must hold the table mutex.
class PrimaryIndex {
    struct Slot {
        uint64_t         hash      = 0;
        std::string_view key;
        size_t           row_idx   = 0;
        bool             occupied  = false;
        bool             tombstone = false;
    };

    std::vector<Slot> slots_;
    size_t            count_    = 0;   // occupied, non-tombstone entries
    size_t            capacity_ = 0;   // slots_.size() (always power of 2)

    static constexpr double MAX_LOAD = 0.6;

    static uint64_t fnv64(std::string_view s) noexcept {
        uint64_t h = 14695981039346656037ULL;
        for (unsigned char c : s)
            h = (h ^ static_cast<uint64_t>(c)) * 1099511628211ULL;
        return h;
    }

    void grow() {
        size_t new_cap = (capacity_ == 0) ? 16 : capacity_ * 2;
        std::vector<Slot> new_slots(new_cap);
        size_t mask = new_cap - 1;
        for (const auto& s : slots_) {
            if (!s.occupied || s.tombstone) continue;
            size_t pos = s.hash & mask;
            while (new_slots[pos].occupied) pos = (pos + 1) & mask;
            new_slots[pos] = s;
        }
        slots_    = std::move(new_slots);
        capacity_ = new_cap;
    }

public:
    // Returns true if key was inserted (false = duplicate).
    bool insert(std::string_view key, size_t row_idx) {
        if (capacity_ == 0 || (double)(count_ + 1) / (double)capacity_ > MAX_LOAD)
            grow();

        uint64_t h    = fnv64(key);
        size_t   mask = capacity_ - 1;
        size_t   pos  = h & mask;

        while (slots_[pos].occupied && !slots_[pos].tombstone) {
            if (slots_[pos].hash == h && slots_[pos].key == key)
                return false; // duplicate
            pos = (pos + 1) & mask;
        }
        slots_[pos] = {h, key, row_idx, true, false};
        ++count_;
        return true;
    }

    // Returns row index, or -1 if not found.
    long long lookup(std::string_view key) const {
        if (capacity_ == 0) return -1LL;
        uint64_t h    = fnv64(key);
        size_t   mask = capacity_ - 1;
        size_t   pos  = h & mask;

        while (slots_[pos].occupied) {
            if (!slots_[pos].tombstone && slots_[pos].hash == h && slots_[pos].key == key)
                return static_cast<long long>(slots_[pos].row_idx);
            pos = (pos + 1) & mask;
        }
        return -1LL;
    }

    void remove(std::string_view key) {
        if (capacity_ == 0) return;
        uint64_t h    = fnv64(key);
        size_t   mask = capacity_ - 1;
        size_t   pos  = h & mask;

        while (slots_[pos].occupied) {
            if (!slots_[pos].tombstone && slots_[pos].hash == h && slots_[pos].key == key) {
                slots_[pos].tombstone = true;
                --count_;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }

    // Update row_idx after compaction (key stays in arena, same pointer).
    void update(std::string_view key, size_t new_idx) {
        if (capacity_ == 0) return;
        uint64_t h    = fnv64(key);
        size_t   mask = capacity_ - 1;
        size_t   pos  = h & mask;

        while (slots_[pos].occupied) {
            if (!slots_[pos].tombstone && slots_[pos].hash == h && slots_[pos].key == key) {
                slots_[pos].row_idx = new_idx;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }

    void   clear()  { slots_.clear(); count_ = 0; capacity_ = 0; }
    size_t size()   const { return count_; }
};

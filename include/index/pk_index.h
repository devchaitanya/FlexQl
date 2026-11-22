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
    size_t            count_      = 0; // occupied, non-tombstone entries
    size_t            tombstones_ = 0; // occupied, tombstone entries
    size_t            capacity_   = 0; // slots_.size() (always power of 2)

    static constexpr double MAX_LOAD    = 0.6;
    static constexpr double GROW_TARGET = 0.5;   // live-key load right after grow()

    static uint64_t fnv64(std::string_view s) noexcept {
        uint64_t h = 14695981039346656037ULL;
        for (unsigned char c : s)
            h = (h ^ static_cast<uint64_t>(c)) * 1099511628211ULL;
        return h;
    }

    // Rebuild into a table sized so the live keys fill at most 50% (when live
    // keys drove the growth — 60% full — that is exactly a doubling; when it
    // was mostly tombstones, the same size or smaller). Uses the stored
    // hashes — no rehashing of key bytes — and drops every tombstone.
    void grow() {
        size_t new_cap = 16;
        while ((double)(count_ + 1) / (double)new_cap > GROW_TARGET) new_cap <<= 1;
        std::vector<Slot> new_slots(new_cap);
        size_t mask = new_cap - 1;
        for (const auto& s : slots_) {
            if (!s.occupied || s.tombstone) continue;
            size_t pos = s.hash & mask;
            while (new_slots[pos].occupied) pos = (pos + 1) & mask;
            new_slots[pos] = s;
        }
        slots_      = std::move(new_slots);
        capacity_   = new_cap;
        tombstones_ = 0;
    }

public:
    // Returns true if key was inserted (false = duplicate).
    bool insert(std::string_view key, size_t row_idx) {
        // Tombstones occupy slots too: the probe loop below stops only at an
        // EMPTY slot, so count them, or a table full of tombstones would make
        // insert() loop forever.
        if (capacity_ == 0 ||
            (double)(count_ + tombstones_ + 1) / (double)capacity_ > MAX_LOAD)
            grow();

        uint64_t h    = fnv64(key);
        size_t   mask = capacity_ - 1;
        size_t   pos  = h & mask;
        size_t   first_tombstone = SIZE_MAX;

        // Scan past ALL occupied slots (including tombstones) to detect duplicates.
        // Record first tombstone position for reuse.
        while (slots_[pos].occupied) {
            if (slots_[pos].tombstone) {
                if (first_tombstone == SIZE_MAX)
                    first_tombstone = pos;
            } else if (slots_[pos].hash == h && slots_[pos].key == key) {
                return false; // duplicate
            }
            pos = (pos + 1) & mask;
        }
        size_t insert_pos = pos;
        if (first_tombstone != SIZE_MAX) { insert_pos = first_tombstone; --tombstones_; }
        slots_[insert_pos] = {h, key, row_idx, true, false};
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
                ++tombstones_;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }

    void   clear()  { slots_.clear(); count_ = 0; tombstones_ = 0; capacity_ = 0; }

    // Drop all entries and pre-size for n keys in one allocation, so a bulk
    // re-insert (compaction) does not go through ~log2(n) grow()+rehash steps.
    void reset(size_t n) {
        size_t cap = 16;
        while ((double)(n + 1) / (double)cap > MAX_LOAD) cap <<= 1;
        slots_.assign(cap, Slot{});
        capacity_   = cap;
        count_      = 0;
        tombstones_ = 0;
    }

    // After compaction moved rows: old row r now lives at remap[r], or was
    // dropped (DROPPED → its key is tombstoned; e.g. an expired row).
    // Keys still point at the same arena bytes (tier 1 never moves strings),
    // so only row numbers change: ONE sequential pass over the slots — no key
    // hashing, no probing, no random inserts.
    static constexpr uint32_t DROPPED = UINT32_MAX;
    void remap_rows(const std::vector<uint32_t>& remap) {
        for (Slot& s : slots_) {
            if (!s.occupied || s.tombstone) continue;
            const uint32_t n = s.row_idx < remap.size() ? remap[s.row_idx] : DROPPED;
            if (n == DROPPED) { s.tombstone = true; --count_; ++tombstones_; }
            else              s.row_idx = n;
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

    // Rebuild when tombstones outnumber live keys. Inserts trigger grow() only
    // when the table gets too full, so a delete-heavy table with few inserts
    // would otherwise keep its tombstones (longer probe chains) and its large
    // slot array forever. grow() drops every tombstone and sizes the new array
    // for the LIVE keys — so this also shrinks the index. Called by the
    // background sweep (tier 1). Returns true if it rebuilt.
    bool compact_if_sparse(size_t min_tombstones = 1024) {
        if (tombstones_ < min_tombstones || tombstones_ <= count_) return false;
        grow();
        return true;
    }

    size_t size()       const { return count_; }
    size_t tombstones() const { return tombstones_; }
    size_t capacity()   const { return capacity_; }
    size_t bytes()      const { return capacity_ * sizeof(Slot); }
};

#pragma once
#include <memory>
#include <string_view>
#include <vector>
#include <cstring>

// ── StringArena ───────────────────────────────────────────────────────────────
// Bump-pointer slab allocator for string data.
// - Allocates 512KB blocks on demand; never moves existing blocks.
// - intern() returns a string_view that is valid for the lifetime of the arena.
// - Not thread-safe by itself; callers must hold the table mutex.
class StringArena {
    static constexpr size_t BLOCK = 512 * 1024; // 512 KB per slab

    std::vector<std::unique_ptr<char[]>> blocks_;
    char*  current_ = nullptr; // cached pointer to start of current slab
    size_t used_    = BLOCK;   // start past end so first intern forces an alloc

    [[gnu::noinline]] void new_slab() {
        blocks_.push_back(std::make_unique<char[]>(BLOCK));
        current_ = blocks_.back().get();
        used_    = 0;
    }

public:
    [[gnu::always_inline]] std::string_view intern(std::string_view sv) {
        if (__builtin_expect(sv.empty(), 0)) return {};
        if (__builtin_expect(used_ + sv.size() > BLOCK, 0)) new_slab();
        char* p = current_ + used_;
        std::memcpy(p, sv.data(), sv.size());
        used_ += sv.size();
        return {p, sv.size()};
    }

    void clear() {
        blocks_.clear();
        current_ = nullptr;
        used_    = BLOCK;
    }
};

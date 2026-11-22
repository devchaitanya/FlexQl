#pragma once
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <new>
#include <string_view>
#include <utility>
#include <vector>
#include <sys/mman.h>

// ── StringArena ───────────────────────────────────────────────────────────────
// Bump-pointer slab allocator for string data.
// - Allocates 4 MB slabs on demand; never moves existing data.
// - intern() returns a string_view that is valid for the lifetime of the arena
//   (or until release() is called for it).
// - Not thread-safe by itself; callers must hold the table mutex — except
//   add_released(), which is atomic (see Table::release_dead_pages).
//
// Memory reclamation support (page reference counting):
// - Slabs are mmap()ed, so they are page-aligned: slab page p is exactly the
//   OS page at base + p*4096, and munmap() really returns memory to the OS.
// - Every slab keeps the number of LIVE bytes on each 4 KB page. intern() adds,
//   release() subtracts. When a page that lies wholly behind the bump pointer
//   drops to 0 live bytes, no string_view can point into it any more and no
//   new string will ever be written there (the bump pointer only moves
//   forward), so it is queued for madvise(MADV_DONTNEED) — returning its RAM
//   to the OS without moving any data.
class StringArena {
public:
    static constexpr size_t BLOCK      = 4 * 1024 * 1024; // 4 MB per slab
    static constexpr size_t PAGE_SHIFT = 12;
    static constexpr size_t PAGE       = size_t(1) << PAGE_SHIFT; // 4 KB

private:
    struct Slab {
        char*                 base  = nullptr;   // mmap()ed, page-aligned
        size_t                size  = 0;         // multiple of PAGE
        std::vector<uint32_t> live;              // live bytes per page
        std::vector<char>     queued;            // page already queued for release

        Slab() = default;
        Slab(char* b, size_t n) : base(b), size(n), live(n >> PAGE_SHIFT, 0),
                                  queued(n >> PAGE_SHIFT, 0) {}
        Slab(Slab&& o) noexcept
            : base(std::exchange(o.base, nullptr)), size(std::exchange(o.size, 0)),
              live(std::move(o.live)), queued(std::move(o.queued)) {}
        Slab& operator=(Slab&& o) noexcept {
            if (this != &o) {
                unmap();
                base = std::exchange(o.base, nullptr);
                size = std::exchange(o.size, 0);
                live = std::move(o.live);
                queued = std::move(o.queued);
            }
            return *this;
        }
        Slab(const Slab&) = delete;
        Slab& operator=(const Slab&) = delete;
        ~Slab() { unmap(); }
        void unmap() { if (base) ::munmap(base, size); base = nullptr; }
    };

    std::vector<Slab>    slabs_;
    // (base address, slab index) sorted by address — maps a pointer to its slab.
    std::vector<std::pair<uintptr_t, uint32_t>> by_addr_;
    mutable size_t       last_slab_ = 0;          // lookup cache (locality)

    char*     current_  = nullptr;   // start of current slab
    uint32_t* cur_live_ = nullptr;   // current slab's page counters
    size_t    cur_slab_ = SIZE_MAX;  // index of current slab
    size_t    used_     = BLOCK;     // start past end so first intern forces an alloc

    size_t    live_bytes_     = 0;   // bytes referenced by live string_views
    size_t    reserved_bytes_ = 0;   // bytes mapped (all slabs)
    std::atomic<size_t> released_bytes_{0};   // bytes given back via madvise

    std::vector<char*> dead_pages_;  // page addresses ready for madvise

    static char* map_bytes(size_t n) {
        void* p = ::mmap(nullptr, n, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (p == MAP_FAILED) throw std::bad_alloc();
        return static_cast<char*>(p);
    }

    size_t add_slab(size_t n) {
        char* p = map_bytes(n);
        slabs_.emplace_back(p, n);
        reserved_bytes_ += n;
        const uint32_t idx = (uint32_t)(slabs_.size() - 1);
        auto key = std::make_pair((uintptr_t)p, idx);
        by_addr_.insert(std::upper_bound(by_addr_.begin(), by_addr_.end(), key), key);
        return idx;
    }

    [[gnu::noinline]] void new_slab() {
        // The old current slab is now closed: every page in it lies behind the
        // (final) bump pointer, so pages that already reached 0 live bytes —
        // or were never used (the unused tail) — can be released now.
        if (cur_slab_ != SIZE_MAX) {
            Slab& s = slabs_[cur_slab_];
            for (size_t p = 0; p < s.live.size(); ++p)
                if (s.live[p] == 0 && !s.queued[p]) queue_page(s, p);
        }
        cur_slab_ = add_slab(BLOCK);
        current_  = slabs_[cur_slab_].base;
        cur_live_ = slabs_[cur_slab_].live.data();
        used_     = 0;
    }

    static void add_live(uint32_t* live, size_t off, size_t n) {
        const size_t first = off >> PAGE_SHIFT;
        const size_t last  = (off + n - 1) >> PAGE_SHIFT;
        if (__builtin_expect(first == last, 1)) { live[first] += (uint32_t)n; return; }
        for (size_t p = first; p <= last; ++p) {
            const size_t lo = std::max(off, p << PAGE_SHIFT);
            const size_t hi = std::min(off + n, (p + 1) << PAGE_SHIFT);
            live[p] += (uint32_t)(hi - lo);
        }
    }

    void queue_page(Slab& s, size_t p) {
        s.queued[p] = 1;
        dead_pages_.push_back(s.base + (p << PAGE_SHIFT));
    }

    size_t find_slab(const char* ptr) const {
        const uintptr_t a = (uintptr_t)ptr;
        const Slab& c = slabs_[last_slab_];
        if (a >= (uintptr_t)c.base && a < (uintptr_t)c.base + c.size) return last_slab_;
        auto it = std::upper_bound(by_addr_.begin(), by_addr_.end(),
                                   std::make_pair(a, UINT32_MAX));
        --it;                                     // last slab with base <= a
        last_slab_ = it->second;
        return last_slab_;
    }

public:
    StringArena() = default;
    StringArena(const StringArena&) = delete;
    StringArena& operator=(const StringArena&) = delete;
    StringArena(StringArena&& o) noexcept { *this = std::move(o); }
    StringArena& operator=(StringArena&& o) noexcept {
        if (this != &o) {
            slabs_          = std::move(o.slabs_);
            by_addr_        = std::move(o.by_addr_);
            dead_pages_     = std::move(o.dead_pages_);
            last_slab_      = std::exchange(o.last_slab_, 0);
            current_        = std::exchange(o.current_, nullptr);
            cur_live_       = std::exchange(o.cur_live_, nullptr);
            cur_slab_       = std::exchange(o.cur_slab_, SIZE_MAX);
            used_           = std::exchange(o.used_, BLOCK);
            live_bytes_     = std::exchange(o.live_bytes_, 0);
            reserved_bytes_ = std::exchange(o.reserved_bytes_, 0);
            released_bytes_.store(o.released_bytes_.exchange(0));
            o.slabs_.clear(); o.by_addr_.clear(); o.dead_pages_.clear();
        }
        return *this;
    }

    [[gnu::always_inline]] std::string_view intern(std::string_view sv) {
        if (__builtin_expect(sv.empty(), 0)) return {};
        const size_t n = sv.size();
        if (__builtin_expect(n > BLOCK, 0)) {
            // Oversized string: a dedicated mapping (never the bump slab).
            const size_t sz = (n + PAGE - 1) & ~(PAGE - 1);
            Slab& s = slabs_[add_slab(sz)];
            std::memcpy(s.base, sv.data(), n);
            add_live(s.live.data(), 0, n);
            live_bytes_ += n;
            return {s.base, n};
        }
        if (__builtin_expect(used_ + n > BLOCK, 0)) new_slab();
        char* p = current_ + used_;
        std::memcpy(p, sv.data(), n);
        add_live(cur_live_, used_, n);
        used_       += n;
        live_bytes_ += n;
        return {p, n};
    }

    // The string_view sv (returned by intern) is no longer referenced.
    // Its bytes become dead; pages that drop to 0 live bytes are queued.
    void release(std::string_view sv) {
        if (sv.empty() || slabs_.empty()) return;
        const size_t si  = find_slab(sv.data());
        Slab&        s   = slabs_[si];
        const size_t off = (size_t)(sv.data() - s.base);
        const size_t n   = sv.size();
        const size_t first = off >> PAGE_SHIFT;
        const size_t last  = (off + n - 1) >> PAGE_SHIFT;
        for (size_t p = first; p <= last; ++p) {
            const size_t lo = std::max(off, p << PAGE_SHIFT);
            const size_t hi = std::min(off + n, (p + 1) << PAGE_SHIFT);
            s.live[p] -= (uint32_t)(hi - lo);
            if (s.live[p] != 0 || s.queued[p]) continue;
            // In the current slab only pages wholly behind the bump pointer are
            // final; the page holding the bump pointer may still get new bytes.
            if (si == cur_slab_ && ((p + 1) << PAGE_SHIFT) > used_) continue;
            queue_page(s, p);
        }
        live_bytes_ -= n;
    }

    // Hand over the queued page addresses (O(1); caller holds the table lock).
    std::vector<char*> take_dead_pages() { return std::exchange(dead_pages_, {}); }

    // Release page-aligned ranges back to the OS. Needs NO table lock: nothing
    // references these pages and nothing will be written there again. Returns
    // the number of bytes released. Input is consumed (sorted in place).
    static size_t madvise_pages(std::vector<char*>& pages) {
        if (pages.empty()) return 0;
        std::sort(pages.begin(), pages.end());
        size_t total = 0;
        size_t i = 0;
        while (i < pages.size()) {
            char*  start = pages[i];
            size_t len   = PAGE;
            while (i + 1 < pages.size() && pages[i + 1] == start + len) { len += PAGE; ++i; }
            ::madvise(start, len, MADV_DONTNEED);   // one syscall per contiguous run
            total += len;
            ++i;
        }
        return total;
    }
    void add_released(size_t n) { released_bytes_.fetch_add(n, std::memory_order_relaxed); }

    void clear() { *this = StringArena(); }

    // ── Accounting ────────────────────────────────────────────────────────────
    size_t live_bytes()     const { return live_bytes_; }
    size_t reserved_bytes() const { return reserved_bytes_; }
    size_t released_bytes() const { return released_bytes_.load(std::memory_order_relaxed); }
    // Bytes in use or wasted (approx. RAM): mapped, minus pages given back,
    // minus the not-yet-used tail of the current slab (never touched).
    size_t resident_bytes() const {
        size_t r = released_bytes();
        if (cur_slab_ != SIZE_MAX) r += BLOCK - used_;
        return reserved_bytes_ > r ? reserved_bytes_ - r : 0;
    }
};

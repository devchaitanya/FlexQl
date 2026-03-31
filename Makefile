CXX      := g++
CXXFLAGS := -std=c++17 -O3 -march=native -funroll-loops -flto=auto \
            -Wall -Wextra -pthread \
            -Iinclude -Isrc/client \
            -MMD -MP

BUILD    := build
BIN      := bin

# ── Source files ──────────────────────────────────────────────────────────────
COMMON_SRCS := \
    src/utils/string_utils.cpp \
    src/storage/table.cpp \
    src/storage/database.cpp \
    src/storage/wal.cpp \
    src/storage/snapshot.cpp \
    src/parser/token.cpp \
    src/parser/parser.cpp \
    src/query/executor.cpp \
    src/cache/lru_cache.cpp \
    src/expiration/ttl_manager.cpp \
    src/concurrency/thread_pool.cpp \
    src/network/protocol.cpp \
    src/network/server.cpp

SERVER_SRCS := $(COMMON_SRCS) src/server/main.cpp
CLIENT_SRCS := src/client/flexql.cpp src/client/client.cpp src/network/protocol.cpp

# ── Object files ──────────────────────────────────────────────────────────────
SERVER_OBJS := $(patsubst src/%.cpp, $(BUILD)/%.o, $(SERVER_SRCS))
CLIENT_OBJS := $(patsubst src/%.cpp, $(BUILD)/%.o, $(CLIENT_SRCS))

# ── Targets ───────────────────────────────────────────────────────────────────
.PHONY: all server client clean compile_commands test test-asan test-tsan

all: server client

server: $(BIN)/flexql-server

client: $(BIN)/flexql-client

$(BIN)/flexql-server: $(SERVER_OBJS) | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(BIN)/flexql-client: $(CLIENT_OBJS) | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ $^

# Compile any src/**.cpp → build/**.o, preserving subdirectory structure
$(BUILD)/%.o: src/%.cpp | $(BUILD)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD):
	mkdir -p $(BUILD)

$(BIN):
	mkdir -p $(BIN)

# Include generated dependency files (header tracking)
-include $(SERVER_OBJS:.o=.d) $(CLIENT_OBJS:.o=.d)

clean:
	rm -rf $(BUILD) $(BIN)

# ── Test suite ────────────────────────────────────────────────────────────────
# Core engine sources (no server/network/client) for standalone tests
TEST_ENGINE_SRCS := \
    src/utils/string_utils.cpp \
    src/storage/table.cpp \
    src/storage/database.cpp \
    src/storage/wal.cpp \
    src/storage/snapshot.cpp \
    src/parser/token.cpp \
    src/parser/parser.cpp \
    src/query/executor.cpp \
    src/cache/lru_cache.cpp \
    src/expiration/ttl_manager.cpp \
    src/concurrency/thread_pool.cpp

TEST_SRCS := tests/test_flexql.cpp $(TEST_ENGINE_SRCS)
TEST_OBJS := $(patsubst src/%.cpp, $(BUILD)/test/%.o, $(TEST_ENGINE_SRCS)) $(BUILD)/test/test_flexql.o

test: $(BIN)/flexql-test
	$(BIN)/flexql-test

$(BIN)/flexql-test: $(TEST_OBJS) | $(BIN)
	$(CXX) $(CXXFLAGS) -o $@ $^

$(BUILD)/test/%.o: src/%.cpp | $(BUILD)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BUILD)/test/test_flexql.o: tests/test_flexql.cpp | $(BUILD)
	@mkdir -p $(dir $@)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# ── Sanitizer builds ─────────────────────────────────────────────────────────
# AddressSanitizer: detects heap/stack buffer overflows, use-after-free, memory leaks
test-asan:
	$(CXX) -std=c++17 -g -O1 -fsanitize=address -fno-omit-frame-pointer \
	    -Wall -Wextra -pthread -Iinclude -Isrc/client \
	    $(TEST_SRCS) -o $(BIN)/flexql-test-asan
	ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 $(BIN)/flexql-test-asan

# ThreadSanitizer: detects data races between threads
test-tsan:
	$(CXX) -std=c++17 -g -O1 -fsanitize=thread \
	    -Wall -Wextra -pthread -Iinclude -Isrc/client \
	    $(TEST_SRCS) -o $(BIN)/flexql-test-tsan
	TSAN_OPTIONS=abort_on_error=1 $(BIN)/flexql-test-tsan

# Regenerate compile_commands.json for clangd IDE support
compile_commands:
	$(CXX) $(CXXFLAGS) -MJ /dev/stdout -fsyntax-only $(SERVER_SRCS) $(CLIENT_SRCS) 2>/dev/null | \
	    sed '1s/^/[/' | sed '$$s/,$$/]/' > compile_commands.json || \
	python3 -c "\
import json, subprocess, os; \
srcs = '$(SERVER_SRCS) $(CLIENT_SRCS)'.split(); \
srcs = list(dict.fromkeys(srcs)); \
wd = os.getcwd(); \
flags = '$(CXX) $(CXXFLAGS)'; \
db = [{'directory': wd, 'file': wd+'/'+s, 'command': flags+' -c '+wd+'/'+s} for s in srcs]; \
open('compile_commands.json','w').write(json.dumps(db, indent=2)); \
print(f'Written {len(db)} entries')"

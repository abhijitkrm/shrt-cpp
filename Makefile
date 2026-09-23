CXX      ?= clang++
ROCKSDB_PREFIX ?= /opt/homebrew/opt/rocksdb
CXXFLAGS ?= -std=c++20 -O3 -march=native -flto -Wall -Wextra -Wno-unused-parameter -I$(ROCKSDB_PREFIX)/include
LDFLAGS  ?= -flto -pthread -L$(ROCKSDB_PREFIX)/lib -lrocksdb

SRC := $(wildcard src/*.cpp)
OBJ := $(patsubst src/%.cpp,build/%.o,$(SRC))

BIN_SHRT  := shrt
BIN_BENCH := shrt-bench
BIN_TEST  := shrt-test

LIB_OBJ := $(filter-out build/main.o build/bench.o build/api_test.o build/store_test.o build/kv_test.o,$(OBJ))

all: $(BIN_SHRT) $(BIN_BENCH) $(BIN_TEST)

build/%.o: src/%.cpp src/%.hpp src/common.hpp | build
	$(CXX) $(CXXFLAGS) -c $< -o $@

build/%.o: src/%.cpp src/common.hpp | build
	$(CXX) $(CXXFLAGS) -c $< -o $@

build/%.o: tests/%.cpp src/*.hpp src/common.hpp | build
	$(CXX) $(CXXFLAGS) -c $< -o $@

$(BIN_SHRT): $(LIB_OBJ) build/main.o
	$(CXX) $^ -o $@ $(LDFLAGS)

$(BIN_BENCH): $(LIB_OBJ) build/bench.o
	$(CXX) $^ -o $@ $(LDFLAGS)

$(BIN_TEST): $(LIB_OBJ) build/store_test.o build/api_test.o build/kv_test.o
	$(CXX) $^ -o $@ $(LDFLAGS)

build:
	mkdir -p build

test: $(BIN_TEST)
	./$(BIN_TEST)

bench: $(BIN_BENCH)
	./$(BIN_BENCH)

clean:
	rm -rf build $(BIN_SHRT) $(BIN_BENCH) $(BIN_TEST)

.PHONY: all test bench clean

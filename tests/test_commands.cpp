#include <gtest/gtest.h>

#include "commands.hpp"
#include "store.hpp"

namespace {
std::string run(Store& s, std::vector<std::string> args) { return execute(s, args); }

struct FakeClock {
    int64_t t = 1000;
    Store::Clock fn() { return [this] { return t; }; }
};
}  // namespace

TEST(Commands, PingAndEcho) {
    Store s;
    EXPECT_EQ(run(s, {"PING"}), "+PONG\r\n");
    EXPECT_EQ(run(s, {"ping", "hi"}), "$2\r\nhi\r\n");
    EXPECT_EQ(run(s, {"ECHO", "yo"}), "$2\r\nyo\r\n");
}

TEST(Commands, SetGetDel) {
    Store s;
    EXPECT_EQ(run(s, {"SET", "user:123", "Naseem"}), "+OK\r\n");
    EXPECT_EQ(run(s, {"GET", "user:123"}), "$6\r\nNaseem\r\n");
    EXPECT_EQ(run(s, {"DEL", "user:123"}), ":1\r\n");
    EXPECT_EQ(run(s, {"GET", "user:123"}), "$-1\r\n");
    EXPECT_EQ(run(s, {"DEL", "user:123"}), ":0\r\n");
}

TEST(Commands, SetOverwrites) {
    Store s;
    run(s, {"SET", "k", "1"});
    run(s, {"SET", "k", "2"});
    EXPECT_EQ(run(s, {"GET", "k"}), "$1\r\n2\r\n");
    EXPECT_EQ(run(s, {"DBSIZE"}), ":1\r\n");
}

TEST(Commands, DelMultipleKeys) {
    Store s;
    run(s, {"SET", "a", "1"});
    run(s, {"SET", "b", "2"});
    EXPECT_EQ(run(s, {"DEL", "a", "b", "c"}), ":2\r\n");
}

TEST(Commands, Errors) {
    Store s;
    EXPECT_EQ(run(s, {"GET"}).substr(0, 4), "-ERR");
    EXPECT_EQ(run(s, {"SET", "k"}).substr(0, 4), "-ERR");
    EXPECT_EQ(run(s, {"NOPE"}).substr(0, 4), "-ERR");
}

TEST(CommandsTtl, SetExThenExpires) {
    FakeClock c; Store s(c.fn());
    EXPECT_EQ(run(s, {"SET", "session:123", "abc", "EX", "60"}), "+OK\r\n");
    EXPECT_EQ(run(s, {"TTL", "session:123"}), ":60\r\n");
    c.t += 59'000;
    EXPECT_EQ(run(s, {"GET", "session:123"}), "$3\r\nabc\r\n");
    EXPECT_EQ(run(s, {"TTL", "session:123"}), ":1\r\n");
    c.t += 1'000;
    EXPECT_EQ(run(s, {"GET", "session:123"}), "$-1\r\n");
    EXPECT_EQ(run(s, {"TTL", "session:123"}), ":-2\r\n");
}

TEST(CommandsTtl, OptionsAreCaseInsensitiveAndPxWorks) {
    FakeClock c; Store s(c.fn());
    run(s, {"set", "k", "v", "px", "1500"});
    EXPECT_EQ(run(s, {"PTTL", "k"}), ":1500\r\n");
    EXPECT_EQ(run(s, {"TTL", "k"}), ":2\r\n");  // 1.5s rounds to 2
}

TEST(CommandsTtl, TtlOfKeyWithoutExpiryAndMissingKey) {
    Store s;
    run(s, {"SET", "k", "v"});
    EXPECT_EQ(run(s, {"TTL", "k"}), ":-1\r\n");
    EXPECT_EQ(run(s, {"TTL", "nope"}), ":-2\r\n");
}

TEST(CommandsTtl, ExpireAndPersist) {
    FakeClock c; Store s(c.fn());
    EXPECT_EQ(run(s, {"EXPIRE", "k", "10"}), ":0\r\n");
    run(s, {"SET", "k", "v"});
    EXPECT_EQ(run(s, {"EXPIRE", "k", "10"}), ":1\r\n");
    EXPECT_EQ(run(s, {"PERSIST", "k"}), ":1\r\n");
    EXPECT_EQ(run(s, {"TTL", "k"}), ":-1\r\n");
    EXPECT_EQ(run(s, {"PEXPIRE", "k", "250"}), ":1\r\n");
    EXPECT_EQ(run(s, {"PTTL", "k"}), ":250\r\n");
}

TEST(CommandsTtl, PlainSetRemovesTtl) {
    FakeClock c; Store s(c.fn());
    run(s, {"SET", "k", "v", "EX", "5"});
    run(s, {"SET", "k", "v2"});
    EXPECT_EQ(run(s, {"TTL", "k"}), ":-1\r\n");
}

TEST(CommandsTtl, BadArgumentsAreRejected) {
    Store s;
    for (auto args : std::vector<std::vector<std::string>>{
             {"SET", "k", "v", "EX"},                        // missing number
             {"SET", "k", "v", "EX", "abc"},                 // not an integer
             {"SET", "k", "v", "EX", "0"},                   // must be positive
             {"SET", "k", "v", "EX", "-5"},
             {"SET", "k", "v", "EX", "9223372036854775807"}, // overflow guard
             {"SET", "k", "v", "EX", "1", "PX", "5"},        // both options
             {"SET", "k", "v", "ZZ", "1"},                   // unknown option
             {"EXPIRE", "k", "x"},
             {"TTL"}}) {
        EXPECT_EQ(run(s, args).substr(0, 4), "-ERR") << args[0] << " " << args.size();
    }
    EXPECT_EQ(run(s, {"DBSIZE"}), ":0\r\n");  // nothing was stored by the bad SETs
}

#include <gtest/gtest.h>

#include "commands.hpp"

namespace {
std::string run(Store& s, std::vector<std::string> args) { return execute(s, args); }
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

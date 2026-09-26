#include <gtest/gtest.h>

#include "resp.hpp"

TEST(RespParse, SetCommand) {
    std::string in = "*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nNaseem\r\n";
    auto r = parse_command(in);
    ASSERT_EQ(r.status, ParseStatus::Ok);
    EXPECT_EQ(r.consumed, in.size());
    EXPECT_EQ(r.args, (std::vector<std::string>{"SET", "name", "Naseem"}));
}

TEST(RespParse, IncompleteAtEveryPrefix) {
    std::string in = "*2\r\n$3\r\nGET\r\n$4\r\nname\r\n";
    for (size_t len = 0; len < in.size(); ++len) {
        EXPECT_EQ(parse_command(std::string_view(in).substr(0, len)).status, ParseStatus::Incomplete)
            << "prefix length " << len;
    }
    EXPECT_EQ(parse_command(in).status, ParseStatus::Ok);
}

TEST(RespParse, PipelinedCommandsConsumeOnlyTheFirst) {
    std::string a = "*1\r\n$4\r\nPING\r\n";
    std::string b = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
    auto r = parse_command(a + b);
    ASSERT_EQ(r.status, ParseStatus::Ok);
    EXPECT_EQ(r.consumed, a.size());
}

TEST(RespParse, ValueMayContainCrlfAndBinary) {
    std::string val("a\r\nb\0c", 6);
    std::string in = "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\n" + val + "\r\n";
    auto r = parse_command(in);
    ASSERT_EQ(r.status, ParseStatus::Ok);
    EXPECT_EQ(r.args[2], val);
}

TEST(RespParse, InlineCommand) {
    auto r = parse_command("SET  a   b\r\n");
    ASSERT_EQ(r.status, ParseStatus::Ok);
    EXPECT_EQ(r.args, (std::vector<std::string>{"SET", "a", "b"}));
}

TEST(RespParse, MalformedInputIsAnError) {
    EXPECT_EQ(parse_command("*x\r\n").status, ParseStatus::Error);
    EXPECT_EQ(parse_command("*1\r\n+PING\r\n").status, ParseStatus::Error);
    EXPECT_EQ(parse_command("*1\r\n$-5\r\n").status, ParseStatus::Error);
    EXPECT_EQ(parse_command("*1\r\n$4\r\nPINGxx").status, ParseStatus::Error);
}

TEST(RespEncode, Replies) {
    EXPECT_EQ(resp_simple("OK"), "+OK\r\n");
    EXPECT_EQ(resp_error("ERR x"), "-ERR x\r\n");
    EXPECT_EQ(resp_integer(42), ":42\r\n");
    EXPECT_EQ(resp_bulk("Naseem"), "$6\r\nNaseem\r\n");
    EXPECT_EQ(resp_null(), "$-1\r\n");
}

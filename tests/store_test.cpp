#include "../src/base62.hpp"
#include "../src/common.hpp"
#include "../src/store.hpp"
#include "test.hpp"

#include <rocksdb/db.h>
#include <cstdlib>
#include <memory>

#include <chrono>
#include <cstdio>
#include <set>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

using namespace shrt;

static std::string tmpdir() {
    std::string d = "/tmp/shrt-cpp-test-" + std::to_string(getpid()) + "-" +
                    std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    return d;
}

TEST(base62_encode) {
    CHECK_EQ(base62_encode(0), "0");
    CHECK_EQ(base62_encode(1), "1");
    CHECK_EQ(base62_encode(61), "Z");
    CHECK_EQ(base62_encode(62), "10");
    CHECK_EQ(base62_encode(3843), "ZZ");
}

TEST(shorten_generates_random_8char_codes) {
    Store s = Store::open(":memory:", -1);
    auto a = s.shorten("https://example.com", nullptr, 0);
    auto b = s.shorten("https://example.org", nullptr, 0);
    CHECK(a && b);
    CHECK_EQ(a->size(), 8u);
    CHECK_EQ(b->size(), 8u);
    CHECK(*a != *b);
}

TEST(resolve_counts_hits) {
    Store s = Store::open(":memory:", -1);
    auto code = s.shorten("https://example.com", nullptr, 0);
    for (int i = 0; i < 2; i++) {
        auto u = s.resolve(*code);
        CHECK(u && *u == "https://example.com");
    }
    auto st = s.stats(*code);
    CHECK(st);
    CHECK_EQ(st->hits, 2);
    CHECK_EQ(st->url, "https://example.com");
}

TEST(resolve_misses) {
    Store s = Store::open(":memory:", -1);
    CHECK(!s.resolve("nope"));
    CHECK(!s.stats("nope"));
}

TEST(alias_collision) {
    Store s = Store::open(":memory:", -1);
    std::string al = "my-link";
    auto r = s.shorten("https://a.com", &al, 0);
    CHECK(r && *r == "my-link");
    CHECK(*s.resolve("my-link") == "https://a.com");
    CHECK(!s.shorten("https://b.com", &al, 0));
    auto gen = s.shorten("https://c.com", nullptr, 0);
    CHECK(*gen != "my-link");
    CHECK(*s.resolve(*gen) == "https://c.com");
}

TEST(expired_links_stop_resolving) {
    Store s = Store::open(":memory:", -1);
    auto code = s.shorten("https://example.com", nullptr, 1);
    CHECK(s.resolve(*code));
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    CHECK(!s.resolve(*code));
}

TEST(shorten_many_aligned) {
    Store s = Store::open(":memory:", -1);
    std::vector<std::string> urls = {"https://a.com", "https://b.com", "https://c.com"};
    auto codes = s.shorten_many(urls, 0);
    CHECK_EQ(codes.size(), 3u);
    std::set<std::string> seen(codes.begin(), codes.end());
    CHECK_EQ(seen.size(), 3u);
    for (size_t i = 0; i < codes.size(); i++)
        CHECK(*s.resolve(codes[i]) == urls[i]);
}

TEST(persists_across_reopen) {
    std::string dir = tmpdir();
    {
        Store s1 = Store::open(dir, -1);
        auto code = s1.shorten("https://example.com", nullptr, 0);
        s1.resolve(*code);
        s1.resolve(*code);
        s1.close();

        Store s2 = Store::open(dir, 0);
        CHECK(*s2.resolve(*code) == "https://example.com");
        CHECK_EQ(s2.stats(*code)->hits, 3);
        s2.close();
    }
    std::string cmd = "rm -rf " + dir;
    system(cmd.c_str());
}

TEST(codes_prefix_sharded) {
    std::string dir = tmpdir();
    {
        Store a = Store::open(dir, 0);
        Store b = Store::open(dir, 1);
        auto ca = a.shorten("https://a.com", nullptr, 0);
        auto cb = b.shorten("https://b.com", nullptr, 0);
        CHECK(*ca != *cb);
        CHECK((*ca)[0] != (*cb)[0]);
        a.close();
        b.close();
    }
    std::string cmd = "rm -rf " + dir;
    system(cmd.c_str());
}

TEST(sibling_tailing_converges) {
    std::string dir = tmpdir();
    {
        Store a = Store::open(dir, 0);
        Store b = Store::open(dir, 1);
        auto code = a.shorten("https://a.com", nullptr, 0);
        a.flush();
        b.poll_tails();
        CHECK(*b.resolve(*code) == "https://a.com");
        a.close();
        b.close();
    }
    std::string cmd = "rm -rf " + dir;
    system(cmd.c_str());
}

TEST(sibling_sees_hits_via_tail) {
    std::string dir = tmpdir();
    {
        Store a = Store::open(dir, 0);
        Store b = Store::open(dir, 1);
        auto code = a.shorten("https://a.com", nullptr, 0);
        a.flush();
        b.poll_tails();
        a.resolve(*code);
        a.resolve(*code);
        a.flush();
        b.poll_tails();
        CHECK_EQ(b.stats(*code)->hits, 2);
        a.close();
        b.close();
    }
    std::string cmd = "rm -rf " + dir;
    system(cmd.c_str());
}

TEST(remove_tombstone_survives_reopen) {
    std::string dir = tmpdir();
    {
        Store s1 = Store::open(dir, -1);
        auto code = s1.shorten("https://example.com", nullptr, 0);
        CHECK(s1.remove(*code) == MutResult::Ok);
        CHECK(!s1.resolve(*code));
        s1.close();

        Store s2 = Store::open(dir, 0);
        CHECK(!s2.resolve(*code));
        CHECK(s2.remove("nope") == MutResult::Missing);
        s2.close();
    }
    std::string cmd = "rm -rf " + dir;
    system(cmd.c_str());
}

TEST(update_persists_across_reopen) {
    std::string dir = tmpdir();
    {
        Store s1 = Store::open(dir, -1);
        auto code = s1.shorten("https://old.example", nullptr, 0);
        s1.resolve(*code);
        CHECK(s1.update(*code, "https://new.example", 60'000, true) == MutResult::Ok);
        CHECK(*s1.resolve(*code) == "https://new.example");
        s1.close();

        Store s2 = Store::open(dir, 0);
        auto e = s2.stats(*code);
        CHECK(e);
        CHECK_EQ(e->url, "https://new.example");
        CHECK(e->expires_at > now_ms());
        CHECK(e->hits >= 1);
        s2.close();
    }
    std::string cmd = "rm -rf " + dir;
    system(cmd.c_str());
}

TEST(remote_owned_mutations_return_remote) {
    std::string dir = tmpdir();
    {
        Store a = Store::open(dir, 0);
        Store b = Store::open(dir, 1);
        auto code = a.shorten("https://a.example", nullptr, 0);
        a.flush();
        b.poll_tails();
        CHECK(b.update(*code, "https://x.example", 0, false) == MutResult::Remote);
        CHECK(b.remove(*code) == MutResult::Remote);
        CHECK(a.remove(*code) == MutResult::Ok);
        a.close();
        b.close();
    }
    std::string cmd = "rm -rf " + dir;
    system(cmd.c_str());
}

TEST(list_paginates_sorts_filters) {
    Store s = Store::open(":memory:", -1);
    auto a = s.shorten("https://aaa.example", nullptr, 0);
    s.shorten("https://bbb.example", nullptr, 0);
    s.shorten("https://ccc.example", nullptr, 0);
    s.resolve(*a);
    s.resolve(*a);

    auto [links, total] = s.list(50, 0, "created", "");
    CHECK_EQ(total, 3u);
    CHECK_EQ(links.size(), 3u);
    auto [page, _] = s.list(2, 0, "created", "");
    CHECK_EQ(page.size(), 2u);
    auto [by_hits, _2] = s.list(50, 0, "hits", "");
    CHECK_EQ(by_hits[0].code, *a);
    auto [filtered, ftotal] = s.list(50, 0, "created", "bbb");
    CHECK_EQ(ftotal, 1u);
    CHECK_EQ(filtered[0].url, "https://bbb.example");
}

TEST(compact_preserves_rows_and_truncates) {
    std::string dir = tmpdir();
    {
        Store s = Store::open(dir, 0);
        auto code = s.shorten("https://example.com", nullptr, 0);
        s.resolve(*code);
        s.compact();
        s.close();

        struct stat st {};
        CHECK(::stat((dir + "/data-0.snap").c_str(), &st) == 0 && st.st_size > 0);

        Store s2 = Store::open(dir, 0);
        CHECK(*s2.resolve(*code) == "https://example.com");
        CHECK_EQ(s2.stats(*code)->hits, 2);
        s2.close();
    }
    std::string cmd = "rm -rf " + dir;
    system(cmd.c_str());
}

int main() { return tst::run(); }

TEST(rocks_legacy_value_decode) {
    const char* dir = "/tmp/shrt-legacy-rocks";
    system(("rm -rf " + std::string(dir)).c_str());
    // write a pre-version "{e}|{c}|{u}" row straight into rocksdb
    {
        std::unique_ptr<rocksdb::DB> d;
        rocksdb::Options o;
        o.create_if_missing = true;
        rocksdb::Status st = rocksdb::DB::Open(o, dir, &d);
        CHECK(st.ok());
        rocksdb::ColumnFamilyHandle* links = nullptr;
        rocksdb::ColumnFamilyHandle* hits = nullptr;
        st = d->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "links", &links);
        CHECK(st.ok());
        st = d->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "hits", &hits);
        CHECK(st.ok());
        st = d->Put(rocksdb::WriteOptions(), links, "legacyR",
                    "0|0|https://rocks-legacy.example");
        CHECK(st.ok());
        d->DestroyColumnFamilyHandle(links);
        d->DestroyColumnFamilyHandle(hits);
    }
    auto s = Store::open_rocks(dir, 0, 16, 5000);
    auto u = s.resolve("legacyR");
    CHECK(u && *u == "https://rocks-legacy.example");
    // new writes are v1-tagged
    auto c = s.shorten("https://v1rocks.example", nullptr, 0);
    CHECK(c);
    s.close();
    {
        std::unique_ptr<rocksdb::DB> d;
        rocksdb::DBOptions dbo;
        std::vector<rocksdb::ColumnFamilyDescriptor> desc{
            {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()},
            {"links", rocksdb::ColumnFamilyOptions()},
            {"hits", rocksdb::ColumnFamilyOptions()}};
        std::vector<rocksdb::ColumnFamilyHandle*> hs;
        rocksdb::Status st = rocksdb::DB::Open(dbo, dir, desc, &hs, &d);
        CHECK(st.ok());
        rocksdb::ColumnFamilyHandle* links = nullptr;
        for (auto* h : hs) if (h->GetName() == "links") links = h;
        CHECK(links);
        std::string v;
        st = d->Get(rocksdb::ReadOptions(), links, *c, &v);
        CHECK(st.ok());
        CHECK(v.rfind("v1|", 0) == 0);
        for (auto* h : hs) d->DestroyColumnFamilyHandle(h);
    }
}

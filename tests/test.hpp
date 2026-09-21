#pragma once
#include <cstdio>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

namespace tst {
struct Case { const char* name; std::function<void()> fn; };
inline std::vector<Case>& cases() { static std::vector<Case> v; return v; }
inline int& fails() { static int f = 0; return f; }
struct Reg {
    Reg(const char* n, std::function<void()> fn) { cases().push_back({n, std::move(fn)}); }
};
inline int run() {
    int pass = 0;
    for (auto& c : cases()) {
        int before = fails();
        try { c.fn(); } catch (const std::exception& e) {
            printf("  FAIL %s: exception %s\n", c.name, e.what());
            fails()++;
        }
        if (fails() == before) { printf("  ok   %s\n", c.name); pass++; }
    }
    printf("%d/%zu passed\n", pass, cases().size());
    return fails() ? 1 : 0;
}
} // namespace tst

#define TEST(name) \
    static void test_##name(); \
    static ::tst::Reg reg_##name(#name, test_##name); \
    static void test_##name()

#define CHECK(cond) \
    do { if (!(cond)) { \
        printf("  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond); \
        ::tst::fails()++; return; \
    } } while (0)

#define CHECK_EQ(a, b) \
    do { auto _va = (a); auto _vb = (b); if (!(_va == _vb)) { \
        printf("  FAIL %s:%d: %s != %s\n", __FILE__, __LINE__, #a, #b); \
        ::tst::fails()++; return; \
    } } while (0)

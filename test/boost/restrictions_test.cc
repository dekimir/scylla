/*
 * Copyright (C) 2019 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <experimental/source_location>
#include <fmt/format.h>
#include <seastar/testing/thread_test_case.hh>

#include "cql3/cql_config.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"

namespace {

std::unique_ptr<cql3::query_options> wip_on() {
    static cql3::cql_config wip{{ .use_wip=true }};
    static auto& d = cql3::query_options::DEFAULT;
    return std::make_unique<cql3::query_options>(
            wip,
            d.get_consistency(), d.get_timeout_config(), d.get_names(), d.get_values(), d.skip_metadata(),
            d.get_specific_options(), d.get_cql_serialization_format());
}

using std::experimental::source_location;

/// Asserts that cquery_nofail(e, qstr, wip_on) contains expected rows, in any order.
void wip_require_rows(cql_test_env& env,
                  const char* qstr,
                  const std::vector<std::vector<bytes_opt>>& expected,
                  const source_location& loc = source_location::current()) {
    try {
        assert_that(cquery_nofail(env, qstr, wip_on(), loc)).is_rows().with_rows_ignore_order(expected);
    }
    catch (const std::exception& e) {
        BOOST_FAIL(format("query '{}' failed: {}\n{}:{}: originally from here",
                          qstr, e.what(), loc.file_name(), loc.line()));
    }
}

auto I(int32_t x) { return int32_type->decompose(x); }
auto F(float f) { return float_type->decompose(f); }
auto T(const char* t) { return utf8_type->decompose(t); }

auto SI(const set_type_impl::native_type& val) {
    const auto int_set_type = set_type_impl::get_instance(int32_type, true);
    return int_set_type->decompose(make_set_value(int_set_type, val));
};

auto ST(const set_type_impl::native_type& val) {
    const auto text_set_type = set_type_impl::get_instance(utf8_type, true);
    return text_set_type->decompose(make_set_value(text_set_type, val));
};

auto LI(const list_type_impl::native_type& val) {
    const auto int_list_type = list_type_impl::get_instance(int32_type, true);
    return int_list_type->decompose(make_list_value(int_list_type, val));
}

auto LT(const list_type_impl::native_type& val) {
    const auto text_list_type = list_type_impl::get_instance(utf8_type, true);
    return text_list_type->decompose(make_list_value(text_list_type, val));
}

/// Creates a table t with int columns p, q, and r.  Inserts data (i,10+i,20+i) for i = 0 to n.
void create_t_with_p_q_r(cql_test_env& e, size_t n) {
    cquery_nofail(e, "create table t (p int primary key, q int, r int)");
    for (size_t i = 0; i <= n; ++i) {
        cquery_nofail(e, fmt::format("insert into t (p,q,r) values ({},{},{});", i, 10+i, 20+i));
    }
}

} // anonymous namespace

SEASTAR_THREAD_TEST_CASE(regular_col_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        create_t_with_p_q_r(e, 3);
        wip_require_rows(e, "select q from t where q=12 allow filtering", {{I(12)}});
        // TODO: enable when supported:
        //wip_require_rows(e, "select q from t where q=12 and q=12 allow filtering", {{I(12)}});
        //wip_require_rows(e, "select q from t where q=12 and q=13 allow filtering", {});
        wip_require_rows(e, "select r from t where q=12 and p=2 allow filtering", {{I(22), I(12)}});
        wip_require_rows(e, "select p from t where q=12 and r=22 allow filtering", {{I(2), I(12), I(22)}});
        wip_require_rows(e, "select r from t where q=12 and p=2 and r=99 allow filtering", {});
        cquery_nofail(e, "insert into t(p) values (100)");
        wip_require_rows(e, "select q from t where q=12 allow filtering", {{I(12)}});
        // TODO: enable when supported:
        //wip_require_rows(e, "select p from t where q=null allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(map_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, m frozen<map<int,int>>)");
        cquery_nofail(e, "insert into t (p, m) values (1, {1:11, 2:12, 3:13})");
        cquery_nofail(e, "insert into t (p, m) values (2, {1:21, 2:22, 3:23})");
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m1 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 11}, {2, 12}, {3, 13}})));
        wip_require_rows(e, "select p from t where m={1:11, 2:12, 3:13} allow filtering", {{I(1), m1}});
        wip_require_rows(e, "select p from t where m={1:11, 2:12} allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(set_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, m frozen<set<int>>)");
        cquery_nofail(e, "insert into t (p, m) values (1, {11,12,13})");
        cquery_nofail(e, "insert into t (p, m) values (2, {21,22,23})");
        wip_require_rows(e, "select p from t where m={21,22,23} allow filtering", {{I(2), SI({21, 22, 23})}});
        wip_require_rows(e, "select p from t where m={21,22,23,24} allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, li frozen<list<int>>)");
        cquery_nofail(e, "insert into t (p, li) values (1, [11,12,13])");
        cquery_nofail(e, "insert into t (p, li) values (2, [21,22,23])");
        wip_require_rows(e, "select p from t where li=[21,22,23] allow filtering", {{I(2), LI({21, 22, 23})}});
        wip_require_rows(e, "select p from t where li=[23,22,21] allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, li frozen<list<int>>)");
        cquery_nofail(e, "insert into t (p, li) values (1, [11,12,13])");
        cquery_nofail(e, "insert into t (p, li) values (2, [21,22,23])");
        wip_require_rows(e, "select li from t where li<[23,22,21] allow filtering",
                     {{LI({11, 12, 13})}, {LI({21, 22, 23})}});
        wip_require_rows(e, "select li from t where li>=[11,12,13] allow filtering",
                     {{LI({11, 12, 13})}, {LI({21, 22, 23})}});
        wip_require_rows(e, "select li from t where li>[11,12,13] allow filtering", {{LI({21, 22, 23})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(tuple_of_list) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, l1 frozen<list<int>>, l2 frozen<list<int>>, primary key(p,l1,l2))");
        cquery_nofail(e, "insert into t (p, l1, l2) values (1, [11,12], [101,102])");
        cquery_nofail(e, "insert into t (p, l1, l2) values (2, [21,22], [201,202])");
        wip_require_rows(e, "select * from t where (l1,l2)<([],[]) allow filtering", {});
        wip_require_rows(e, "select l1 from t where (l1,l2)<([20],[200]) allow filtering", {{LI({11, 12}), LI({101, 102})}});
        wip_require_rows(e, "select l1 from t where (l1,l2)>=([11,12],[101,102]) allow filtering",
                     {{LI({11, 12}), LI({101, 102})}, {LI({21, 22}), LI({201, 202})}});
        wip_require_rows(e, "select l1 from t where (l1,l2)<([11,12],[101,103]) allow filtering",
                     {{LI({11, 12}), LI({101, 102})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(map_entry_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, m map<int,int>)");
        cquery_nofail(e, "insert into t (p, m) values (1, {1:11, 2:12, 3:13})");
        cquery_nofail(e, "insert into t (p, m) values (2, {1:21, 2:22, 3:23})");
        cquery_nofail(e, "insert into t (p, m) values (3, {1:31, 2:32, 3:33})");
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m2 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 21}, {2, 22}, {3, 23}})));
        wip_require_rows(e, "select p from t where m[1]=21 allow filtering", {{I(2), m2}});
        wip_require_rows(e, "select p from t where m[1]=21 and m[3]=23 allow filtering", {{I(2), m2}});
        wip_require_rows(e, "select p from t where m[99]=21 allow filtering", {});
        wip_require_rows(e, "select p from t where m[1]=99 allow filtering", {});
        cquery_nofail(e, "delete from t where p=2");
        wip_require_rows(e, "select p from t where m[1]=21 allow filtering", {});
        wip_require_rows(e, "select p from t where m[1]=21 and m[3]=23 allow filtering", {});
        const auto m3 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 31}, {2, 32}, {3, 33}})));
        wip_require_rows(e, "select m from t where m[1]=31 allow filtering", {{m3}});
        cquery_nofail(e, "update t set m={1:111} where p=3");
        wip_require_rows(e, "select p from t where m[1]=31 allow filtering", {});
        wip_require_rows(e, "select p from t where m[1]=21 allow filtering", {});
        const auto m3new = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 111}})));
        wip_require_rows(e, "select p from t where m[1]=111 allow filtering", {{I(3), m3new}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(regular_col_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        create_t_with_p_q_r(e, 3);
        wip_require_rows(e, "select q from t where q>12 allow filtering", {{I(13)}});
        wip_require_rows(e, "select q from t where q<12 allow filtering", {{I(10)}, {I(11)}});
        wip_require_rows(e, "select q from t where q>99 allow filtering", {});
        wip_require_rows(e, "select r from t where q<12 and q>=11 allow filtering", {{I(21), I(11)}});
        // TODO: enable when #5799 is fixed:
        //wip_require_rows(e, "select * from t where q<11 and q>11 allow filtering", {});
        wip_require_rows(e, "select q from t where q<=12 and r>=21 allow filtering", {{I(11), I(21)}, {I(12), I(22)}});
        // TODO: enable when supported:
        //wip_require_rows(e, "select q from t where q < null allow filtering", {{I(10)}, {I(11)}, {I(12)}, {I(13)}});
        cquery_nofail(e, "insert into t(p) values (4)");
        wip_require_rows(e, "select q from t where q<12 allow filtering", {{std::nullopt}, {I(10)}, {I(11)}});
        wip_require_rows(e, "select q from t where q>10 allow filtering", {{I(11)}, {I(12)}, {I(13)}});
        wip_require_rows(e, "select q from t where q<12 and q>10 allow filtering", {{I(11)}});
        // TODO: enable when supported:
        // wip_require_rows(e, "select q from t where q < null allow filtering",
        //              {{std::nullopt}, {I(10)}, {I(11)}, {I(12)}, {I(13)}});
        // wip_require_rows(e, "select q from t where q > null allow filtering",
        //              {{std::nullopt}, {I(10)}, {I(11)}, {I(12)}, {I(13)}});
    }).get();
}

#if 0 // TODO: enable when supported.
SEASTAR_THREAD_TEST_CASE(regular_col_neq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        create_t_with_p_q_r(e, 3);
        wip_require_rows(e, "select q from t where q!=10 allow filtering", {{I(11)}, {I(12)}, {I(13)}});
        wip_require_rows(e, "select q from t where q!=10 and q!=13 allow filtering", {{I(11)}, {I(12)}});
        wip_require_rows(e, "select r from t where q!=11 and r!=22 allow filtering", {{I(10), I(20)}, {I(13), I(23)}});
    }).get();
}
#endif // 0

SEASTAR_THREAD_TEST_CASE(multi_col_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c1 text, c2 float, primary key (p, c1, c2))");
        cquery_nofail(e, "insert into t (p, c1, c2) values (1, 'one', 11);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (2, 'two', 12);");
        wip_require_rows(e, "select c2 from t where p=1 and (c1,c2)=('one',11)", {{F(11)}});
        wip_require_rows(e, "select p from t where (c1,c2)=('two',12) allow filtering", {{I(2), T("two"), F(12)}});
        wip_require_rows(e, "select c2 from t where (c1,c2)=('one',12) allow filtering", {});
        wip_require_rows(e, "select c2 from t where (c1,c2)=('two',11) allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(multi_col_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c1 text, c2 float, primary key (p, c1, c2))");
        cquery_nofail(e, "insert into t (p, c1, c2) values (1, 'a', 11);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (2, 'b', 2);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (3, 'c', 13);");
        wip_require_rows(e, "select c2 from t where (c1,c2)>('a',20) allow filtering", {{F(2), T("b")}, {F(13), T("c")}});
        wip_require_rows(e, "select p from t where (c1,c2)>=('a',20) and (c1,c2)<('b',3) allow filtering",
                     {{I(2), T("b"), F(2)}});
        wip_require_rows(e, "select * from t where (c1,c2)<('a',11) allow filtering", {});
        wip_require_rows(e, "select c1 from t where (c1,c2)<('a',12) allow filtering", {{T("a"), F(11)}});
        wip_require_rows(e, "select c1 from t where (c1,c2)<=('c',13) allow filtering",
                     {{T("a"), F(11)}, {T("b"), F(2)}, {T("c"), F(13)}});
        wip_require_rows(e, "select c1 from t where (c1,c2)>=('b',2) and (c1,c2)<=('b',2) allow filtering",
                     {{T("b"), F(2)}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(set_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<set<int>>, c frozen<set<int>>, s set<text>, st set<int> static, primary key (p, c))");
        wip_require_rows(e, "select * from t where c contains 222 allow filtering", {});
        cquery_nofail(e, "insert into t (p, c, s) values ({1}, {11, 12}, {'a1', 'b1'})");
        cquery_nofail(e, "insert into t (p, c, s) values ({2}, {21, 22}, {'a2', 'b1'})");
        cquery_nofail(e, "insert into t (p, c, s) values ({1, 3}, {31, 32}, {'a3', 'b3'})");
        wip_require_rows(e, "select * from t where s contains 'xyz' allow filtering", {});
        wip_require_rows(e, "select * from t where p contains 999 allow filtering", {});
        wip_require_rows(e, "select p from t where p contains 3 allow filtering", {{SI({1, 3})}});
        wip_require_rows(e, "select p from t where p contains 1 allow filtering", {{SI({1, 3})}, {SI({1})}});
        wip_require_rows(e, "select p from t where p contains 1 and s contains 'a1' allow filtering",
                     {{SI({1}), ST({"a1", "b1"})}});
        wip_require_rows(e, "select c from t where c contains 31 allow filtering", {{SI({31, 32})}});
        wip_require_rows(e, "select c from t where c contains 11 and p contains 1 allow filtering",
                     {{SI({11, 12}), SI({1})}});
        wip_require_rows(e, "select s from t where s contains 'a1' allow filtering", {{ST({"a1", "b1"})}});
        wip_require_rows(e, "select s from t where s contains 'b1' allow filtering",
                     {{ST({"a1", "b1"})}, {ST({"a2", "b1"})}});
        wip_require_rows(e, "select s from t where s contains 'b1' and s contains '' allow filtering", {});
        wip_require_rows(e, "select s from t where s contains 'b1' and p contains 4 allow filtering", {});
        cquery_nofail(e, "insert into t (p, c, st) values ({4}, {41}, {104})");
        wip_require_rows(e, "select st from t where st contains 4 allow filtering", {});
        wip_require_rows(e, "select st from t where st contains 104 allow filtering", {{SI({104})}});
        cquery_nofail(e, "insert into t (p, c, st) values ({4}, {42}, {105})");
        wip_require_rows(e, "select c from t where st contains 104 allow filtering", {});
        wip_require_rows(e, "select c from t where st contains 105 allow filtering",
                     {{SI({41}), SI({105})}, {SI({42}), SI({105})}});
        cquery_nofail(e, "insert into t (p, c, st) values ({5}, {52}, {104, 105})");
        wip_require_rows(e, "select p from t where st contains 105 allow filtering",
                     {{SI({4}), SI({105})}, {SI({4}), SI({105})}, {SI({5}), SI({104, 105})}});
        cquery_nofail(e, "delete from t where p={4}");
        wip_require_rows(e, "select p from t where st contains 105 allow filtering", {{SI({5}), SI({104, 105})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<list<int>>, c frozen<list<int>>, ls list<int>, st list<text> static,"
                      "primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c) values ([1], [11,12,13])");
        cquery_nofail(e, "insert into t (p, c, ls) values ([2], [21,22,23], [102])");
        cquery_nofail(e, "insert into t (p, c, ls, st) values ([3], [21,32,33], [103], ['a', 'b'])");
        cquery_nofail(e, "insert into t (p, c, st) values ([4], [41,42,43], ['a'])");
        cquery_nofail(e, "insert into t (p, c) values ([4], [41,42])");
        wip_require_rows(e, "select p from t where p contains 222 allow filtering", {});
        wip_require_rows(e, "select p from t where c contains 222 allow filtering", {});
        wip_require_rows(e, "select p from t where ls contains 222 allow filtering", {});
        wip_require_rows(e, "select p from t where st contains 'xyz' allow filtering", {});
        wip_require_rows(e, "select p from t where p contains 1 allow filtering", {{LI({1})}});
        wip_require_rows(e, "select p from t where p contains 4 allow filtering", {{LI({4})}, {LI({4})}});
        wip_require_rows(e, "select c from t where c contains 22 allow filtering", {{LI({21,22,23})}});
        wip_require_rows(e, "select c from t where c contains 21 allow filtering", {{LI({21,22,23})}, {LI({21,32,33})}});
        wip_require_rows(e, "select c from t where c contains 21 and ls contains 102 allow filtering",
                     {{LI({21,22,23}), LI({102})}});
        wip_require_rows(e, "select ls from t where ls contains 102 allow filtering", {{LI({102})}});
        wip_require_rows(e, "select st from t where st contains 'a' allow filtering", {{LT({"a"})}, {LT({"a"})}, {LT({"a", "b"})}});
        wip_require_rows(e, "select st from t where st contains 'b' allow filtering", {{LT({"a", "b"})}});
        cquery_nofail(e, "delete from t where p=[2]");
        wip_require_rows(e, "select c from t where c contains 21 allow filtering", {{LI({21,32,33})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(map_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<map<int,int>>, c frozen<map<int,int>>, m map<int,int>,"
                      "s map<int,int> static, primary key(p, c))");
        cquery_nofail(e, "insert into t (p, c, m) values ({1:1}, {10:10}, {1:11, 2:12})");
        wip_require_rows(e, "select * from t where m contains 21 allow filtering", {});
        cquery_nofail(e, "insert into t (p, c, m) values ({2:2}, {20:20}, {1:21, 2:12})");
        cquery_nofail(e, "insert into t (p, c) values ({3:3}, {30:30})");
        cquery_nofail(e, "insert into t (p, c, s) values ({3:3}, {31:31}, {3:100})");
        cquery_nofail(e, "insert into t (p, c, s) values ({4:4}, {40:40}, {4:100})");
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m2 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 21}, {2, 12}})));
        wip_require_rows(e, "select m from t where m contains 21 allow filtering", {{m2}});
        const auto m1 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 11}, {2, 12}})));
        wip_require_rows(e, "select m from t where m contains 11 allow filtering", {{m1}});
        wip_require_rows(e, "select m from t where m contains 12 allow filtering", {{m1}, {m2}});
        wip_require_rows(e, "select m from t where m contains 11 and m contains 12 allow filtering", {{m1}});
        cquery_nofail(e, "delete from t where p={2:2}");
        wip_require_rows(e, "select m from t where m contains 12 allow filtering", {{m1}});
        const auto s3 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{3, 100}})));
        const auto s4 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{4, 100}})));
        wip_require_rows(e, "select s from t where s contains 100 allow filtering", {{s3}, {s3}, {s4}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(contains_key) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e,
                      "create table t (p frozen<map<int,int>>, c frozen<map<text,int>>, m map<int,int>, "
                      "s map<int,text> static, primary key(p, c))");
        cquery_nofail(e, "insert into t (p,c,m) values ({1:11, 2:12}, {'el':11, 'twel':12}, {11:11, 12:12})");
        wip_require_rows(e, "select * from t where p contains key 3 allow filtering", {});
        wip_require_rows(e, "select * from t where c contains key 'x' allow filtering", {});
        wip_require_rows(e, "select * from t where m contains key 3 allow filtering", {});
        cquery_nofail(e, "insert into t (p,c,m) values ({3:33}, {'th':33}, {11:33})");
        const auto int_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m1 = int_map_type->decompose(
                make_map_value(int_map_type, map_type_impl::native_type({{11, 11}, {12, 12}})));
        const auto m2 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{11, 33}})));
        wip_require_rows(e, "select m from t where m contains key 12 allow filtering", {{m1}});
        wip_require_rows(e, "select m from t where m contains key 11 allow filtering", {{m1}, {m2}});
        const auto text_map_type = map_type_impl::get_instance(utf8_type, int32_type, true);
        const auto c1 = text_map_type->decompose(
                make_map_value(text_map_type, map_type_impl::native_type({{"el", 11}, {"twel", 12}})));
        wip_require_rows(e, "select c from t where c contains key 'el' allow filtering", {{c1}});
        wip_require_rows(e, "select c from t where c contains key 'twel' allow filtering", {{c1}});
        const auto p3 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{3, 33}})));
        wip_require_rows(e, "select p from t where p contains key 3 allow filtering", {{p3}});
        cquery_nofail(e, "insert into t (p,c) values ({4:44}, {'aaaa':44})");
        wip_require_rows(e, "select m from t where m contains key 12 allow filtering", {{m1}});
        cquery_nofail(e, "delete from t where p={1:11, 2:12}");
        wip_require_rows(e, "select m from t where m contains key 12 allow filtering", {});
        wip_require_rows(e, "select s from t where s contains key 55 allow filtering", {});
        cquery_nofail(e, "insert into t (p,c,s) values ({5:55}, {'aaaa':55}, {55:'aaaa'})");
        cquery_nofail(e, "insert into t (p,c,s) values ({5:55}, {'aaa':55}, {55:'aaaa'})");
        const auto int_text_map_type = map_type_impl::get_instance(int32_type, utf8_type, true);
        const auto s5 = int_text_map_type->decompose(
                make_map_value(int_text_map_type, map_type_impl::native_type({{55, "aaaa"}})));
        wip_require_rows(e, "select s from t where s contains key 55 allow filtering", {{s5}, {s5}});
        const auto c51 = text_map_type->decompose(
                make_map_value(text_map_type, map_type_impl::native_type({{"aaaa", 55}})));
        const auto c52 = text_map_type->decompose(
                make_map_value(text_map_type, map_type_impl::native_type({{"aaa", 55}})));
        wip_require_rows(e, "select c from t where s contains key 55 allow filtering", {{c51, s5}, {c52, s5}});
        cquery_nofail(e, "insert into t (p,c,s) values ({6:66}, {'bbb':66}, {66:'bbbb', 55:'bbbb'})");
        const auto p5 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{5, 55}})));
        const auto p6 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{6, 66}})));
        const auto s6 = int_text_map_type->decompose(
                make_map_value(int_text_map_type, map_type_impl::native_type({{55, "bbbb"}, {66, "bbbb"}})));
        wip_require_rows(e, "select p from t where s contains key 55 allow filtering", {{p5, s5}, {p5, s5}, {p6, s6}});
    }).get();
}

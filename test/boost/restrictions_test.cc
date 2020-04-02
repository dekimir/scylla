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
///
/// TODO: DRY from cql_query_test.
void require_rows(cql_test_env& env,
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
        require_rows(e, "select q from t where q=12 allow filtering", {{I(12)}});
        // TODO: enable when supported:
        //require_rows(e, "select q from t where q=12 and q=12 allow filtering", {{I(12)}});
        //require_rows(e, "select q from t where q=12 and q=13 allow filtering", {});
        require_rows(e, "select r from t where q=12 and p=2 allow filtering", {{I(22), I(12)}});
        require_rows(e, "select p from t where q=12 and r=22 allow filtering", {{I(2), I(12), I(22)}});
        require_rows(e, "select r from t where q=12 and p=2 and r=99 allow filtering", {});
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
        require_rows(e, "select p from t where m={1:11, 2:12, 3:13} allow filtering", {{I(1), m1}});
        require_rows(e, "select p from t where m={1:11, 2:12} allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(set_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, m frozen<set<int>>)");
        cquery_nofail(e, "insert into t (p, m) values (1, {11,12,13})");
        cquery_nofail(e, "insert into t (p, m) values (2, {21,22,23})");
        const auto my_set_type = set_type_impl::get_instance(int32_type, true);
        const auto s2 = my_set_type->decompose(
                make_set_value(my_set_type, set_type_impl::native_type({21, 22, 23})));
        require_rows(e, "select p from t where m={21,22,23} allow filtering", {{I(2), s2}});
        require_rows(e, "select p from t where m={21,22,23,24} allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, li frozen<list<int>>)");
        cquery_nofail(e, "insert into t (p, li) values (1, [11,12,13])");
        cquery_nofail(e, "insert into t (p, li) values (2, [21,22,23])");
        const auto my_list_type = list_type_impl::get_instance(int32_type, true);
        const auto li2 = my_list_type->decompose(
                make_list_value(my_list_type, list_type_impl::native_type({21, 22, 23})));
        require_rows(e, "select p from t where li=[21,22,23] allow filtering", {{I(2), li2}});
        require_rows(e, "select p from t where li=[23,22,21] allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, li frozen<list<int>>)");
        cquery_nofail(e, "insert into t (p, li) values (1, [11,12,13])");
        cquery_nofail(e, "insert into t (p, li) values (2, [21,22,23])");
        const auto my_list_type = list_type_impl::get_instance(int32_type, true);
        const auto li1 = my_list_type->decompose(
                make_list_value(my_list_type, list_type_impl::native_type({11, 12, 13})));
        const auto li2 = my_list_type->decompose(
                make_list_value(my_list_type, list_type_impl::native_type({21, 22, 23})));
        require_rows(e, "select li from t where li<[23,22,21] allow filtering", {{li1}, {li2}});
        require_rows(e, "select li from t where li>=[11,12,13] allow filtering", {{li1}, {li2}});
        require_rows(e, "select li from t where li>[11,12,13] allow filtering", {{li2}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(tuple_of_list) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, l1 frozen<list<int>>, l2 frozen<list<int>>, primary key(p,l1,l2))");
        cquery_nofail(e, "insert into t (p, l1, l2) values (1, [11,12], [101,102])");
        cquery_nofail(e, "insert into t (p, l1, l2) values (2, [21,22], [201,202])");
        require_rows(e, "select * from t where (l1,l2)<([],[]) allow filtering", {});
        const auto my_list_type = list_type_impl::get_instance(int32_type, true);
        const auto l11 = my_list_type->decompose(
                make_list_value(my_list_type, list_type_impl::native_type({11, 12})));
        const auto l21 = my_list_type->decompose(
                make_list_value(my_list_type, list_type_impl::native_type({101, 102})));
        require_rows(e, "select l1 from t where (l1,l2)<([20],[200]) allow filtering", {{l11, l21}});
        const auto l12 = my_list_type->decompose(
                make_list_value(my_list_type, list_type_impl::native_type({21, 22})));
        const auto l22 = my_list_type->decompose(
                make_list_value(my_list_type, list_type_impl::native_type({201, 202})));
        require_rows(e, "select l1 from t where (l1,l2)>=([11,12],[101,102]) allow filtering", {{l11, l21}, {l12, l22}});
        require_rows(e, "select l1 from t where (l1,l2)<([11,12],[101,103]) allow filtering", {{l11, l21}});
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
        require_rows(e, "select p from t where m[1]=21 allow filtering", {{I(2), m2}});
        require_rows(e, "select p from t where m[1]=21 and m[3]=23 allow filtering", {{I(2), m2}});
        require_rows(e, "select p from t where m[99]=21 allow filtering", {});
        require_rows(e, "select p from t where m[1]=99 allow filtering", {});
        cquery_nofail(e, "delete from t where p=2");
        require_rows(e, "select p from t where m[1]=21 allow filtering", {});
        require_rows(e, "select p from t where m[1]=21 and m[3]=23 allow filtering", {});
        const auto m3 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 31}, {2, 32}, {3, 33}})));
        require_rows(e, "select m from t where m[1]=31 allow filtering", {{m3}});
        cquery_nofail(e, "update t set m={1:111} where p=3");
        require_rows(e, "select p from t where m[1]=31 allow filtering", {});
        require_rows(e, "select p from t where m[1]=21 allow filtering", {});
        const auto m3new = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 111}})));
        require_rows(e, "select p from t where m[1]=111 allow filtering", {{I(3), m3new}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(regular_col_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        create_t_with_p_q_r(e, 3);
        require_rows(e, "select q from t where q>12 allow filtering", {{I(13)}});
        require_rows(e, "select q from t where q<12 allow filtering", {{I(10)}, {I(11)}});
        require_rows(e, "select q from t where q>99 allow filtering", {});
        require_rows(e, "select r from t where q<12 and q>=11 allow filtering", {{I(21), I(11)}});
        // TODO: enable when #5799 is fixed:
        //require_rows(e, "select * from t where q<11 and q>11 allow filtering", {});
        require_rows(e, "select q from t where q<=12 and r>=21 allow filtering", {{I(11), I(21)}, {I(12), I(22)}});
    }).get();
}

#if 0 // TODO: enable when supported.
SEASTAR_THREAD_TEST_CASE(regular_col_neq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        create_t_with_p_q_r(e, 3);
        require_rows(e, "select q from t where q!=10 allow filtering", {{I(11)}, {I(12)}, {I(13)}});
        require_rows(e, "select q from t where q!=10 and q!=13 allow filtering", {{I(11)}, {I(12)}});
        require_rows(e, "select r from t where q!=11 and r!=22 allow filtering", {{I(10), I(20)}, {I(13), I(23)}});
    }).get();
}
#endif // 0

SEASTAR_THREAD_TEST_CASE(multi_col_eq) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c1 text, c2 float, primary key (p, c1, c2))");
        cquery_nofail(e, "insert into t (p, c1, c2) values (1, 'one', 11);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (2, 'two', 12);");
        require_rows(e, "select c2 from t where p=1 and (c1,c2)=('one',11)", {{F(11)}});
        require_rows(e, "select p from t where (c1,c2)=('two',12) allow filtering", {{I(2), T("two"), F(12)}});
        require_rows(e, "select c2 from t where (c1,c2)=('one',12) allow filtering", {});
        require_rows(e, "select c2 from t where (c1,c2)=('two',11) allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(multi_col_slice) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c1 text, c2 float, primary key (p, c1, c2))");
        cquery_nofail(e, "insert into t (p, c1, c2) values (1, 'a', 11);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (2, 'b', 2);");
        cquery_nofail(e, "insert into t (p, c1, c2) values (3, 'c', 13);");
        require_rows(e, "select c2 from t where (c1,c2)>('a',20) allow filtering", {{F(2), T("b")}, {F(13), T("c")}});
        require_rows(e, "select p from t where (c1,c2)>=('a',20) and (c1,c2)<('b',3) allow filtering",
                     {{I(2), T("b"), F(2)}});
        require_rows(e, "select * from t where (c1,c2)<('a',11) allow filtering", {});
        require_rows(e, "select c1 from t where (c1,c2)<('a',12) allow filtering", {{T("a"), F(11)}});
        require_rows(e, "select c1 from t where (c1,c2)<=('c',13) allow filtering",
                     {{T("a"), F(11)}, {T("b"), F(2)}, {T("c"), F(13)}});
        require_rows(e, "select c1 from t where (c1,c2)>=('b',2) and (c1,c2)<=('b',2) allow filtering",
                     {{T("b"), F(2)}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(bounds) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, c int, primary key (p, c))");
        cquery_nofail(e, "insert into t (p, c) values (1, 11);");
        cquery_nofail(e, "insert into t (p, c) values (2, 12);");
        cquery_nofail(e, "insert into t (p, c) values (3, 13);");
        require_rows(e, "select p from t where p=1 and c > 10", {{I(1)}});
        require_rows(e, "select c from t where p in (1,2,3) and c > 11 and c < 13", {{I(12)}});
        require_rows(e, "select c from t where p in (1,2,3) and c >= 11 and c < 13", {{I(11)}, {I(12)}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(token) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, q int, r int, primary key ((p, q)))");
        cquery_nofail(e, "insert into t (p,q,r) values (1,11,101);");
        cquery_nofail(e, "insert into t (p,q,r) values (2,12,102);");
        cquery_nofail(e, "insert into t (p,q,r) values (3,13,103);");
        require_rows(e, "select p from t where token(p,q) = token(1,11)", {{I(1)}});
        require_rows(e, "select p from t where token(p,q) >= token(1,11) and token(p,q) <= token(1,11)", {{I(1)}});
        require_rows(e, "select p from t where token(p,q) <= token(1,11) and r<102 allow filtering", {{I(1), I(101)}});
        require_rows(e, "select p from t where token(p,q) = token(2,12) and r<102 allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(set_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p frozen<set<int>>, c frozen<set<int>>, s set<text>, st set<int> static, primary key (p, c))");
        require_rows(e, "select * from t where c contains 222 allow filtering", {});
        cquery_nofail(e, "insert into t (p, c, s) values ({1}, {11, 12}, {'a1', 'b1'})");
        cquery_nofail(e, "insert into t (p, c, s) values ({2}, {21, 22}, {'a2', 'b1'})");
        cquery_nofail(e, "insert into t (p, c, s) values ({1, 3}, {31, 32}, {'a3', 'b3'})");
        require_rows(e, "select * from t where s contains 'xyz' allow filtering", {});
        require_rows(e, "select * from t where p contains 999 allow filtering", {});
        require_rows(e, "select p from t where p contains 3 allow filtering", {{SI({1, 3})}});
        require_rows(e, "select p from t where p contains 1 allow filtering", {{SI({1, 3})}, {SI({1})}});
        require_rows(e, "select c from t where c contains 31 allow filtering", {{SI({31, 32})}});
        require_rows(e, "select c from t where c contains 11 and p contains 1 allow filtering",
                     {{SI({11, 12}), SI({1})}});
        require_rows(e, "select s from t where s contains 'a1' allow filtering", {{ST({"a1", "b1"})}});
        require_rows(e, "select s from t where s contains 'b1' allow filtering",
                     {{ST({"a1", "b1"})}, {ST({"a2", "b1"})}});
        require_rows(e, "select s from t where s contains 'b1' and s contains '' allow filtering", {});
        require_rows(e, "select s from t where s contains 'b1' and p contains 4 allow filtering", {});
        cquery_nofail(e, "insert into t (p, c, st) values ({4}, {41}, {104})");
        require_rows(e, "select st from t where st contains 4 allow filtering", {});
        require_rows(e, "select st from t where st contains 104 allow filtering", {{SI({104})}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(list_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, s list<int>)");
        cquery_nofail(e, "insert into t (p, s) values (1, [11,12,13])");
        cquery_nofail(e, "insert into t (p, s) values (2, [21,22,23])");
        cquery_nofail(e, "insert into t (p, s) values (3, [31,32,33])");
        require_rows(e, "select p from t where s contains 222 allow filtering", {});
        const auto my_list_type = list_type_impl::get_instance(int32_type, true);
        const auto s2 = my_list_type->decompose(
                make_list_value(my_list_type, list_type_impl::native_type({21, 22, 23})));
        require_rows(e, "select p from t where s contains 22 allow filtering", {{I(2), s2}});
        require_rows(e, "select p from t where s contains 22 and s contains 23 allow filtering", {{I(2), s2}});
        require_rows(e, "select p from t where s contains 22 and s contains 32 allow filtering", {});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(map_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int primary key, m map<int,int>)");
        cquery_nofail(e, "insert into t (p, m) values (1, {1:11, 2:12})");
        require_rows(e, "select * from t where m contains 22 allow filtering", {});
        cquery_nofail(e, "insert into t (p, m) values (2, {1:21, 2:22})");
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m2 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 21}, {2, 22}})));
        require_rows(e, "select m from t where m contains 22 allow filtering", {{m2}});
        const auto m1 = my_map_type->decompose(
                make_map_value(my_map_type, map_type_impl::native_type({{1, 11}, {2, 12}})));
        require_rows(e, "select m from t where m contains 11 allow filtering", {{m1}});
        require_rows(e, "select m from t where m contains 11 and m contains 12 allow filtering", {{m1}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(clustering_key_contains) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table t (p int, s frozen<set<int>>, m frozen<map<int,int>>, primary key(p,s,m))");
        cquery_nofail(e, "insert into t (p, s, m) values (1, {11,12}, {1:11})");
        cquery_nofail(e, "insert into t (p, s, m) values (2, {21,22}, {2:12})");
        cquery_nofail(e, "insert into t (p, s, m) values (3, {31,32}, {3:13})");
        const auto my_set_type = set_type_impl::get_instance(int32_type, true);
        const auto s2 = my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type({21, 22})));
        require_rows(e, "select s from t where s contains 22 allow filtering", {{s2}});
        require_rows(e, "select s from t where s contains 44 allow filtering", {});
        const auto my_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m3 = my_map_type->decompose(make_map_value(my_map_type, map_type_impl::native_type({{3, 13}})));
        require_rows(e, "select m from t where m contains 13 allow filtering", {{m3}});
        const auto s3 = my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type({31, 32})));
        require_rows(e, "select m from t where m contains 13 and s contains 31 allow filtering", {{m3, s3}});
        cquery_nofail(e, "insert into t (p, s, m) values (4, {41,42,22}, {4:14})");
        const auto s4 = my_set_type->decompose(make_set_value(my_set_type, set_type_impl::native_type({22, 41, 42})));
        require_rows(e, "select s from t where s contains 22 allow filtering", {{s2}, {s4}});
        cquery_nofail(e, "delete from t where p=2");
        require_rows(e, "select s from t where s contains 22 allow filtering", {{s4}});
    }).get();
}

SEASTAR_THREAD_TEST_CASE(contains_key) {
    do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e,
                "create table t (p frozen<map<int,int>>, c frozen<map<text,int>>, m map<int,int>, primary key(p, c))");
        cquery_nofail(e, "insert into t (p,c,m) values ({1:11, 2:12}, {'el':11, 'twel':12}, {11:11, 12:12})");
        require_rows(e, "select * from t where p contains key 3 allow filtering", {});
        require_rows(e, "select * from t where c contains key 'x' allow filtering", {});
        require_rows(e, "select * from t where m contains key 3 allow filtering", {});
        cquery_nofail(e, "insert into t (p,c,m) values ({3:33}, {'th':33}, {11:33})");
        const auto int_map_type = map_type_impl::get_instance(int32_type, int32_type, true);
        const auto m1 = int_map_type->decompose(
                make_map_value(int_map_type, map_type_impl::native_type({{11, 11}, {12, 12}})));
        const auto m2 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{11, 33}})));
        require_rows(e, "select m from t where m contains key 12 allow filtering", {{m1}});
        require_rows(e, "select m from t where m contains key 11 allow filtering", {{m1}, {m2}});
        const auto text_map_type = map_type_impl::get_instance(utf8_type, int32_type, true);
        const auto c1 = text_map_type->decompose(
                make_map_value(text_map_type, map_type_impl::native_type({{"el", 11}, {"twel", 12}})));
        require_rows(e, "select c from t where c contains key 'el' allow filtering", {{c1}});
        require_rows(e, "select c from t where c contains key 'twel' allow filtering", {{c1}});
        const auto p3 = int_map_type->decompose(make_map_value(int_map_type, map_type_impl::native_type({{3, 33}})));
        require_rows(e, "select p from t where p contains key 3 allow filtering", {{p3}});
    }).get();
}

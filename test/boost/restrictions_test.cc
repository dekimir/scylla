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

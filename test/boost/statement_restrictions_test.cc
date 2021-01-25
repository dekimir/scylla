/*
 * Copyright (C) 2020 ScyllaDB
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


#include <seastar/testing/test_case.hh>

#include <vector>

#include "cql3/relation.hh"
#include "cql3/restrictions/statement_restrictions.hh"
#include "cql3/util.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"

using namespace cql3;
using cql3::util::where_clause_to_relations;

namespace {

auto get_clustering_bounds(
        const std::vector<relation_ptr>& where_clause, database& db,
        const sstring& table_name = "t", const sstring& keyspace_name = "ks") {
    variable_specifications bound_names;
    return restrictions::statement_restrictions(
            db,
            db.find_schema(keyspace_name, table_name),
            statements::statement_type::SELECT,
            where_clause,
            bound_names,
            /*contains_only_static_columns=*/false)
            .get_clustering_bounds(query_options({}));
}

auto I(int32_t x) { return int32_type->decompose(x); }

} // anonymous namespace

SEASTAR_TEST_CASE(slice_empty_restriction) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(p int, c int, primary key(p,c))");
        BOOST_CHECK_EQUAL(
                get_clustering_bounds(/*where_clause=*/{}, e.local_db()),
                std::vector{query::clustering_range::make_open_ended_both_sides()});
    });
}

SEASTAR_TEST_CASE(slice_one_restriction) {
    return do_with_cql_env_thread([](cql_test_env& e) {
        cquery_nofail(e, "create table ks.t(p int, c int, primary key(p,c))");
        const bool inclusive = true, exclusive = false;
        BOOST_CHECK_EQUAL(
                get_clustering_bounds(where_clause_to_relations("c>123"), e.local_db()),
                std::vector{query::clustering_range::make_starting_with({clustering_key_prefix({I(123)}), exclusive})});
    });
}

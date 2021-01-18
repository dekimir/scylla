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

#include <limits>

#include "cql3/expr/range_gen.hh"
#include "cql3/result_generator.hh"
#include "cql3/result_set.hh"
#include "cql3/selection/selection.hh"
#include "cql3/stats.hh"
#include "cql3/untyped_result_set.hh"
#include "partition_slice_builder.hh"
#include "test/lib/cql_assertions.hh"
#include "test/lib/cql_test_env.hh"
#include "transport/messages/result_message.hh"
#include "types.hh"

using namespace cql3::selection;
using namespace cql_transport::messages;
using namespace service;

// Transform a WHERE expression into a series of commands.  A command could be
// 1. a proxy query with specified ranges and slice
// 2. a proxy query with primary keys from the input
// 3. a filtering expression

namespace {

auto I(int32_t x) { return int32_type->decompose(x); }

} // anonymous namespace

SEASTAR_TEST_CASE(whole_table) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table ks.cf (p int primary key, r int)");
        cquery_nofail(e, "insert into ks.cf(p, r) values (1, 11)");
        cquery_nofail(e, "insert into ks.cf(p, r) values (2, 12)");
        cquery_nofail(e, "insert into ks.cf(p, r) values (3, 13)");
        cquery_nofail(e, "insert into ks.cf(p, r) values (4, 14)");
        auto schema = e.local_db().find_schema("ks", "cf");
        client_state state(client_state::external_tag(), e.local_auth_service());
        const auto max_size = std::numeric_limits<size_t>::max();
        auto col_p = schema->get_column_definition("p");
        auto col_r = schema->get_column_definition("r");
        query::partition_slice::option_set opts;
        opts.set(query::partition_slice::option::send_partition_key);
        const auto cmd = make_lw_shared<query::read_command>(
                schema->id(), schema->version(),
                query::partition_slice(
                        {query::clustering_range::make_open_ended_both_sides()},
                        /*static_columns=*/{},
                        /*regular_columns=*/{col_r->id},
                        opts),
                query::max_result_size(max_size), query::row_limit(1000));
        auto ranges = cql3::expr::make_partition_ranges(true);
        auto results = get_local_storage_proxy().query(
                schema,
                cmd,
                move(ranges),
                db::consistency_level::ANY,
                storage_proxy::coordinator_query_options(
                        db::no_timeout, empty_service_permit(), state)
        ).get0().query_result;
        auto sel = selection::for_columns(schema, std::vector{col_p, col_r});
        cql3::cql_stats stats;
        rows_assertions(
                make_shared<result_message::rows>(
                        cql3::result(
                                cql3::result_generator(schema, std::move(results), std::move(cmd), sel, stats),
                                ::make_shared<cql3::metadata>(*sel->get_result_metadata()))))
                .with_rows_ignore_order({{I(1), I(11)}, {I(2), I(12)}, {I(3), I(13)}, {I(4), I(14)}});
    });
}

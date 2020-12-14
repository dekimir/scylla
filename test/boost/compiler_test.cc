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

namespace {

class stub_selection : public selection {
public:
    stub_selection(schema_ptr schema,
                   std::vector<const column_definition*> columns,
                   std::vector<lw_shared_ptr<cql3::column_specification>> metadata)
        : selection(schema, std::move(columns), std::move(metadata),
                    /*collect_timestamps=*/false, /*collect_TTLs=*/false, trivial::yes)
    {}

    bool is_wildcard() const override { return false; }
    bool is_aggregate() const override { return false; }
protected:
    class stub_selectors : public selectors {
    public:
        void reset() override {};

        bool requires_thread() const override { return false; }

        std::vector<bytes_opt> get_output_row(cql_serialization_format sf) override {
            return {};
        }

        void add_input_row(cql_serialization_format sf, result_set_builder& rs) override {}

        bool is_aggregate() const { return false; }
    };

    std::unique_ptr<selectors> new_selectors() const override {
        return std::make_unique<stub_selectors>();
    }
};

} // anonymous namespace

// Transform a WHERE expression into a series of proxy queries plus a filtering expression.  A proxy query is a
// read optionally followed by an indirection.  A read is table + selection + partition range + partition
// slice.  Indirection is table + selection + reference to a prior index read.

SEASTAR_TEST_CASE(empty_expr) {
    return do_with_cql_env_thread([] (cql_test_env& e) {
        cquery_nofail(e, "create table ks.cf (p int primary key, r int)");
        cquery_nofail(e, "insert into ks.cf(p, r) values (1, 11)");
        cquery_nofail(e, "insert into ks.cf(p, r) values (2, 12)");
        cquery_nofail(e, "insert into ks.cf(p, r) values (3, 13)");
        cquery_nofail(e, "insert into ks.cf(p, r) values (4, 14)");
        auto schema = e.local_db().find_schema("ks", "cf");
        client_state state(client_state::external_tag(), e.local_auth_service());
        const auto max_size = std::numeric_limits<size_t>::max();
        const auto cmd = make_lw_shared<query::read_command>(
                schema->id(), schema->version(), partition_slice_builder(*schema).build(),
                query::max_result_size(max_size), query::row_limit(1000));
        auto ranges = cql3::expr::make_partition_ranges(true);
        auto results = get_local_storage_proxy().query(
                schema,
                cmd,
                move(ranges),
                db::consistency_level::ANY,
                storage_proxy::coordinator_query_options(
                        storage_proxy::clock_type::time_point(), empty_service_permit(), state)
        ).get0().query_result;
        BOOST_CHECK_EQUAL(results->row_count(), 0);
        auto sel = ::make_shared<stub_selection>(
                schema,
                std::vector<const column_definition*>{},
                std::vector<lw_shared_ptr<cql3::column_specification>>{});
        cql3::cql_stats stats;
        cql3::untyped_result_set rset(
                static_pointer_cast<result_message>(
                        make_shared<result_message::rows>(
                                cql3::result(
                                        cql3::result_generator(schema, std::move(results), std::move(cmd), sel, stats),
                                        ::make_shared<cql3::metadata>(*sel->get_result_metadata())))));
    });
}

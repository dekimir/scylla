
/*
 * Copyright (C) 2015 ScyllaDB
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

#include <algorithm>
#include <boost/algorithm/cxx11/all_of.hpp>
#include <boost/algorithm/cxx11/any_of.hpp>
#include <boost/range/algorithm/transform.hpp>
#include <boost/range/algorithm.hpp>
#include <boost/range/adaptors.hpp>
#include <stdexcept>

#include "query-result-reader.hh"
#include "statement_restrictions.hh"
#include "single_column_primary_key_restrictions.hh"
#include "token_restriction.hh"
#include "database.hh"

#include "cql3/constants.hh"
#include "cql3/selection/selection.hh"
#include "cql3/single_column_relation.hh"
#include "types/list.hh"
#include "types/map.hh"
#include "types/set.hh"
#include "utils/overloaded_functor.hh"

namespace cql3 {

namespace restrictions {

static logging::logger rlogger("restrictions");

using boost::adaptors::filtered;
using boost::adaptors::transformed;

template<typename T>
class statement_restrictions::initial_key_restrictions : public primary_key_restrictions<T> {
    bool _allow_filtering;
public:
    initial_key_restrictions(bool allow_filtering)
        : _allow_filtering(allow_filtering) {}
    using bounds_range_type = typename primary_key_restrictions<T>::bounds_range_type;

    ::shared_ptr<primary_key_restrictions<T>> do_merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) const {
        if (restriction->is_multi_column()) {
            throw std::runtime_error(format("{} not implemented", __PRETTY_FUNCTION__));
        }
        return ::make_shared<single_column_primary_key_restrictions<T>>(schema, _allow_filtering)->merge_to(schema, restriction);
    }
    ::shared_ptr<primary_key_restrictions<T>> merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) override {
        if (restriction->is_multi_column()) {
            throw std::runtime_error(format("{} not implemented", __PRETTY_FUNCTION__));
        }
        if (restriction->is_on_token()) {
            return static_pointer_cast<token_restriction>(restriction);
        }
        return ::make_shared<single_column_primary_key_restrictions<T>>(schema, _allow_filtering)->merge_to(restriction);
    }
    void merge_with(::shared_ptr<restriction> restriction) override {
        throw exceptions::unsupported_operation_exception();
    }
    std::vector<bytes_opt> values(const query_options& options) const override {
        // throw? should not reach?
        return {};
    }
    bytes_opt value_for(const column_definition& cdef, const query_options& options) const override {
        return {};
    }
    std::vector<T> values_as_keys(const query_options& options) const override {
        // throw? should not reach?
        return {};
    }
    std::vector<bounds_range_type> bounds_ranges(const query_options&) const override {
        // throw? should not reach?
        return {};
    }
    std::vector<const column_definition*> get_column_defs() const override {
        // throw? should not reach?
        return {};
    }
    bool uses_function(const sstring&, const sstring&) const override {
        return false;
    }
    bool empty() const override {
        return true;
    }
    uint32_t size() const override {
        return 0;
    }
    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager, allow_local_index allow_local) const override {
        return false;
    }
    sstring to_string() const override {
        return "Initial restrictions";
    }
    virtual bool is_satisfied_by(const schema& schema,
                                 const partition_key& key,
                                 const clustering_key_prefix& ckey,
                                 const row& cells,
                                 const query_options& options,
                                 gc_clock::time_point now) const override {
        return true;
    }
};

template<>
::shared_ptr<primary_key_restrictions<partition_key>>
statement_restrictions::initial_key_restrictions<partition_key>::merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) {
    if (restriction->is_on_token()) {
        return static_pointer_cast<token_restriction>(restriction);
    }
    return do_merge_to(std::move(schema), std::move(restriction));
}

template<>
::shared_ptr<primary_key_restrictions<clustering_key_prefix>>
statement_restrictions::initial_key_restrictions<clustering_key_prefix>::merge_to(schema_ptr schema, ::shared_ptr<restriction> restriction) {
    if (restriction->is_multi_column()) {
        return static_pointer_cast<primary_key_restrictions<clustering_key_prefix>>(restriction);
    }
    return do_merge_to(std::move(schema), std::move(restriction));
}

::shared_ptr<partition_key_restrictions> statement_restrictions::get_initial_partition_key_restrictions(bool allow_filtering) {
    static thread_local ::shared_ptr<partition_key_restrictions> initial_kr_true = ::make_shared<initial_key_restrictions<partition_key>>(true);
    static thread_local ::shared_ptr<partition_key_restrictions> initial_kr_false = ::make_shared<initial_key_restrictions<partition_key>>(false);
    return allow_filtering ? initial_kr_true : initial_kr_false;
}

::shared_ptr<clustering_key_restrictions> statement_restrictions::get_initial_clustering_key_restrictions(bool allow_filtering) {
    static thread_local ::shared_ptr<clustering_key_restrictions> initial_kr_true = ::make_shared<initial_key_restrictions<clustering_key>>(true);
    static thread_local ::shared_ptr<clustering_key_restrictions> initial_kr_false = ::make_shared<initial_key_restrictions<clustering_key>>(false);
    return allow_filtering ? initial_kr_true : initial_kr_false;
}

std::vector<::shared_ptr<column_identifier>>
statement_restrictions::get_partition_key_unrestricted_components() const {
    std::vector<::shared_ptr<column_identifier>> r;

    auto restricted = _partition_key_restrictions->get_column_defs();
    auto is_not_restricted = [&restricted] (const column_definition& def) {
        return !boost::count(restricted, &def);
    };

    boost::copy(_schema->partition_key_columns() | filtered(is_not_restricted) | transformed(to_identifier),
        std::back_inserter(r));
    return r;
}

statement_restrictions::statement_restrictions(schema_ptr schema, bool allow_filtering)
    : _schema(schema)
    , _partition_key_restrictions(get_initial_partition_key_restrictions(allow_filtering))
    , _clustering_columns_restrictions(get_initial_clustering_key_restrictions(allow_filtering))
    , _nonprimary_key_restrictions(::make_shared<single_column_restrictions>(schema))
{ }
#if 0
static const column_definition*
to_column_definition(const schema_ptr& schema, const ::shared_ptr<column_identifier::raw>& entity) {
    return get_column_definition(schema,
            *entity->prepare_column_identifier(schema));
}
#endif

statement_restrictions::statement_restrictions(database& db,
        schema_ptr schema,
        statements::statement_type type,
        const std::vector<::shared_ptr<relation>>& where_clause,
        variable_specifications& bound_names,
        bool selects_only_static_columns,
        bool select_a_collection,
        bool for_view,
        bool allow_filtering)
    : statement_restrictions(schema, allow_filtering)
{
    /*
     * WHERE clause. For a given entity, rules are: - EQ relation conflicts with anything else (including a 2nd EQ)
     * - Can't have more than one LT(E) relation (resp. GT(E) relation) - IN relation are restricted to row keys
     * (for now) and conflicts with anything else (we could allow two IN for the same entity but that doesn't seem
     * very useful) - The value_alias cannot be restricted in any way (we don't support wide rows with indexed value
     * in CQL so far)
     */
    if (!where_clause.empty()) {
        for (auto&& relation : where_clause) {
            if (relation->get_operator() == cql3::operator_type::IS_NOT) {
                single_column_relation* r =
                        dynamic_cast<single_column_relation*>(relation.get());
                // The "IS NOT NULL" restriction is only supported (and
                // mandatory) for materialized view creation:
                if (!r) {
                    throw exceptions::invalid_request_exception("IS NOT only supports single column");
                }
                // currently, the grammar only allows the NULL argument to be
                // "IS NOT", so this assertion should not be able to fail
                assert(r->get_value() == cql3::constants::NULL_LITERAL);

                auto col_id = r->get_entity()->prepare_column_identifier(*schema);
                const auto *cd = get_column_definition(*schema, *col_id);
                if (!cd) {
                    throw exceptions::invalid_request_exception(format("restriction '{}' unknown column {}", relation->to_string(), r->get_entity()->to_string()));
                }
                _not_null_columns.insert(cd);

                if (!for_view) {
                    throw exceptions::invalid_request_exception(format("restriction '{}' is only supported in materialized view creation", relation->to_string()));
                }
            } else {
                add_restriction(relation->to_restriction(db, schema, bound_names), for_view, allow_filtering);
            }
        }
    }
    auto& cf = db.find_column_family(schema);
    auto& sim = cf.get_index_manager();
    const allow_local_index allow_local(!_partition_key_restrictions->has_unrestricted_components(*_schema) && _partition_key_restrictions->is_all_eq());
    const bool has_queriable_clustering_column_index = _clustering_columns_restrictions->has_supporting_index(sim, allow_local);
    const bool has_queriable_pk_index = _partition_key_restrictions->has_supporting_index(sim, allow_local);
    const bool has_queriable_regular_index = _nonprimary_key_restrictions->has_supporting_index(sim, allow_local);

    // At this point, the select statement if fully constructed, but we still have a few things to validate
    process_partition_key_restrictions(has_queriable_pk_index, for_view, allow_filtering);

    // Some but not all of the partition key columns have been specified;
    // hence we need turn these restrictions into index expressions.
    if (_uses_secondary_indexing || _partition_key_restrictions->needs_filtering(*_schema)) {
        _index_restrictions.push_back(_partition_key_restrictions);
    }

    // If the only updated/deleted columns are static, then we don't need clustering columns.
    // And in fact, unless it is an INSERT, we reject if clustering columns are provided as that
    // suggest something unintended. For instance, given:
    //   CREATE TABLE t (k int, v int, s int static, PRIMARY KEY (k, v))
    // it can make sense to do:
    //   INSERT INTO t(k, v, s) VALUES (0, 1, 2)
    // but both
    //   UPDATE t SET s = 3 WHERE k = 0 AND v = 1
    //   DELETE s FROM t WHERE k = 0 AND v = 1
    // sounds like you don't really understand what your are doing.
    if (selects_only_static_columns && has_clustering_columns_restriction()) {
        if (type.is_update() || type.is_delete()) {
            throw exceptions::invalid_request_exception(format("Invalid restrictions on clustering columns since the {} statement modifies only static columns", type));
        }

        if (type.is_select()) {
            throw exceptions::invalid_request_exception(
                "Cannot restrict clustering columns when selecting only static columns");
        }
    }

    process_clustering_columns_restrictions(has_queriable_clustering_column_index, select_a_collection, for_view, allow_filtering);

    // Covers indexes on the first clustering column (among others).
    if (_is_key_range && has_queriable_clustering_column_index) {
        _uses_secondary_indexing = true;
    }

    if (_uses_secondary_indexing || _clustering_columns_restrictions->needs_filtering(*_schema)) {
        _index_restrictions.push_back(_clustering_columns_restrictions);
    } else if (_clustering_columns_restrictions->is_contains()) {
        fail(unimplemented::cause::INDEXES);
#if 0
        _index_restrictions.push_back(new Forwardingprimary_key_restrictions() {

            @Override
            protected primary_key_restrictions getDelegate()
            {
                return _clustering_columns_restrictions;
            }

            @Override
            public void add_index_expression_to(List<::shared_ptr<index_expression>> expressions, const query_options& options) throws InvalidRequestException
            {
                List<::shared_ptr<index_expression>> list = new ArrayList<>();
                super.add_index_expression_to(list, options);

                for (::shared_ptr<index_expression> expression : list)
                {
                    if (expression.is_contains() || expression.is_containsKey())
                        expressions.add(expression);
                }
            }
        });
        uses_secondary_indexing = true;
#endif
    }

    if (!_nonprimary_key_restrictions->empty()) {
        if (has_queriable_regular_index) {
            _uses_secondary_indexing = true;
        } else if (!allow_filtering) {
            throw exceptions::invalid_request_exception("Cannot execute this query as it might involve data filtering and "
                "thus may have unpredictable performance. If you want to execute "
                "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
        _index_restrictions.push_back(_nonprimary_key_restrictions);
    }

    if (_uses_secondary_indexing && !(for_view || allow_filtering)) {
        validate_secondary_index_selections(selects_only_static_columns);
    }
}

void statement_restrictions::add_restriction(::shared_ptr<restriction> restriction, bool for_view, bool allow_filtering) {
    if (restriction->is_multi_column()) {
        _clustering_columns_restrictions = _clustering_columns_restrictions->merge_to(_schema, restriction);
    } else if (restriction->is_on_token()) {
        _partition_key_restrictions = _partition_key_restrictions->merge_to(_schema, restriction);
    } else {
        add_single_column_restriction(::static_pointer_cast<single_column_restriction>(restriction), for_view, allow_filtering);
    }
}

void statement_restrictions::add_single_column_restriction(::shared_ptr<single_column_restriction> restriction, bool for_view, bool allow_filtering) {
    auto& def = restriction->get_column_def();
    if (def.is_partition_key()) {
        // A SELECT query may not request a slice (range) of partition keys
        // without using token(). This is because there is no way to do this
        // query efficiently: mumur3 turns a contiguous range of partition
        // keys into tokens all over the token space.
        // However, in a SELECT statement used to define a materialized view,
        // such a slice is fine - it is used to check whether individual
        // partitions, match, and does not present a performance problem.
        assert(!restriction->is_on_token());
        if (restriction->is_slice() && !for_view && !allow_filtering) {
            throw exceptions::invalid_request_exception(
                    "Only EQ and IN relation are supported on the partition key (unless you use the token() function or allow filtering)");
        }
        _partition_key_restrictions = _partition_key_restrictions->merge_to(_schema, restriction);
    } else if (def.is_clustering_key()) {
        _clustering_columns_restrictions = _clustering_columns_restrictions->merge_to(_schema, restriction);
    } else {
        _nonprimary_key_restrictions->add_restriction(restriction);
    }
}

bool statement_restrictions::uses_function(const sstring& ks_name, const sstring& function_name) const {
    return  _partition_key_restrictions->uses_function(ks_name, function_name)
            || _clustering_columns_restrictions->uses_function(ks_name, function_name)
            || _nonprimary_key_restrictions->uses_function(ks_name, function_name);
}

const std::vector<::shared_ptr<restrictions>>& statement_restrictions::index_restrictions() const {
    return _index_restrictions;
}

// Current score table:
// local and restrictions include full partition key: 2
// global: 1
// local and restrictions does not include full partition key: 0 (do not pick)
int statement_restrictions::score(const secondary_index::index& index) const {
    if (index.metadata().local()) {
        const bool allow_local = !_partition_key_restrictions->has_unrestricted_components(*_schema) && _partition_key_restrictions->is_all_eq();
        return  allow_local ? 2 : 0;
    }
    return 1;
}

std::pair<std::optional<secondary_index::index>, ::shared_ptr<cql3::restrictions::restrictions>> statement_restrictions::find_idx(secondary_index::secondary_index_manager& sim) const {
    std::optional<secondary_index::index> chosen_index;
    int chosen_index_score = 0;
    ::shared_ptr<cql3::restrictions::restrictions> chosen_index_restrictions;

    for (const auto& index : sim.list_indexes()) {
        for (::shared_ptr<cql3::restrictions::restrictions> restriction : index_restrictions()) {
            for (const auto& cdef : restriction->get_column_defs()) {
                if (index.depends_on(*cdef)) {
                    if (score(index) > chosen_index_score) {
                        chosen_index = index;
                        chosen_index_score = score(index);
                        chosen_index_restrictions = restriction;
                    }
                }
            }
        }
    }
    return {chosen_index, chosen_index_restrictions};
}

std::vector<const column_definition*> statement_restrictions::get_column_defs_for_filtering(database& db) const {
    std::vector<const column_definition*> column_defs_for_filtering;
    if (need_filtering()) {
        auto& sim = db.find_column_family(_schema).get_index_manager();
        auto [opt_idx, _] = find_idx(sim);
        auto column_uses_indexing = [&opt_idx] (const column_definition* cdef, ::shared_ptr<single_column_restriction> restr) {
            return opt_idx && restr && restr->is_supported_by(*opt_idx);
        };
        auto single_pk_restrs = dynamic_pointer_cast<single_column_partition_key_restrictions>(_partition_key_restrictions);
        if (_partition_key_restrictions->needs_filtering(*_schema)) {
            for (auto&& cdef : _partition_key_restrictions->get_column_defs()) {
                ::shared_ptr<single_column_restriction> restr;
                if (single_pk_restrs) {
                    auto it = single_pk_restrs->restrictions().find(cdef);
                    if (it != single_pk_restrs->restrictions().end()) {
                        restr = dynamic_pointer_cast<single_column_restriction>(it->second);
                    }
                }
                if (!column_uses_indexing(cdef, restr)) {
                    column_defs_for_filtering.emplace_back(cdef);
                }
            }
        }
        auto single_ck_restrs = dynamic_pointer_cast<single_column_clustering_key_restrictions>(_clustering_columns_restrictions);
        const bool pk_has_unrestricted_components = _partition_key_restrictions->has_unrestricted_components(*_schema);
        if (pk_has_unrestricted_components || _clustering_columns_restrictions->needs_filtering(*_schema)) {
            column_id first_filtering_id = pk_has_unrestricted_components ? 0 : _schema->clustering_key_columns().begin()->id +
                    _clustering_columns_restrictions->num_prefix_columns_that_need_not_be_filtered();
            for (auto&& cdef : _clustering_columns_restrictions->get_column_defs()) {
                ::shared_ptr<single_column_restriction> restr;
                if (single_pk_restrs) {
                    auto it = single_ck_restrs->restrictions().find(cdef);
                    if (it != single_ck_restrs->restrictions().end()) {
                        restr = dynamic_pointer_cast<single_column_restriction>(it->second);
                    }
                }
                if (cdef->id >= first_filtering_id && !column_uses_indexing(cdef, restr)) {
                    column_defs_for_filtering.emplace_back(cdef);
                }
            }
        }
        for (auto&& cdef : _nonprimary_key_restrictions->get_column_defs()) {
            auto restr = dynamic_pointer_cast<single_column_restriction>(_nonprimary_key_restrictions->get_restriction(*cdef));
            if (!column_uses_indexing(cdef, restr)) {
                column_defs_for_filtering.emplace_back(cdef);
            }
        }
    }
    return column_defs_for_filtering;
}

void statement_restrictions::process_partition_key_restrictions(bool has_queriable_index, bool for_view, bool allow_filtering) {
    // If there is a queriable index, no special condition are required on the other restrictions.
    // But we still need to know 2 things:
    // - If we don't have a queriable index, is the query ok
    // - Is it queriable without 2ndary index, which is always more efficient
    // If a component of the partition key is restricted by a relation, all preceding
    // components must have a EQ. Only the last partition key component can be in IN relation.
    if (_partition_key_restrictions->is_on_token()) {
        _is_key_range = true;
    } else if (_partition_key_restrictions->has_unrestricted_components(*_schema)) {
        _is_key_range = true;
        _uses_secondary_indexing = has_queriable_index;
    }

    if (_partition_key_restrictions->needs_filtering(*_schema)) {
        if (!allow_filtering && !for_view && !has_queriable_index) {
            throw exceptions::invalid_request_exception("Cannot execute this query as it might involve data filtering and "
                "thus may have unpredictable performance. If you want to execute "
                "this query despite the performance unpredictability, use ALLOW FILTERING");
        }
        _is_key_range = true;
        _uses_secondary_indexing = has_queriable_index;
    }

}

bool statement_restrictions::has_partition_key_unrestricted_components() const {
    return _partition_key_restrictions->has_unrestricted_components(*_schema);
}

bool statement_restrictions::has_unrestricted_clustering_columns() const {
    return _clustering_columns_restrictions->has_unrestricted_components(*_schema);
}

void statement_restrictions::process_clustering_columns_restrictions(bool has_queriable_index, bool select_a_collection, bool for_view, bool allow_filtering) {
    if (!has_clustering_columns_restriction()) {
        return;
    }

    if (_clustering_columns_restrictions->is_IN() && select_a_collection) {
        throw exceptions::invalid_request_exception(
            "Cannot restrict clustering columns by IN relations when a collection is selected by the query");
    }
    if (_clustering_columns_restrictions->is_contains() && !has_queriable_index && !allow_filtering) {
        throw exceptions::invalid_request_exception(
            "Cannot restrict clustering columns by a CONTAINS relation without a secondary index or filtering");
    }

    if (has_clustering_columns_restriction() && _clustering_columns_restrictions->needs_filtering(*_schema)) {
        if (has_queriable_index) {
            _uses_secondary_indexing = true;
        } else if (!allow_filtering && !for_view) {
            auto clustering_columns_iter = _schema->clustering_key_columns().begin();
            for (auto&& restricted_column : _clustering_columns_restrictions->get_column_defs()) {
                const column_definition* clustering_column = &(*clustering_columns_iter);
                ++clustering_columns_iter;
                if (clustering_column != restricted_column) {
                        throw exceptions::invalid_request_exception(format("PRIMARY KEY column \"{}\" cannot be restricted as preceding column \"{}\" is not restricted",
                            restricted_column->name_as_text(), clustering_column->name_as_text()));
                }
            }
        }
    }
}

dht::partition_range_vector statement_restrictions::get_partition_key_ranges(const query_options& options) const {
    if (_partition_key_restrictions->empty()) {
        return {dht::partition_range::make_open_ended_both_sides()};
    }
    if (_partition_key_restrictions->needs_filtering(*_schema)) {
        return {dht::partition_range::make_open_ended_both_sides()};
    }
    return _partition_key_restrictions->bounds_ranges(options);
}

std::vector<query::clustering_range> statement_restrictions::get_clustering_bounds(const query_options& options) const {
    if (_clustering_columns_restrictions->empty()) {
        return {query::clustering_range::make_open_ended_both_sides()};
    }
    if (_clustering_columns_restrictions->needs_filtering(*_schema)) {
        if (auto single_ck_restrictions = dynamic_pointer_cast<single_column_clustering_key_restrictions>(_clustering_columns_restrictions)) {
            return single_ck_restrictions->get_longest_prefix_restrictions()->bounds_ranges(options);
        }
        return {query::clustering_range::make_open_ended_both_sides()};
    }
    return _clustering_columns_restrictions->bounds_ranges(options);
}

bool statement_restrictions::need_filtering() const {
    uint32_t number_of_restricted_columns_for_indexing = 0;
    for (auto&& restrictions : _index_restrictions) {
        number_of_restricted_columns_for_indexing += restrictions->size();
    }

    int number_of_filtering_restrictions = _nonprimary_key_restrictions->size();
    // If the whole partition key is restricted, it does not imply filtering
    if (_partition_key_restrictions->has_unrestricted_components(*_schema) || !_partition_key_restrictions->is_all_eq()) {
        number_of_filtering_restrictions += _partition_key_restrictions->size() + _clustering_columns_restrictions->size();
    } else if (_clustering_columns_restrictions->has_unrestricted_components(*_schema)) {
        number_of_filtering_restrictions += _clustering_columns_restrictions->size() - _clustering_columns_restrictions->prefix_size();
    }
    return number_of_restricted_columns_for_indexing > 1
            || (number_of_restricted_columns_for_indexing == 0 && _partition_key_restrictions->empty() && !_clustering_columns_restrictions->empty())
            || (number_of_restricted_columns_for_indexing != 0 && _nonprimary_key_restrictions->has_multiple_contains())
            || (number_of_restricted_columns_for_indexing != 0 && !_uses_secondary_indexing)
            || (_uses_secondary_indexing && number_of_filtering_restrictions > 1);
}

void statement_restrictions::validate_secondary_index_selections(bool selects_only_static_columns) {
    if (key_is_in_relation()) {
        throw exceptions::invalid_request_exception(
            "Select on indexed columns and with IN clause for the PRIMARY KEY are not supported");
    }
    // When the user only select static columns, the intent is that we don't query the whole partition but just
    // the static parts. But 1) we don't have an easy way to do that with 2i and 2) since we don't support index on
    // static columns
    // so far, 2i means that you've restricted a non static column, so the query is somewhat non-sensical.
    if (selects_only_static_columns) {
        throw exceptions::invalid_request_exception(
            "Queries using 2ndary indexes don't support selecting only static columns");
    }
}

const single_column_restrictions::restrictions_map& statement_restrictions::get_single_column_partition_key_restrictions() const {
    static single_column_restrictions::restrictions_map empty;
    auto single_restrictions = dynamic_pointer_cast<single_column_partition_key_restrictions>(_partition_key_restrictions);
    if (!single_restrictions) {
        if (dynamic_pointer_cast<initial_key_restrictions<partition_key>>(_partition_key_restrictions)) {
            return empty;
        }
        throw std::runtime_error("statement restrictions for multi-column partition key restrictions are not implemented yet");
    }
    return single_restrictions->restrictions();
}

/**
 * @return clustering key restrictions split into single column restrictions (e.g. for filtering support).
 */
const single_column_restrictions::restrictions_map& statement_restrictions::get_single_column_clustering_key_restrictions() const {
    static single_column_restrictions::restrictions_map empty;
    auto single_restrictions = dynamic_pointer_cast<single_column_clustering_key_restrictions>(_clustering_columns_restrictions);
    if (!single_restrictions) {
        if (dynamic_pointer_cast<initial_key_restrictions<clustering_key>>(_clustering_columns_restrictions)) {
            return empty;
        }
        throw std::runtime_error("statement restrictions for multi-column partition key restrictions are not implemented yet");
    }
    return single_restrictions->restrictions();
}

static std::optional<atomic_cell_value_view> do_get_value(const schema& schema,
        const column_definition& cdef,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        gc_clock::time_point now) {
    switch(cdef.kind) {
        case column_kind::partition_key:
            return atomic_cell_value_view(key.get_component(schema, cdef.component_index()));
        case column_kind::clustering_key:
            return atomic_cell_value_view(ckey.get_component(schema, cdef.component_index()));
        default:
            auto cell = cells.find_cell(cdef.id);
            if (!cell) {
                return std::nullopt;
            }
            assert(cdef.is_atomic());
            auto c = cell->as_atomic_cell(cdef);
            return c.is_dead(now) ? std::nullopt : std::optional<atomic_cell_value_view>(c.value());
    }
}

std::optional<atomic_cell_value_view> single_column_restriction::get_value(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        gc_clock::time_point now) const {
    return do_get_value(schema, _column_def, key, ckey, cells, std::move(now));
}

bool single_column_restriction::EQ::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    auto operand = value(options);
    if (operand) {
        auto cell_value = get_value(schema, key, ckey, cells, now);
        if (!cell_value) {
            return false;
        }
        return cell_value->with_linearized([&] (bytes_view cell_value_bv) {
            return _column_def.type->compare(*operand, cell_value_bv) == 0;
        });
    }
    return false;
}

bool single_column_restriction::EQ::is_satisfied_by(bytes_view data, const query_options& options) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    auto operand = value(options);
    if (!operand) {
        throw exceptions::invalid_request_exception(format("Invalid null value for {}", _column_def.name_as_text()));
    }
    return _column_def.type->compare(*operand, data) == 0;
}

bool single_column_restriction::IN::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    auto cell_value = get_value(schema, key, ckey, cells, now);
    if (!cell_value) {
        return false;
    }
    auto operands = values(options);
  return cell_value->with_linearized([&] (bytes_view cell_value_bv) {
    return std::any_of(operands.begin(), operands.end(), [&] (auto&& operand) {
        return operand && _column_def.type->compare(*operand, cell_value_bv) == 0;
    });
  });
}

bool single_column_restriction::IN::is_satisfied_by(bytes_view data, const query_options& options) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    auto operands = values(options);
    return boost::algorithm::any_of(operands, [this, &data] (const bytes_opt& operand) {
        return operand && _column_def.type->compare(*operand, data) == 0;
    });
}

static query::range<bytes_view> to_range(const term_slice& slice, const query_options& options, const sstring& name) {
    using range_type = query::range<bytes_view>;
    auto extract_bound = [&] (statements::bound bound) -> std::optional<range_type::bound> {
        if (!slice.has_bound(bound)) {
            return { };
        }
        auto value = slice.bound(bound)->bind_and_get(options);
        if (!value) {
            throw exceptions::invalid_request_exception(format("Invalid null bound for {}", name));
        }
        auto value_view = options.linearize(*value);
        return { range_type::bound(value_view, slice.is_inclusive(bound)) };
    };
    return range_type(
        extract_bound(statements::bound::START),
        extract_bound(statements::bound::END));
}

bool single_column_restriction::slice::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    auto cell_value = get_value(schema, key, ckey, cells, now);
    if (!cell_value) {
        return false;
    }
    return cell_value->with_linearized([&] (bytes_view cell_value_bv) {
        return to_range(_slice, options, _column_def.name_as_text()).contains(
                cell_value_bv, _column_def.type->as_tri_comparator());
    });
}

bool single_column_restriction::slice::is_satisfied_by(bytes_view data, const query_options& options) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    return to_range(_slice, options, _column_def.name_as_text()).contains(
            data, _column_def.type->underlying_type()->as_tri_comparator());
}

bool single_column_restriction::contains::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (_column_def.type->is_counter()) {
        fail(unimplemented::cause::COUNTERS);
    }
    if (!_column_def.type->is_collection()) {
        return false;
    }

    auto col_type = static_cast<const collection_type_impl*>(_column_def.type.get());
    if ((!_keys.empty() || !_entry_keys.empty()) && !col_type->is_map()) {
        return false;
    }
    assert(_entry_keys.size() == _entry_values.size());

    auto&& map_key_type = col_type->name_comparator();
    auto&& element_type = col_type->is_set() ? col_type->name_comparator() : col_type->value_comparator();
    if (_column_def.type->is_multi_cell()) {
        auto cell = cells.find_cell(_column_def.id);
      return cell->as_collection_mutation().with_deserialized(*col_type, [&] (collection_mutation_view_description mv) {
        auto&& elements = mv.cells;
        auto end = std::remove_if(elements.begin(), elements.end(), [now] (auto&& element) {
            return element.second.is_dead(now);
        });
        for (auto&& value : _values) {
            auto val = value->bind_and_get(options);
            if (!val) {
                continue;
            }
            auto found = with_linearized(*val, [&] (bytes_view bv) {
              return std::find_if(elements.begin(), end, [&] (auto&& element) {
                return element.second.value().with_linearized([&] (bytes_view value_bv) {
                    return element_type->compare(value_bv, bv) == 0;
                });
              });
            });
            if (found == end) {
                return false;
            }
        }
        for (auto&& key : _keys) {
            auto k = key->bind_and_get(options);
            if (!k) {
                continue;
            }
            auto found = with_linearized(*k, [&] (bytes_view bv) {
              return std::find_if(elements.begin(), end, [&] (auto&& element) {
                return map_key_type->compare(element.first, bv) == 0;
              });
            });
            if (found == end) {
                return false;
            }
        }
        for (uint32_t i = 0; i < _entry_keys.size(); ++i) {
            auto map_key = _entry_keys[i]->bind_and_get(options);
            auto map_value = _entry_values[i]->bind_and_get(options);
            if (!map_key || !map_value) {
                continue;
            }
            auto found = with_linearized(*map_key, [&] (bytes_view map_key_bv) {
              return std::find_if(elements.begin(), end, [&] (auto&& element) {
                return map_key_type->compare(element.first, map_key_bv) == 0;
              });
            });
            if (found == end) {
                return false;
            }
            auto cmp = with_linearized(*map_value, [&] (bytes_view map_value_bv) {
              return found->second.value().with_linearized([&] (bytes_view value_bv) {
                return element_type->compare(value_bv, map_value_bv);
              });
            });
            if (cmp != 0) {
                return false;
            }
        }
        return true;
      });
    } else {
        auto cell_value = get_value(schema, key, ckey, cells, now);
        if (!cell_value) {
            return false;
        }
        return cell_value->with_linearized([&] (bytes_view cell_value_bv) {
            return is_satisfied_by(cell_value_bv, options);
        });
    }

    return true;
}

bool single_column_restriction::contains::is_satisfied_by(bytes_view collection_bv, const query_options& options) const {
    assert(_column_def.type->is_collection());
    auto col_type = static_pointer_cast<const collection_type_impl>(_column_def.type);
    if (collection_bv.empty() || ((!_keys.empty() || !_entry_keys.empty()) && !col_type->is_map())) {
        return false;
    }

    auto&& map_key_type = col_type->name_comparator();
    auto&& element_type = col_type->is_set() ? col_type->name_comparator() : col_type->value_comparator();

    auto deserialized = _column_def.type->deserialize(collection_bv);
    for (auto&& value : _values) {
        auto fragmented_val = value->bind_and_get(options);
        if (!fragmented_val) {
            continue;
        }
        const bool value_matches = with_linearized(*fragmented_val, [&] (bytes_view val) {
            auto exists_in = [&](auto&& range) {
                auto found = std::find_if(range.begin(), range.end(), [&] (auto&& element) {
                    return element_type->compare(element.serialize_nonnull(), val) == 0;
                });
                return found != range.end();
            };
            if (col_type->is_list()) {
                if (!exists_in(value_cast<list_type_impl::native_type>(deserialized))) {
                    return false;
                }
            } else if (col_type->is_set()) {
                if (!exists_in(value_cast<set_type_impl::native_type>(deserialized))) {
                    return false;
                }
            } else {
                auto data_map = value_cast<map_type_impl::native_type>(deserialized);
                if (!exists_in(data_map | boost::adaptors::transformed([] (auto&& p) { return p.second; }))) {
                    return false;
                }
            }
            return true;
        });
        if (!value_matches) {
            return false;
        }
    }
    if (col_type->is_map()) {
        auto& data_map = value_cast<map_type_impl::native_type>(deserialized);
        for (auto&& key : _keys) {
            auto k = key->bind_and_get(options);
            if (!k) {
                continue;
            }
            auto found = with_linearized(*k, [&] (bytes_view k_bv) {
              return std::find_if(data_map.begin(), data_map.end(), [&] (auto&& element) {
                return map_key_type->compare(element.first.serialize_nonnull(), k_bv) == 0;
              });
            });
            if (found == data_map.end()) {
                return false;
            }
        }
        for (uint32_t i = 0; i < _entry_keys.size(); ++i) {
            auto map_key = _entry_keys[i]->bind_and_get(options);
            auto map_value = _entry_values[i]->bind_and_get(options);
            if (!map_key || !map_value) {
                throw exceptions::invalid_request_exception(
                        format("Unsupported null map {} for column {}",
                               map_key ? "key" : "value", _column_def.name_as_text()));
            }
            auto found = with_linearized(*map_key, [&] (bytes_view map_key_bv) {
              return std::find_if(data_map.begin(), data_map.end(), [&] (auto&& element) {
                return map_key_type->compare(element.first.serialize_nonnull(), map_key_bv) == 0;
              });
            });
            if (found == data_map.end()
                || with_linearized(*map_value, [&] (bytes_view map_value_bv) {
                     return element_type->compare(found->second.serialize_nonnull(), map_value_bv);
                   }) != 0) {
                return false;
            }
        }
    }

    return true;
}

bool token_restriction::EQ::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    bool satisfied = false;
    auto cdef = _column_definitions.begin();
    for (auto&& operand : values(options)) {
        if (operand) {
            auto cell_value = do_get_value(schema, **cdef, key, ckey, cells, now);
            satisfied = cell_value && cell_value->with_linearized([&] (bytes_view cell_value_bv) {
                return (*cdef)->type->compare(*operand, cell_value_bv) == 0;
            });
        }
        if (!satisfied) {
            break;
        }
    }
    return satisfied;
}

bool token_restriction::slice::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    bool satisfied = false;
    auto range = to_range(_slice, options, "token");
    for (auto* cdef : _column_definitions) {
        auto cell_value = do_get_value(schema, *cdef, key, ckey, cells, now);
        if (!cell_value) {
            return false;
        }
        satisfied = cell_value->with_linearized([&] (bytes_view cell_value_bv) {
            return range.contains(cell_value_bv, cdef->type->as_tri_comparator());
        });
        if (!satisfied) {
            break;
        }
    }
    return satisfied;
}

bool single_column_restriction::LIKE::init_matchers(const query_options& options) const {
    for (size_t i = 0; i < _values.size(); ++i) {
        auto pattern = to_bytes_opt(_values[i]->bind_and_get(options));
        if (!pattern) {
            return false;
        }
        if (i < _matchers.size()) {
            _matchers[i].reset(*pattern);
        } else {
            _matchers.emplace_back(*pattern);
        }
    }
    return true;
}

bool single_column_restriction::LIKE::is_satisfied_by(const schema& schema,
        const partition_key& key,
        const clustering_key_prefix& ckey,
        const row& cells,
        const query_options& options,
        gc_clock::time_point now) const {
    if (!_column_def.type->is_string()) {
        throw exceptions::invalid_request_exception("LIKE is allowed only on string types");
    }
    auto cell_value = get_value(schema, key, ckey, cells, now);
    if (!cell_value) {
        return false;
    }
    if (!init_matchers(options)) {
        return false;
    }
    return cell_value->with_linearized([&] (bytes_view data) {
        return boost::algorithm::all_of(_matchers, [&] (auto& m) { return m(data); });
    });
}

bool single_column_restriction::LIKE::is_satisfied_by(bytes_view data, const query_options& options) const {
    if (!_column_def.type->is_string()) {
        throw exceptions::invalid_request_exception("LIKE is allowed only on string types");
    }
    if (!init_matchers(options)) {
        return false;
    }
    return boost::algorithm::all_of(_matchers, [&] (auto& m) { return m(data); });
}

sstring single_column_restriction::LIKE::to_string() const {
    std::vector<sstring> vs(_values.size());
    for (size_t i = 0; i < _values.size(); ++i) {
        vs[i] = _values[i]->to_string();
    }
    return join(" AND ", vs);
}

void single_column_restriction::LIKE::merge_with(::shared_ptr<restriction> rest) {
    if (auto other = dynamic_pointer_cast<LIKE>(rest)) {
        boost::copy(other->_values, back_inserter(_values));
    } else {
        throw exceptions::invalid_request_exception(
                format("{} cannot be restricted by both LIKE and non-LIKE restrictions", _column_def.name_as_text()));
    }
}

::shared_ptr<single_column_restriction> single_column_restriction::LIKE::apply_to(const column_definition& cdef) {
    auto r = ::make_shared<LIKE>(cdef, _values[0]);
    std::copy(_values.cbegin() + 1, _values.cend(), back_inserter(r->_values));
    return r;
}

namespace wip {

namespace {

using children_t = std::vector<expression>; // conjunction's children.

children_t explode_conjunction(const expression& e) {
    return std::visit(overloaded_functor{
            [] (const conjunction& c) { return c.children; },
            [&] (const auto&) { return children_t{e}; },
        }, e);
}

} // anonymous namespace

expression make_conjunction(const expression& a, const expression& b) {
    auto children = explode_conjunction(a);
    boost::copy(explode_conjunction(b), back_inserter(children));
    return conjunction{children};
}

namespace {

using cql3::selection::selection;

/// Serialized values for all types of cells.
struct row_data {
    const std::vector<bytes>& partition_key;
    const std::vector<bytes>& clustering_key;
    const std::vector<bytes_opt>& other_columns;
};

/// Returns col's value from the fetched data.
bytes_opt get_value(const column_value& col, const selection& selection, row_data cells,
                    const query_options& options) {
    auto cdef = col.col;
    if (col.sub) {
        auto col_type = static_pointer_cast<const collection_type_impl>(cdef->type);
        if (!col_type->is_map()) {
            throw exceptions::invalid_request_exception("subscripting non-map column");
        }
        const auto deserialized = cdef->type->deserialize(*cells.other_columns[selection.index_of(*cdef)]);
        const auto& data_map = value_cast<map_type_impl::native_type>(deserialized);
        const auto key = col.sub->bind_and_get(options);
        auto&& key_type = col_type->name_comparator();
        const auto found = with_linearized(*key, [&] (bytes_view key_bv) {
            return std::find_if(data_map.cbegin(), data_map.cend(), [&] (auto&& element) {
                return key_type->compare(element.first.serialize_nonnull(), key_bv) == 0;
            });
        });
        return found == data_map.cend() ? bytes_opt() : bytes_opt(found->second.serialize_nonnull());
    } else {
        switch (cdef->kind) {
        case column_kind::partition_key:
            return cells.partition_key[cdef->id];
        case column_kind::clustering_key:
            return cells.clustering_key[cdef->id];
        case column_kind::static_column:
        case column_kind::regular_column:
            return cells.other_columns[selection.index_of(*cdef)];
        default:
            throw exceptions::unsupported_operation_exception("Unknown column kind");
        }
    }
}

/// The comparator type for cv.
data_type comparator(const column_value& cv) {
    return cv.sub ?
            static_pointer_cast<const collection_type_impl>(cv.col->type)->value_comparator() : cv.col->type;
}

/// True iff lhs's value equals rhs.
bool equal(const bytes_opt& rhs, const column_value& lhs,
           const selection& selection, row_data cells, const query_options& options) {
    if (!rhs) {
        return false;
    }
    const auto value = get_value(lhs, selection, cells, options);
    if (!value) {
        return false;
    }
    return comparator(lhs)->equal(*value, *rhs);
}

/// True iff columns' values equal t.
bool equal(::shared_ptr<term> t, const std::vector<column_value>& columns, const selection& selection,
           row_data cells, const query_options& options) {
    if (columns.size() > 1) {
        auto multi = dynamic_pointer_cast<multi_item_terminal>(t);
        if (!multi) {
            throw std::logic_error("RHS for multi-column is not a tuple");
        }
        const auto& rhs = multi->get_elements();
        if (rhs.size() != columns.size()) {
            throw std::logic_error("LHS and RHS size mismatch");
        }
        return boost::equal(rhs, columns, [&] (const bytes_opt& rhs, const column_value& lhs) {
            return equal(rhs, lhs, selection, cells, options);
        });
    } else if (columns.size() == 1) {
        return equal(to_bytes_opt(t->bind_and_get(options)), columns[0], selection, cells, options);
    } else {
        throw std::logic_error("empty tuple on LHS of =");
    }
}

/// True iff lhs is limited by rhs in the manner prescribed by op.
bool limits(bytes_view lhs, const operator_type& op, bytes_view rhs, const abstract_type& type) {
    if (!op.is_compare()) {
        throw std::logic_error("limits() called on non-compare op");
    }
    const auto cmp = type.as_tri_comparator()(lhs, rhs);
    if (cmp < 0) {
        return op == operator_type::LT || op == operator_type::LTE || op == operator_type::NEQ;
    } else if (cmp > 0) {
        return op == operator_type::GT || op == operator_type::GTE || op == operator_type::NEQ;
    } else {
        return op == operator_type::LTE || op == operator_type::GTE || op == operator_type::EQ;
    }
}

/// True iff the value of opr.lhs is limited by opr.rhs in the manner prescribed by opr.op.
bool limits(const binary_operator& opr, const selection& selection, row_data cells,
            const query_options& options) {
    if (!opr.op->is_slice()) { // For EQ or NEQ, use equals().
        throw std::logic_error("limits() called on non-slice op");
    }
    const auto& columns = std::get<0>(opr.lhs);
    if (columns.size() > 1) {
        auto multi = dynamic_pointer_cast<multi_item_terminal>(opr.rhs);
        if (!multi) {
            throw std::logic_error("RHS for multi-column is not a tuple");
        }
        const auto& rhs = multi->get_elements();
        if (rhs.size() != columns.size()) {
            throw std::logic_error("LHS and RHS size mismatch");
        }
        for (size_t i = 0; i < rhs.size(); ++i) {
            const auto cmp = comparator(columns[i])->as_tri_comparator()(
                    // CQL dictates that columns[i] is a clustering column and non-null.
                    *get_value(columns[i], selection, cells, options),
                    *rhs[i]);
            // If the components aren't equal, then we just learned the LHS/RHS order.
            if (cmp < 0) {
                if (*opr.op == operator_type::LT || *opr.op == operator_type::LTE) {
                    return true;
                } else if (*opr.op == operator_type::GT || *opr.op == operator_type::GTE) {
                    return false;
                } else {
                    throw std::logic_error("Unknown slice operator");
                }
            } else if (cmp > 0) {
                if (*opr.op == operator_type::LT || *opr.op == operator_type::LTE) {
                    return false;
                } else if (*opr.op == operator_type::GT || *opr.op == operator_type::GTE) {
                    return true;
                } else {
                    throw std::logic_error("Unknown slice operator");
                }
            }
            // Otherwise, we don't know the LHS/RHS order, so check the next component.
        }
        // Getting here means LHS == RHS.
        return *opr.op == operator_type::LTE || *opr.op == operator_type::GTE;
    } else if (columns.size() == 1) {
        auto lhs = get_value(columns[0], selection, cells, options);
        if (!lhs) {
            lhs = bytes(); // Compatible with old code, which feeds null to type comparators.
        }
        auto rhs = to_bytes_opt(opr.rhs->bind_and_get(options));
        if (!rhs) {
            return false;
        }
        return limits(*lhs, *opr.op, *rhs, *columns[0].col->type);
    } else {
        throw std::logic_error("empty tuple on LHS of an inequality");
    }
}

/// True iff collection (list, set, or map) contains value.
bool contains(const data_value& collection, const raw_value_view& value) {
    if (!value) {
        return true; // Compatible with old code, which skips null terms in value comparisons.
    }
    auto col_type = static_pointer_cast<const collection_type_impl>(collection.type());
    auto&& element_type = col_type->is_set() ? col_type->name_comparator() : col_type->value_comparator();
    return with_linearized(*value, [&] (bytes_view val) {
        auto exists_in = [&](auto&& range) {
            auto found = std::find_if(range.begin(), range.end(), [&] (auto&& element) {
                return element_type->compare(element.serialize_nonnull(), val) == 0;
            });
            return found != range.end();
        };
        if (col_type->is_list()) {
            if (!exists_in(value_cast<list_type_impl::native_type>(collection))) {
                return false;
            }
        } else if (col_type->is_set()) {
            if (!exists_in(value_cast<set_type_impl::native_type>(collection))) {
                return false;
            }
        } else {
            auto data_map = value_cast<map_type_impl::native_type>(collection);
            using entry = std::pair<data_value, data_value>;
            if (!exists_in(data_map | boost::adaptors::transformed([] (const entry& e) { return e.second; }))) {
                return false;
            }
        }
        return true;
    });
}

/// True iff columns is a single collection containing value.
bool contains(const raw_value_view& value, const std::vector<column_value>& columns, const selection& selection,
              row_data cells, const query_options& options) {
    if (columns.size() != 1) {
        throw exceptions::unsupported_operation_exception("tuple CONTAINS not allowed");
    }
    if (columns[0].sub) {
        throw exceptions::unsupported_operation_exception("CONTAINS lhs is subscripted");
    }
    const auto collection = get_value(columns[0], selection, cells, options);
    if (collection) {
        return contains(columns[0].col->type->deserialize(*collection), value);
    } else {
        return false;
    }
}

/// True iff \p columns has a single element that's a map containing \p key.
bool contains_key(const std::vector<column_value>& columns, cql3::raw_value_view key,
                  const selection& selection, row_data cells, const query_options& options) {
    if (columns.size() != 1) {
        throw exceptions::unsupported_operation_exception("CONTAINS KEY on a tuple");
    }
    if (columns[0].sub) {
        throw exceptions::unsupported_operation_exception("CONTAINS KEY lhs is subscripted");
    }
    if (!key) {
        return true; // Compatible with old code, which skips null terms in key comparisons.
    }
    auto cdef = columns[0].col;
    const auto collection = get_value(columns[0], selection, cells, options);
    if (!collection) {
        return false;
    }
    const auto data_map = value_cast<map_type_impl::native_type>(cdef->type->deserialize(*collection));
    auto key_type = static_pointer_cast<const collection_type_impl>(cdef->type)->name_comparator();
    auto found = with_linearized(*key, [&] (bytes_view k_bv) {
        using entry = std::pair<data_value, data_value>;
        return std::find_if(data_map.begin(), data_map.end(), [&] (const entry& element) {
            return key_type->compare(element.first.serialize_nonnull(), k_bv) == 0;
        });
    });
    return found != data_map.end();
}

/// Fetches the next cell value from iter and returns its value as bytes_opt if the cell is atomic;
/// otherwise, returns nullopt.
bytes_opt next_value(query::result_row_view::iterator_type& iter, const column_definition* cdef) {
    if (cdef->type->is_multi_cell()) {
        auto cell = iter.next_collection_cell();
        if (cell) {
            return cell->with_linearized([] (bytes_view data) {
                return bytes(data.cbegin(), data.cend());
            });
        }
    } else {
        auto cell = iter.next_atomic_cell();
        if (cell) {
            return cell->value().with_linearized([] (bytes_view data) {
                return bytes(data.cbegin(), data.cend());
            });
        }
    }
    return std::nullopt;
}

/// Returns values of non-primary-key columns from selection.  The kth element of the result
/// corresponds to the kth column in selection.
std::vector<bytes_opt> get_non_pk_values(const selection& selection, const query::result_row_view& static_row,
                                         const query::result_row_view* row) {
    const auto& cols = selection.get_columns();
    std::vector<bytes_opt> vals(cols.size());
    auto static_row_iterator = static_row.iterator();
    auto row_iterator = row ? std::optional<query::result_row_view::iterator_type>(row->iterator()) : std::nullopt;
    for (size_t i = 0; i < cols.size(); ++i) {
        // TODO: use do_get_value() here.
        switch (cols[i]->kind) {
        case column_kind::static_column:
            vals[i] = next_value(static_row_iterator, cols[i]);
            break;
        case column_kind::regular_column:
            if (row) {
                vals[i] = next_value(*row_iterator, cols[i]);
            }
            break;
        default: // Skip.
            break;
        }
    }
    return vals;
}

/// WIP equivalent of restriction::is_satisfied_by.
bool is_satisfied_by(
        const expression& restr,
        const std::vector<bytes>& partition_key, const std::vector<bytes>& clustering_key,
        const query::result_row_view& static_row, const query::result_row_view* row,
        const selection& selection, const query_options& options) {
    return std::visit(overloaded_functor{
            [&] (bool v) { return v; },
            [&] (const conjunction& conj) {
                return boost::algorithm::all_of(conj.children, [&] (const expression& c) {
                    return is_satisfied_by(c, partition_key, clustering_key, static_row, row, selection, options);
                });
            },
            [&] (const binary_operator& opr) {
                return std::visit(overloaded_functor{
                        [&] (const std::vector<column_value>& cvs) {
                            row_data cells{
                                partition_key, clustering_key,
                                get_non_pk_values(selection, static_row, row)
                            };
                            if (*opr.op == operator_type::EQ) {
                                return equal(opr.rhs, cvs, selection, cells, options);
                            } else if (*opr.op == operator_type::NEQ) {
                                return !equal(opr.rhs, cvs, selection, cells, options);
                            } else if (opr.op->is_slice()) {
                                return limits(opr, selection, cells, options);
                            } else if (*opr.op == operator_type::CONTAINS) {
                                return contains(opr.rhs->bind_and_get(options), cvs,
                                                selection, cells, options);
                            } else if (*opr.op == operator_type::CONTAINS_KEY) {
                                return contains_key(std::get<0>(opr.lhs), opr.rhs->bind_and_get(options),
                                                    selection, cells, options);
                            } else {
                                throw exceptions::unsupported_operation_exception("Unhandled wip::binary_operator");
                            }
                        },
                        // TODO: implement.
                        [] (const token& tok) -> bool {
                            throw exceptions::unsupported_operation_exception("wip::token");
                        },
                    }, opr.lhs);
            },
        }, restr);
}

} // anonymous namespace

void check_is_satisfied_by(
        const expression& restr,
        const std::vector<bytes>& partition_key, const std::vector<bytes>& clustering_key,
        const query::result_row_view& static_row, const query::result_row_view* row,
        const selection& selection, const query_options& options,
        bool expected) {
    if (!options.get_cql_config().restrictions.use_wip) {
        return;
    }
    if (expected != is_satisfied_by(restr, partition_key, clustering_key, static_row, row, selection, options)) {
        throw std::logic_error("WIP restrictions mismatch");
    }
}

} // namespace wip

} // namespace restrictions
} // namespace cql3

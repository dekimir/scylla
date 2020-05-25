
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
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm.hpp>
#include <functional>
#include <optional>
#include <stdexcept>

#include "query-result-reader.hh"
#include "statement_restrictions.hh"
#include "single_column_primary_key_restrictions.hh"
#include "token_restriction.hh"
#include "database.hh"

#include "cql3/constants.hh"
#include "cql3/lists.hh"
#include "cql3/selection/selection.hh"
#include "cql3/single_column_relation.hh"
#include "cql3/tuples.hh"
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
        : _allow_filtering(allow_filtering) {
        this->expression = wip::conjunction{};
    }
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
    bytes_opt value_for(const column_definition& cdef, const query_options& options) const override {
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
        expression = make_conjunction(std::move(expression), rest->expression);
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

children_t explode_conjunction(expression e) {
    return std::visit(overloaded_functor{
            [] (const conjunction& c) { return std::move(c.children); },
            [&] (const auto&) { return children_t{std::move(e)}; },
        }, e);
}

using cql3::selection::selection;

/// Serialized values for all types of cells, plus selection (to find a column's index) and options (for
/// subscript term's value).
struct row_data_from_partition_slice {
    const std::vector<bytes>& partition_key;
    const std::vector<bytes>& clustering_key;
    const std::vector<bytes_opt>& other_columns;
    const selection& sel;
};

/// Data used to derive cell values from a mutation.
struct row_data_from_mutation {
    // Underscores avoid name clashes.
    const partition_key& partition_key_;
    const clustering_key_prefix& clustering_key_;
    const row& other_columns;
    const schema& schema_;
    gc_clock::time_point now;
};

/// Everything needed to compute column values during restriction evaluation.
struct column_value_eval_bag {
    const query_options& options; // For evaluating subscript terms.
    std::variant<row_data_from_partition_slice, row_data_from_mutation> row_data;
};

/// Returns col's value from queried data.
bytes_opt get_value_from_partition_slice(
        const column_value& col, row_data_from_partition_slice data, const query_options& options) {
    auto cdef = col.col;
    if (col.sub) {
        auto col_type = static_pointer_cast<const collection_type_impl>(cdef->type);
        if (!col_type->is_map()) {
            throw exceptions::invalid_request_exception(format("subscripting non-map column {}", cdef->name_as_text()));
        }
        const auto deserialized = cdef->type->deserialize(*data.other_columns[data.sel.index_of(*cdef)]);
        const auto& data_map = value_cast<map_type_impl::native_type>(deserialized);
        const auto key = col.sub->bind_and_get(options);
        auto&& key_type = col_type->name_comparator();
        const auto found = with_linearized(*key, [&] (bytes_view key_bv) {
            using entry = std::pair<data_value, data_value>;
            return std::find_if(data_map.cbegin(), data_map.cend(), [&] (const entry& element) {
                return key_type->compare(element.first.serialize_nonnull(), key_bv) == 0;
            });
        });
        return found == data_map.cend() ? bytes_opt() : bytes_opt(found->second.serialize_nonnull());
    } else {
        switch (cdef->kind) {
        case column_kind::partition_key:
            return data.partition_key[cdef->id];
        case column_kind::clustering_key:
            return data.clustering_key[cdef->id];
        case column_kind::static_column:
        case column_kind::regular_column:
            return data.other_columns[data.sel.index_of(*cdef)];
        default:
            throw exceptions::unsupported_operation_exception("Unknown column kind");
        }
    }
}

/// Returns col's value from a mutation.
bytes_opt get_value_from_mutation(const column_value& col, row_data_from_mutation data) {
    const auto v = do_get_value(
            data.schema_, *col.col, data.partition_key_, data.clustering_key_, data.other_columns, data.now);
    return v ? v->linearize() : bytes_opt();
}

/// Returns col's value from the fetched data.
bytes_opt get_value(const column_value& col, column_value_eval_bag bag) {
    using std::placeholders::_1;
    return std::visit(overloaded_functor{
            std::bind(get_value_from_mutation, col, _1),
            std::bind(get_value_from_partition_slice, col, _1, bag.options),
        }, bag.row_data);
}

/// Type for comparing results of get_value().
const abstract_type* get_value_comparator(const column_value& cv) {
    auto col_type = cv.col->type;
    if (cv.sub) {
        return static_pointer_cast<const collection_type_impl>(col_type)->value_comparator().get();
    }
    return col_type->is_reversed() ? col_type->underlying_type().get() : col_type.get();
}

/// Returns a tuple-valued terminal from t, if possible.  Otherwise, returns null.
::shared_ptr<terminal> get_tuple(const ::shared_ptr<term>& t, const query_options& opts) {
    auto tml = dynamic_pointer_cast<terminal>(t);
    if (tml) {
        return tml;
    }
    auto marker = dynamic_pointer_cast<tuples::marker>(t);
    if (marker) {
        return marker->bind(opts);
    }
    auto delayed = dynamic_pointer_cast<tuples::delayed_value>(t);
    if (delayed) {
        return delayed->bind(opts);
    }
    return nullptr;
}

/// True iff lhs's value equals rhs.
bool equal(const bytes_opt& rhs, const column_value& lhs, column_value_eval_bag bag) {
    if (!rhs) {
        return false;
    }
    const auto value = get_value(lhs, bag);
    if (!value) {
        return false;
    }
    return get_value_comparator(lhs)->equal(*value, *rhs);
}

/// True iff columns' values equal t.
bool equal(::shared_ptr<term> t, const std::vector<column_value>& columns, column_value_eval_bag bag) {
    if (columns.size() > 1) {
        auto multi = dynamic_pointer_cast<multi_item_terminal>(get_tuple(t, bag.options));
        if (!multi) {
            throw exceptions::invalid_request_exception("multi-column equality has right-hand side that isn't a tuple");
        }
        const auto& rhs = multi->get_elements();
        if (rhs.size() != columns.size()) {
            throw exceptions::invalid_request_exception(
                    format("tuple equality size mismatch: {} elements on left-hand side, {} on right",
                           columns.size(), rhs.size()));
        }
        return boost::equal(rhs, columns, [&] (const bytes_opt& rhs, const column_value& lhs) {
            return equal(rhs, lhs, bag);
        });
    } else if (columns.size() == 1) {
        auto tup = dynamic_pointer_cast<tuples::value>(get_tuple(t, bag.options));
        if (tup && tup->size() == 1) {
            // Assume this is an external query WHERE (ck1)=(123), rather than an internal query WHERE
            // col=(123), because internal queries have no reason to use single-element tuples.
            //
            // TODO: make the two cases distinguishable.
            return equal(tup->get_elements()[0], columns[0], bag);
        }
        return equal(to_bytes_opt(t->bind_and_get(bag.options)), columns[0], bag);
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

/// True iff the value of opr.lhs (which must be column_values) is limited by opr.rhs in the manner prescribed
/// by opr.op.
bool limits(const binary_operator& opr, column_value_eval_bag bag) {
    if (!opr.op->is_slice()) { // For EQ or NEQ, use equal().
        throw std::logic_error("limits() called on non-slice op");
    }
    const auto& columns = std::get<0>(opr.lhs);
    if (columns.size() > 1) {
        auto multi = dynamic_pointer_cast<multi_item_terminal>(get_tuple(opr.rhs, bag.options));
        if (!multi) {
            throw exceptions::invalid_request_exception("multi-column comparison has right-hand side that isn't a tuple");
        }
        const auto& rhs = multi->get_elements();
        if (rhs.size() != columns.size()) {
            throw exceptions::invalid_request_exception(
                    format("tuple comparison size mismatch: {} elements on left-hand side, {} on right",
                           columns.size(), rhs.size()));
        }
        for (size_t i = 0; i < rhs.size(); ++i) {
            const auto cmp = get_value_comparator(columns[i])->as_tri_comparator()(
                    // CQL dictates that columns[i] is a clustering column and non-null.
                    *get_value(columns[i], bag),
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
        auto lhs = get_value(columns[0], bag);
        if (!lhs) {
            lhs = bytes(); // Compatible with old code, which feeds null to type comparators.
        }
        auto tup = dynamic_pointer_cast<tuples::value>(get_tuple(opr.rhs, bag.options));
        // Assume this is an external query WHERE (ck1)>(123), rather than an internal query WHERE col>(123),
        // because internal queries have no reason to use single-element tuples.
        //
        // TODO: make the two cases distinguishable.
        auto rhs = (tup && tup->size() == 1) ? tup->get_elements()[0]
                : to_bytes_opt(opr.rhs->bind_and_get(bag.options));
        if (!rhs) {
            return false;
        }
        return limits(*lhs, *opr.op, *rhs, *get_value_comparator(columns[0]));
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
            return exists_in(value_cast<list_type_impl::native_type>(collection));
        } else if (col_type->is_set()) {
            return exists_in(value_cast<set_type_impl::native_type>(collection));
        } else if (col_type->is_map()) {
            auto data_map = value_cast<map_type_impl::native_type>(collection);
            using entry = std::pair<data_value, data_value>;
            return exists_in(data_map | transformed([] (const entry& e) { return e.second; }));
        } else {
            throw std::logic_error("unsupported collection type in a CONTAINS expression");
        }
    });
}

/// True iff columns is a single collection containing value.
bool contains(const raw_value_view& value, const std::vector<column_value>& columns, column_value_eval_bag bag) {
    if (columns.size() != 1) {
        throw exceptions::unsupported_operation_exception("tuple CONTAINS not allowed");
    }
    if (columns[0].sub) {
        throw exceptions::unsupported_operation_exception("CONTAINS lhs is subscripted");
    }
    const auto collection = get_value(columns[0], bag);
    if (collection) {
        return contains(columns[0].col->type->deserialize(*collection), value);
    } else {
        return false;
    }
}

/// True iff \p columns has a single element that's a map containing \p key.
bool contains_key(const std::vector<column_value>& columns, cql3::raw_value_view key, column_value_eval_bag bag) {
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
    const auto collection = get_value(columns[0], bag);
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

/// Fetches the next cell value from iter and returns its (possibly null) value.
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

/// True iff cv matches the CQL LIKE pattern.
bool like(const column_value& cv, const bytes_opt& pattern, column_value_eval_bag bag) {
    if (!cv.col->type->is_string()) {
        throw exceptions::invalid_request_exception(
                format("LIKE is allowed only on string types, which {} is not", cv.col->name_as_text()));
    }
    auto value = get_value(cv, bag);
    return (pattern && value) ? like_matcher(*pattern)(*value) : false;
}

/// True iff columns' values match rhs pattern(s) as defined by CQL LIKE.
bool like(const std::vector<column_value>& columns, term& rhs, column_value_eval_bag bag) {
    // TODO: reuse matchers.
    if (columns.size() > 1) {
        if (auto multi = dynamic_cast<multi_item_terminal*>(&rhs)) {
            const auto& elements = multi->get_elements();
            if (elements.size() != columns.size()) {
                throw exceptions::invalid_request_exception(
                        format("LIKE tuple size mismatch: {} elements on left-hand side, {} on right",
                               columns.size(), elements.size()));
            }
            return boost::equal(columns, elements, [&] (const column_value& cv, const bytes_opt& pattern) {
                return like(cv, pattern, bag);
            });
        } else {
            throw exceptions::invalid_request_exception("multi-column LIKE has right-hand side that isn't a tuple");
        }
    } else if (columns.size() == 1) {
        return like(columns[0], to_bytes_opt(rhs.bind_and_get(bag.options)), bag);
    } else {
        throw exceptions::invalid_request_exception("empty tuple on left-hand side of LIKE");
    }
}

/// True iff the tuple of column values is in the set defined by rhs.
bool is_one_of(const std::vector<column_value>& cvs, term& rhs, column_value_eval_bag bag) {
    if (auto dv = dynamic_cast<lists::delayed_value*>(&rhs)) {
        return boost::algorithm::any_of(dv->get_elements(), [&] (const ::shared_ptr<term>& t) {
                return equal(t, cvs, bag);
            });
    } else if (auto mkr = dynamic_cast<lists::marker*>(&rhs)) {
        auto multi = dynamic_pointer_cast<multi_item_terminal>(mkr->bind(bag.options));
        if (multi) {
            if (cvs.size() != 1) {
                throw std::logic_error("too many columns for lists::marker in is_one_of");
            }
            return boost::algorithm::any_of(multi->get_elements(), [&] (const bytes_opt& b) {
                    return equal(b, cvs[0], bag);
                });
        }
    } else if (auto mkr = dynamic_cast<tuples::in_marker*>(&rhs)) {
        const auto val = dynamic_pointer_cast<tuples::in_value>(mkr->bind(bag.options));
        if (!val) {
            return false;
        }
        return boost::algorithm::any_of(val->get_split_values(), [&] (const std::vector<bytes_opt>& el) {
                return boost::equal(cvs, el, [&] (const column_value& c, const bytes_opt& b) {
                    return equal(b, c, bag);
                });
            });
    }
    throw std::logic_error("unexpected term type in is_one_of");
}

/// True iff op means bnd type of bound.
bool matches(const operator_type* op, statements::bound bnd) {
    static const std::vector<std::vector<const operator_type*>> operators{
        {&operator_type::EQ, &operator_type::GT, &operator_type::GTE}, // These mean a lower bound.
        {&operator_type::EQ, &operator_type::LT, &operator_type::LTE}, // These mean an upper bound.
    };
    const auto zero_if_lower_one_if_upper = get_idx(bnd);
    return boost::algorithm::any_of_equal(operators[zero_if_lower_one_if_upper], op);
}

/// Finds the first multi-column binary_operator in restr that represents bnd and returns its RHS value.  If
/// no such binary_operator exists, returns an empty vector.  The search is depth first.
std::vector<bytes_opt> first_multicolumn_bound(
        const expression& restr, const query_options& options, statements::bound bnd) {
    std::vector<bytes_opt> empty;
    return std::visit(overloaded_functor{
            [&] (const conjunction& conj) {
                for (const auto& c : conj.children) {
                    auto cb = first_multicolumn_bound(c, options, bnd);
                    if (!cb.empty()) {
                        return cb;
                    }
                }
                return empty;
            },
            [&] (const binary_operator& opr) {
                if (!matches(opr.op, bnd) || !std::holds_alternative<std::vector<column_value>>(opr.lhs)) {
                    return empty;
                }
                auto cvs = std::get<std::vector<column_value>>(opr.lhs);
                auto value = static_pointer_cast<tuples::value>(opr.rhs->bind(options));
                return value->get_elements();
            },
            [&] (auto& others) { return empty; },
        }, restr);
}

const value_set empty_value_set = value_list{};
const value_set unbounded_value_set = value_interval{};

struct intersection_visitor {
    value_set operator()(const value_list& a, const value_list& b) const {
        value_list common;
        boost::set_intersection(a, b, back_inserter(common));
        return std::move(common);
    }

    value_set operator()(const value_interval& a, const value_list& b) const {
        const auto common = b | boost::adaptors::filtered([&] (const bytes& el) { return a.includes(el); });
        return value_list(common.begin(), common.end());
    }

    value_set operator()(const value_list& a, const value_interval& b) const {
        return (*this)(b, a);
    }

    value_set operator()(const value_interval& a, const value_interval& b) const {
        const auto newlb = std::max(a.lb, b.lb); // nullopt is less than a value, so this works.
        const auto newub = std::min(
                a.ub, b.ub, [] (const std::optional<upper_bound>& x, const std::optional<upper_bound>& y) {
                    if (x == y || !x) {
                        return false; // Consider nullopt greater than a value, which becomes newub.
                    } else  {
                        return !y || y->includes(x->value);
                    }
                });
        return value_interval{newlb, newub};
    }
};

value_set intersection(value_set a, value_set b) {
    return std::visit(intersection_visitor(), a, b);
}

bool is_satisfied_by(const expression& restr, column_value_eval_bag bag) {
    return std::visit(overloaded_functor{
            [&] (bool v) { return v; },
            [&] (const conjunction& conj) {
                return boost::algorithm::all_of(conj.children, [&] (const expression& c) {
                    return is_satisfied_by(c, bag);
                });
            },
            [&] (const binary_operator& opr) {
                return std::visit(overloaded_functor{
                        [&] (const std::vector<column_value>& cvs) {
                            if (*opr.op == operator_type::EQ) {
                                return equal(opr.rhs, cvs, bag);
                            } else if (*opr.op == operator_type::NEQ) {
                                return !equal(opr.rhs, cvs, bag);
                            } else if (opr.op->is_slice()) {
                                return limits(opr, bag);
                            } else if (*opr.op == operator_type::CONTAINS) {
                                return contains(opr.rhs->bind_and_get(bag.options), cvs, bag);
                            } else if (*opr.op == operator_type::CONTAINS_KEY) {
                                return contains_key(cvs, opr.rhs->bind_and_get(bag.options), bag);
                            } else if (*opr.op == operator_type::LIKE) {
                                return like(cvs, *opr.rhs, bag);
                            } else if (*opr.op == operator_type::IN) {
                                return is_one_of(cvs, *opr.rhs, bag);
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

expression make_conjunction(expression a, expression b) {
    auto children = explode_conjunction(std::move(a));
    boost::copy(explode_conjunction(std::move(b)), back_inserter(children));
    return conjunction{std::move(children)};
}

bool is_satisfied_by(
        const expression& restr,
        const std::vector<bytes>& partition_key, const std::vector<bytes>& clustering_key,
        const query::result_row_view& static_row, const query::result_row_view* row,
        const selection& selection, const query_options& options) {
    const auto regulars = get_non_pk_values(selection, static_row, row);
    return is_satisfied_by(
            restr, {options, row_data_from_partition_slice{partition_key, clustering_key, regulars, selection}});
}

bool is_satisfied_by(
        const expression& restr,
        const schema& schema, const partition_key& key, const clustering_key_prefix& ckey, const row& cells,
        const query_options& options, gc_clock::time_point now) {
    return is_satisfied_by(restr, {options, row_data_from_mutation{key, ckey, cells, schema, now}});
}

void check_multicolumn_bound(const expression& restr, const query_options& options, statements::bound bnd,
                             const std::vector<bytes_opt>& expected) {
    if (!options.get_cql_config().restrictions.use_wip) {
        return;
    }
    // Upstream validation guarantees there is at most one binary_operator node dictating bnd, so just look for
    // the first one:
    if (expected != first_multicolumn_bound(restr, options, bnd)) {
        throw std::logic_error("WIP restrictions mismatch: multicolumn bound");
    }
}

value_set possible_lhs_values(const expression& expr, const query_options& options) {
    return std::visit(overloaded_functor{
            [] (bool b) {
                return b ? unbounded_value_set : empty_value_set;
            },
            [&] (const conjunction& conj) {
                return boost::accumulate(conj.children, unbounded_value_set,
                        [&] (const value_set& acc, const expression& child) {
                            return intersection(std::move(acc), possible_lhs_values(child, options));
                        });
            },
            [&] (const binary_operator& oper) -> value_set {
                if (*oper.op == operator_type::EQ) {
                    const auto rhs = oper.rhs->bind_and_get(options);
                    return rhs ? value_list{to_bytes(rhs)} : empty_value_set; // Nothing equals null.
                } else if (*oper.op == operator_type::IN) {
                    const auto sorted_uniqued = [] (auto&& range) {
                        std::vector<bytes> values(range.begin(), range.end()); // Need a random-access range to sort.
                        const auto sorted = boost::unique(boost::sort(values));
                        return value_list(sorted.begin(), sorted.end());
                    };
                    if (auto mkr = dynamic_cast<lists::marker*>(oper.rhs.get())) {
                        if (auto multi = dynamic_pointer_cast<multi_item_terminal>(mkr->bind(options))) {
                            const auto values = multi->get_elements()
                                    | filtered([] (const bytes_opt& b) { return b.has_value(); })
                                    | transformed([] (const bytes_opt& b) { return *b; });
                            return sorted_uniqued(std::move(values));
                        }
                    } else if (auto dv = dynamic_cast<lists::delayed_value*>(oper.rhs.get())) {
                        const auto values = dv->get_elements()
                                | transformed([&] (const ::shared_ptr<term>& t) { return t->bind_and_get(options); })
                                | filtered([] (const raw_value_view& v) { return v.is_value(); })
                                | transformed([] (const raw_value_view& v) { return to_bytes(v); });
                        return sorted_uniqued(std::move(values));
                    }
                    throw std::logic_error("possible_lhs_values: unexpected IN term");
                } else if (oper.op->is_slice()) {
                    const auto val = oper.rhs->bind_and_get(options);
                    if (!val) {
                        return empty_value_set;
                    }
                    auto cmptype = long_type.get();
                    if (auto cvs = std::get_if<std::vector<column_value>>(&oper.lhs)) {
                        cmptype = get_value_comparator((*cvs)[0]);
                    }
                    static constexpr bool inclusive = true, exclusive = false;
                    std::optional<lower_bound> lb;
                    std::optional<upper_bound> ub;
                    if (*oper.op == operator_type::LT) {
                        ub = upper_bound{to_bytes(val), exclusive, cmptype};
                    } else if (*oper.op == operator_type::LTE) {
                        ub = upper_bound{to_bytes(val), inclusive, cmptype};
                    } else if (*oper.op == operator_type::GT) {
                        lb = lower_bound{to_bytes(val), exclusive, cmptype};
                    } else if (*oper.op == operator_type::GTE) {
                        lb = lower_bound{to_bytes(val), inclusive, cmptype};
                    }
                    return value_interval{lb, ub};
                } else {
                    return unbounded_value_set;
                }
            },
        }, expr);
}

value_interval to_interval(value_set s) {
    return std::visit(overloaded_functor{
            [] (value_interval& ivl) { return std::move(ivl); },
            [] (value_list& lst) {
                if (lst.size() != 1) {
                    throw std::logic_error(format("to_interval called on list of size {}", lst.size()));
                }
                static constexpr bool inclusive = true;
                return value_interval{lower_bound{lst[0], inclusive}, upper_bound{lst[0], inclusive}};
            },
        }, s);
}

bool uses_function(const expression& expr, const sstring& ks_name, const sstring& function_name) {
    return std::visit(overloaded_functor{
            [&] (const conjunction& conj) {
                using std::placeholders::_1;
                return boost::algorithm::any_of(conj.children, std::bind(uses_function, _1, ks_name, function_name));
            },
            [&] (const binary_operator& oper) {
                if (oper.rhs && oper.rhs->uses_function(ks_name, function_name)) {
                    return true;
                } else if (auto columns = std::get_if<std::vector<column_value>>(&oper.lhs)) {
                    return boost::algorithm::any_of(*columns, [&] (const column_value& cv) {
                        return cv.sub && cv.sub->uses_function(ks_name, function_name);
                    });
                }
                return false;
            },
            [&] (const auto& default_case) { return false; },
        }, expr);
}

} // namespace wip

} // namespace restrictions
} // namespace cql3

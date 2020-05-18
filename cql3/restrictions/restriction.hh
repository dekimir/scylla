/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright (C) 2019 ScyllaDB
 *
 * Modified by ScyllaDB
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

#pragma once

#include <variant>
#include <vector>

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include "cql3/query_options.hh"
#include "cql3/term.hh"
#include "cql3/statements/bound.hh"
#include "index/secondary_index_manager.hh"
#include "query-result-reader.hh"
#include "types.hh"

namespace cql3 {

namespace selection {
class selection;
} // namespace selection

namespace restrictions {

class restriction;

struct allow_local_index_tag {};
using allow_local_index = bool_class<allow_local_index_tag>;

/// Work-in-progress new restriction representation.  When complete, it will replace the old one.
namespace wip {

// The new representation exposes its member data publicly.  Operations on restrictions are
// performed by free functions that take restrictions as parameters and use the visitor pattern to
// specialize code for different kinds of restrictions.
//
// The most interesting class is wip::binary_operator below, which can represent both multi- and
// single-column restrictions.  Instead of the old merge_with mechanism for combining restrictions,
// we will simply add them into a conjunction expression that will be processed using visitors.

class binary_operator;
class conjunction;

/// A restriction expression -- union of all possible restriction types.  bool means a Boolean constant.
using expression = std::variant<bool, conjunction, binary_operator>;

/// A column, optionally subscripted by a term (eg, c1 or c2['abc']).
struct column_value {
    const column_definition* col;
    ::shared_ptr<term> sub; ///< If present, this LHS is col[sub], otherwise just col.
    /// For easy creation of vector<column_value> from vector<column_definition*>.
    column_value(const column_definition* col) : col(col) {}
    /// The compiler doesn't auto-generate this due to the other constructor's existence.
    column_value(const column_definition* col, ::shared_ptr<term> sub) : col(col), sub(sub) {}
};

/// Represents token function on LHS of an operator relation.  No need to list column definitions
/// here -- token takes exactly the partition key as its argument.
struct token {};

/// Operator restriction: LHS op RHS.
struct binary_operator {
    std::variant<std::vector<column_value>, token> lhs;
    const operator_type* op; // Pointer because operator_type isn't copyable or assignable.
    ::shared_ptr<term> rhs;
};

/// A conjunction of restrictions.
struct conjunction {
    std::vector<expression> children;
};

/// Creates a conjunction of a and b.  If either a or b is itself a conjunction, its children are inserted
/// directly into the resulting conjunction's children, flattening the expression tree.
extern expression make_conjunction(expression a, expression b);

/// WIP equivalent of restriction::is_satisfied_by.
extern bool is_satisfied_by(
        const expression& restr,
        const std::vector<bytes>& partition_key, const std::vector<bytes>& clustering_key,
        const query::result_row_view& static_row, const query::result_row_view* row,
        const selection::selection&, const query_options&);

/// A column's bound, from WHERE restrictions.
class bound_t {
    bool _unbounded;
    bytes_opt _value; // Invalid when _unbounded is true.
    const abstract_type* _value_type;
public:
    /// \p t must outlive *this.
    explicit bound_t(const abstract_type* t) : _unbounded(true), _value_type(t) {}

    /// \p t must outlive *this.
    explicit bound_t(const data_type& t) : bound_t(t.get()) {}

    /// \p t and \p v must outlive *this.
    bound_t(const abstract_type* t, const bytes_opt& v) : _unbounded(false), _value(v), _value_type(t) {}

    /// \p t and \p v must outlive *this.
    bound_t(const data_type& t, const bytes_opt& v) : bound_t(t.get(), v) {}

    /// True iff *this is a tighter lower bound than \p that.
    bool is_tighter_lb_than(const bound_t& that) const;

    /// True iff *this is a tighter upper bound than \p that.
    bool is_tighter_ub_than(const bound_t& that) const;

    /// Returns value if *this is bounded; otherwise, throws.
    bytes_view_opt value() const;

    /// True iff *this has a value.
    operator bool() const {
        return !_unbounded;
    }

private:
    /// If the comparison *this<=>that can be shortcircuited (due to unbounded or null cases), returns the
    /// comparison result.  Otherwise, returns nullopt.
    std::optional<bool> shortcircuit(const bound_t& that) const;
};

/// Returns a restriction's bound.
bound_t get_bound(const expression&, statements::bound, const query_options&);

/// Calculates bound of a multicolumn restriction, then throws if the result is different from expected.
void check_multicolumn_bound(const expression&, const query_options&, statements::bound,
                             const std::vector<bytes_opt>& expected);

struct upper_bound {
    bytes value;
    bool inclusive;
    const abstract_type* type;
    bool includes(const bytes& v) const {
        const auto cmp = type->compare(v, this->value);
        return cmp < 0 || (cmp == 0 && this->inclusive);
    }
    bool operator==(const upper_bound& that) const {
        return value == that.value && inclusive == that.inclusive && type == that.type;
    }
    bool operator!=(const upper_bound& that) const {
        return !(*this == that);
    }
};

struct lower_bound {
    bytes value;
    bool inclusive;
    const abstract_type* type;
    bool includes(const bytes& v) const {
        const auto cmp = type->compare(v, this->value);
        return cmp > 0 || (cmp == 0 && this->inclusive);
    }
    bool operator==(const lower_bound& that) const {
        return value == that.value && inclusive == that.inclusive && type == that.type;
    }
    bool operator!=(const lower_bound& that) const {
        return !(*this == that);
    }
    bool operator<(const lower_bound& that) const {
        return this->includes(that.value) && *this != that;
    }
};

/// An interval of values between two bounds.
struct value_interval {
    std::optional<lower_bound> lb;
    std::optional<upper_bound> ub;

    bool includes(const bytes_opt& el) const {
        if (!el) {
            return false;
        }
        if (lb && !lb->includes(*el)) {
            return false;
        }
        if (ub && !ub->includes(*el)) {
            return false;
        }
        return true;
    }
};

/// A set of discrete values.
using value_list = std::vector<bytes>; // Sorted (bitwise) and deduped.

/// General set of values.
using value_set = std::variant<value_list, value_interval>;

/// A vector of all LHS values that would satisfy an expression.  Assumes all expression's atoms have the same
/// LHS, which is either token or a single column_value.
value_set possible_lhs_values(const expression&, const query_options&);

} // namespace wip

/**
 * Base class for <code>Restriction</code>s
 */
class restriction {
public:
    enum class op {
        EQ, SLICE, IN, CONTAINS, LIKE
    };
    enum class target {
        SINGLE_COLUMN, MULTIPLE_COLUMNS, TOKEN
    };
protected:
    using op_enum = super_enum<restriction::op, restriction::op::EQ, restriction::op::SLICE, restriction::op::IN, restriction::op::CONTAINS, restriction::op::LIKE>;
    enum_set<op_enum> _ops;
    target _target = target::SINGLE_COLUMN;
public:
    wip::expression expression = false; ///< wip equivalent of *this.
    virtual ~restriction() {}

    restriction() = default;
    explicit restriction(op op) : _target(target::SINGLE_COLUMN) {
        _ops.set(op);
    }

    restriction(op op, target target) : _target(target) {
        _ops.set(op);
    }

    bool is_on_token() const {
        return _target == target::TOKEN;
    }

    bool is_multi_column() const {
        return _target == target::MULTIPLE_COLUMNS;
    }

    bool is_slice() const {
        return _ops.contains(op::SLICE);
    }

    bool is_EQ() const {
        return _ops.contains(op::EQ);
    }

    bool is_IN() const {
        return _ops.contains(op::IN);
    }

    bool is_contains() const {
        return _ops.contains(op::CONTAINS);
    }

    bool is_LIKE() const {
        return _ops.contains(op::LIKE);
    }

    const enum_set<op_enum>& get_ops() const {
        return _ops;
    }

    virtual bool is_inclusive(statements::bound b) const {
        return true;
    }

    /**
     * Merges this restriction with the specified one.
     *
     * @param otherRestriction the restriction to merge into this one
     * @return the restriction resulting of the merge
     * @throws InvalidRequestException if the restrictions cannot be merged
     */
    virtual void merge_with(::shared_ptr<restriction> other) = 0;

    /**
     * Check if the restriction is on indexed columns.
     *
     * @param indexManager the index manager
     * @return <code>true</code> if the restriction is on indexed columns, <code>false</code>
     */
    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager, allow_local_index allow_local) const = 0;

    virtual sstring to_string() const = 0;

    /**
     * Returns <code>true</code> if one of the restrictions use the specified function.
     *
     * @param ks_name the keyspace name
     * @param function_name the function name
     * @return <code>true</code> if one of the restrictions use the specified function, <code>false</code> otherwise.
     */
    virtual bool uses_function(const sstring& ks_name, const sstring& function_name) const = 0;

    virtual std::vector<bytes_opt> values(const query_options& options) const = 0;

    virtual bytes_opt value(const query_options& options) const {
        auto vec = values(options);
        assert(vec.size() == 1);
        return std::move(vec[0]);
    }

    /**
     * Whether the specified row satisfied this restriction.
     * Assumes the row is live, but not all cells. If a cell
     * isn't live and there's a restriction on its column,
     * then the function returns false.
     *
     * @param schema the schema the row belongs to
     * @param key the partition key
     * @param ckey the clustering key
     * @param cells the remaining row columns
     * @return the restriction resulting of the merge
     * @throws InvalidRequestException if the restrictions cannot be merged
     */
    virtual bool is_satisfied_by(const schema& schema,
                                 const partition_key& key,
                                 const clustering_key_prefix& ckey,
                                 const row& cells,
                                 const query_options& options,
                                 gc_clock::time_point now) const = 0;

protected:
    /**
     * Checks if the specified term is using the specified function.
     *
     * @param term the term to check
     * @param ks_name the function keyspace name
     * @param function_name the function name
     * @return <code>true</code> if the specified term is using the specified function, <code>false</code> otherwise.
     */
    static bool term_uses_function(::shared_ptr<term> term, const sstring& ks_name, const sstring& function_name) {
        return bool(term) && term->uses_function(ks_name, function_name);
    }

    /**
     * Checks if one of the specified term is using the specified function.
     *
     * @param terms the terms to check
     * @param ks_name the function keyspace name
     * @param function_name the function name
     * @return <code>true</code> if one of the specified term is using the specified function, <code>false</code> otherwise.
     */
    static bool term_uses_function(const std::vector<::shared_ptr<term>>& terms, const sstring& ks_name, const sstring& function_name) {
        for (auto&& value : terms) {
            if (term_uses_function(value, ks_name, function_name)) {
                return true;
            }
        }
        return false;
    }
};

}

}

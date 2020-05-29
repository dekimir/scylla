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

#include <numeric>
#include <optional>
#include <ostream>
#include <variant>
#include <vector>

#include <fmt/ostream.h>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/sstring.hh>
#include <utils/overloaded_functor.hh>
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

/// True iff restr is satisfied with respect to the row provided from a partition slice.
extern bool is_satisfied_by(
        const expression& restr,
        const std::vector<bytes>& partition_key, const std::vector<bytes>& clustering_key,
        const query::result_row_view& static_row, const query::result_row_view* row,
        const selection::selection&, const query_options&);

/// True iff restr is satisfied with respect to the row provided from a mutation.
extern bool is_satisfied_by(
        const expression& restr,
        const schema& schema, const partition_key& key, const clustering_key_prefix& ckey, const row& cells,
        const query_options& options, gc_clock::time_point now);

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

/// Turns s into an interval if possible, otherwise throws.
value_interval to_interval(value_set s);

/// True iff expr references the function.
bool uses_function(const expression& expr, const sstring& ks_name, const sstring& function_name);

/// True iff any of the indices from the manager can support the entire expression.  If allow_local, use all
/// indices; otherwise, use only global indices.
bool has_supporting_index(
        const expression&, const secondary_index::secondary_index_manager&, allow_local_index allow_local);

extern sstring to_string(const expression&);

extern std::ostream& operator<<(std::ostream&, const column_value&);

extern std::ostream& operator<<(std::ostream&, const expression&);

/// If there is a binary_operator atom b for which f(b) is true, returns it.  Otherwise returns null.
template<typename Fn>
const expression* find_if(const expression& e, Fn f) {
    return std::visit(overloaded_functor{
            [&] (const binary_operator& op) { return f(op) ? &e : nullptr; },
            [] (bool) -> const expression* { return nullptr; },
            [&] (const conjunction& conj) -> const expression* {
                for (auto& child : conj.children) {
                    if (auto found = find_if(child, f)) {
                        return found;
                    }
                }
                return nullptr;
            },
        }, e);
}

/// Counts binary_operator atoms b for which f(b) is true.
template<typename Fn>
size_t count_if(const expression& e, Fn f) {
    return std::visit(overloaded_functor{
            [&] (const binary_operator& op) -> size_t { return f(op) ? 1 : 0; },
            [&] (const conjunction& conj) {
                return std::accumulate(conj.children.cbegin(), conj.children.cend(), size_t{0},
                                       [&] (size_t acc, const expression& c) { return acc + count_if(c, f); });
            },
            [] (bool) -> size_t { return 0; },
        }, e);
}

inline const expression* find(const expression& e, const operator_type& op) {
    return find_if(e, [&] (const binary_operator& o) { return *o.op == op; });
}

inline bool needs_filtering(const expression& e) {
    return find_if(e, [] (const binary_operator& o) { return o.op->needs_filtering(); });
}

inline bool has_slice(const expression& e) {
    return find_if(e, [] (const binary_operator& o) { return o.op->is_slice(); });
}

inline bool has_token(const expression& e) {
    return find_if(e, [] (const binary_operator& o) { return std::holds_alternative<token>(o.lhs); });
}

/// True iff binary_operator involves a collection.
extern bool is_on_collection(const binary_operator&);

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
    target _target = target::SINGLE_COLUMN;
public:
    wip::expression expression = false; ///< wip equivalent of *this.
    virtual ~restriction() {}

    restriction() = default;
    explicit restriction(op op) : _target(target::SINGLE_COLUMN) {
    }

    restriction(op op, target target) : _target(target) {
    }

    bool is_multi_column() const {
        return _target == target::MULTIPLE_COLUMNS;
    }
};

}

}

// This makes fmt::join() work on expression and column_value using operator<<.
//
// See https://github.com/fmtlib/fmt/issues/1283#issuecomment-526114915
template <typename Char>
struct fmt::formatter<cql3::restrictions::wip::expression, Char>
    : fmt::v6::internal::fallback_formatter<cql3::restrictions::wip::expression, Char>
{};
template <typename Char>
struct fmt::formatter<cql3::restrictions::wip::column_value, Char>
    : fmt::v6::internal::fallback_formatter<cql3::restrictions::wip::column_value, Char>
{};

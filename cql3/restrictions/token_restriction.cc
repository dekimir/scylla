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

#include "token_restriction.hh"

#include <utils/overloaded_functor.hh>

namespace cql3 {

namespace restrictions {

using bounds_range_type = partition_key_restrictions::bounds_range_type;
using bound_t = bounds_range_type::bound;
using rngvec = dht::partition_range_vector;

/// Empty vector means empty range.  TODO: drop the vector; bounds_range_type can represent an empty range just
/// fine.
rngvec get_token_range(const expression& expr, const query_options& options, schema_ptr schema) {
    static const auto entire_token_range = dht::partition_range::make_open_ended_both_sides();
    // conjunction = intersection of ranges
    // but `< min_token` is special
    // default bound = min/max inclusive
    // bounds_range_type = dht::partition_range = nonwrapping_range<ring_position>
    // bound_t = bounds_range_type::bound = nonwrapping_range<ring_position>::bound = range_bound<ring_position>
    return std::visit(overloaded_functor{
            [&] (const binary_operator& oper) {
                if (std::holds_alternative<token>(oper.lhs)) {
                    const auto val = oper.rhs->bind_and_get(options);
                    if (!val) {
                        return rngvec{}; // Null means an empty set, as no row will satisfy the restriction.
                    }
                    const auto token = dht::token::from_bytes(to_bytes(val));
                    if (*oper.op == operator_type::EQ) {
                        return rngvec{
                            {range_bound(dht::ring_position::starting_at(token)),
                             range_bound(dht::ring_position::ending_at(token))},
                        };
                    } else if (*oper.op == operator_type::GT) {
                        return rngvec{{range_bound(dht::ring_position::ending_at(token)), {}},};
                    } else if (*oper.op == operator_type::GTE) {
                        return rngvec{{range_bound(dht::ring_position::starting_at(token)), {}},};
                    } else if (*oper.op == operator_type::LT) {
                        // token < MIN is interpreted as token < MAX, unsure why...
                        const auto ub = dht::ring_position::starting_at(
                                token.is_minimum() ? dht::maximum_token() : token);
                        return rngvec{{{}, range_bound(ub)},};
                    } else if (*oper.op == operator_type::LTE) {
                        // token <= MIN is interpreted as token <= MAX, unsure why...
                        const auto ub = dht::ring_position::ending_at(
                                token.is_minimum() ? dht::maximum_token() : token);
                        return rngvec{{{}, range_bound(ub)},};
                    }
                }
                return rngvec{entire_token_range};
            },
            [] (bool b) {
                return b ? rngvec{entire_token_range} : rngvec{};
            },
            [&] (const conjunction& conj) {
                using range_opt = std::optional<bounds_range_type>;
                const range_opt intersection =
                        boost::accumulate(conj.children, range_opt{}, [&] (range_opt&& acc, const expression& child) {
                            if (!acc) {
                                return std::move(acc);
                            }
                            const auto r = get_token_range(child, options, schema);
                            if (r.empty()) {
                                return range_opt{};
                            }
                            return acc->intersection(r[0], dht::ring_position_comparator(*schema));
                        });
                return intersection ? rngvec{*intersection} : rngvec{};
            },
        }, expr);
}

} // namespace restrictions

} // namespace cql3

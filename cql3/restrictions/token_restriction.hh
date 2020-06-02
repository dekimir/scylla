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
 * Copyright (C) 2015 ScyllaDB
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

#include "restriction.hh"
#include "primary_key_restrictions.hh"
#include "exceptions/exceptions.hh"
#include "term_slice.hh"
#include "keys.hh"

class column_definition;

namespace cql3 {

namespace restrictions {

/**
 * <code>Restriction</code> using the token function.
 */
class token_restriction: public partition_key_restrictions {
private:
    /**
     * The definition of the columns to which apply the token restriction.
     */
    std::vector<const column_definition *> _column_definitions;
public:
    token_restriction(op op, std::vector<const column_definition *> c)
            : partition_key_restrictions(op, target::TOKEN), _column_definitions(std::move(c)) {
    }

    std::vector<const column_definition*> get_column_defs() const override {
        return _column_definitions;
    }

    bool uses_function(const sstring& ks_name, const sstring& function_name) const override {
        return wip::uses_function(expression, ks_name, function_name);
    }

    virtual bool has_supporting_index(const secondary_index::secondary_index_manager& index_manager, allow_local_index allow_local) const override {
        return false;
    }

#if 0
    void add_index_expression_to(std::vector<::shared_ptr<index_expression>>& expressions,
                                         const query_options& options) override {
        throw exceptions::unsupported_operation_exception();
    }
#endif

    std::vector<bounds_range_type> bounds_ranges(const query_options& options) const override {
        using namespace wip;
        const auto bounds = to_interval(possible_lhs_values(expression, options));
        const auto start_token = bounds.lb ? dht::token::from_bytes(bounds.lb->value) : dht::minimum_token();
        auto end_token = bounds.ub ? dht::token::from_bytes(bounds.ub->value) : dht::maximum_token();
        if (end_token.is_minimum()) {
            // The token was parsed as a minimum marker (token::kind::before_all_keys), but as it appears in
            // the end bound position, it is actually the maximum marker (token::kind::after_all_keys).
            end_token = dht::maximum_token();
        }
        const bool include_start = bounds.lb && bounds.lb->inclusive;
        const auto include_end = bounds.ub && bounds.ub->inclusive;

        /*
         * If we ask SP.getRangeSlice() for (token(200), token(200)], it will happily return the whole ring.
         * However, wrapping range doesn't really make sense for CQL, and we want to return an empty result in that
         * case (CASSANDRA-5573). So special case to create a range that is guaranteed to be empty.
         *
         * In practice, we want to return an empty result set if either startToken > endToken, or both are equal but
         * one of the bound is excluded (since [a, a] can contains something, but not (a, a], [a, a) or (a, a)).
         */
        if (start_token > end_token
                || (start_token == end_token
                    && (!include_start || !include_end))) {
            return {};
        }

        typedef typename bounds_range_type::bound bound;

        auto start = bound(include_start
                           ? dht::ring_position::starting_at(start_token)
                           : dht::ring_position::ending_at(start_token));
        auto end = bound(include_end
                           ? dht::ring_position::ending_at(end_token)
                           : dht::ring_position::starting_at(end_token));

        return { bounds_range_type(std::move(start), std::move(end)) };
    }

    ::shared_ptr<partition_key_restrictions> merge_to(schema_ptr, ::shared_ptr<restriction> restriction) {
        this->expression = make_conjunction(std::move(this->expression), restriction->expression);
        return this->shared_from_this();
    }

    sstring to_string() const override {
        return wip::to_string(expression);
    }

    class EQ;
    class slice;
};


class token_restriction::EQ final : public token_restriction {
private:
    ::shared_ptr<term> _value;
public:
    EQ(std::vector<const column_definition*> column_defs, ::shared_ptr<term> value)
        : token_restriction(op::EQ, column_defs)
        , _value(std::move(value))
    {
        expression = wip::binary_operator{wip::token{}, &operator_type::EQ, _value};
    }
};

class token_restriction::slice final : public token_restriction {
private:
    term_slice _slice;
public:
    slice(std::vector<const column_definition*> column_defs, statements::bound bound, bool inclusive, ::shared_ptr<term> term)
        : token_restriction(op::SLICE, column_defs)
        , _slice(term_slice::new_instance(bound, inclusive, term))
    {
        const auto op = is_start(bound) ? (inclusive ? &operator_type::GTE : &operator_type::GT)
                : (inclusive ? &operator_type::LTE : &operator_type::LT);
        expression = wip::binary_operator{wip::token{}, op, std::move(term)};
    }
};

}

}

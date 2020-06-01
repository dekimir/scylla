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

#include <optional>

#include "cql3/restrictions/restriction.hh"
#include "cql3/restrictions/term_slice.hh"
#include "cql3/term.hh"
#include "cql3/abstract_marker.hh"
#include "cql3/lists.hh"
#include <seastar/core/shared_ptr.hh>
#include "schema_fwd.hh"
#include "to_string.hh"
#include "exceptions/exceptions.hh"
#include "keys.hh"
#include "utils/like_matcher.hh"

namespace cql3 {

namespace restrictions {

class single_column_restriction : public restriction {
protected:
    /**
     * The definition of the column to which apply the restriction.
     */
    const column_definition& _column_def;
public:
    single_column_restriction(const column_definition& column_def) : _column_def(column_def) {}

    const column_definition& get_column_def() const {
        return _column_def;
    }

#if 0
    @Override
    public void addIndexExpressionTo(List<IndexExpression> expressions,
                                     QueryOptions options) throws InvalidRequestException
    {
        List<ByteBuffer> values = values(options);
        checkTrue(values.size() == 1, "IN restrictions are not supported on indexed columns");

        ByteBuffer value = validateIndexedValue(columnDef, values.get(0));
        expressions.add(new IndexExpression(columnDef.name.bytes, Operator.EQ, value));
    }

    /**
     * Check if this type of restriction is supported by the specified index.
     *
     * @param index the Secondary index
     * @return <code>true</code> this type of restriction is supported by the specified index,
     * <code>false</code> otherwise.
     */
    protected abstract boolean isSupportedBy(SecondaryIndex index);
#endif

    class IN;
    class IN_with_values;
    class IN_with_marker;
    class LIKE;
    class slice;
    class contains;

protected:
    std::optional<atomic_cell_value_view> get_value(const schema& schema,
            const partition_key& key,
            const clustering_key_prefix& ckey,
            const row& cells,
            gc_clock::time_point now) const;
};

class single_column_restriction::IN : public single_column_restriction {
public:
    IN(const column_definition& column_def)
        : single_column_restriction(column_def)
    { }

    virtual std::vector<bytes_opt> values_raw(const query_options& options) const = 0;

#if 0
    @Override
    protected final boolean isSupportedBy(SecondaryIndex index)
    {
        return index.supportsOperator(Operator.IN);
    }
#endif
};

class single_column_restriction::IN_with_values : public single_column_restriction::IN {
protected:
    std::vector<::shared_ptr<term>> _values;
public:
    IN_with_values(const column_definition& column_def, std::vector<::shared_ptr<term>> values)
        : single_column_restriction::IN(column_def)
        , _values(std::move(values))
    {
        expression = wip::binary_operator{
            std::vector{wip::column_value(&column_def)}, &operator_type::IN, ::make_shared<lists::delayed_value>(_values)};
    }

    virtual std::vector<bytes_opt> values_raw(const query_options& options) const override {
        std::vector<bytes_opt> ret;
        for (auto&& v : _values) {
            ret.emplace_back(to_bytes_opt(v->bind_and_get(options)));
        }
        return ret;
    }
};

class single_column_restriction::IN_with_marker : public IN {
public:
    shared_ptr<abstract_marker> _marker;
public:
    IN_with_marker(const column_definition& column_def, shared_ptr<abstract_marker> marker)
            : IN(column_def), _marker(std::move(marker)) {
        expression = wip::binary_operator{
            std::vector{wip::column_value(&column_def)}, &operator_type::IN, std::move(_marker)};
    }

    virtual std::vector<bytes_opt> values_raw(const query_options& options) const override {
        auto&& lval = dynamic_pointer_cast<multi_item_terminal>(_marker->bind(options));
        if (!lval) {
            throw exceptions::invalid_request_exception("Invalid null value for IN restriction");
        }
        return lval->get_elements();
    }
};

class single_column_restriction::slice : public single_column_restriction {
private:
    term_slice _slice;
public:
    slice(const column_definition& column_def, statements::bound bound, bool inclusive, ::shared_ptr<term> term)
        : single_column_restriction(column_def)
        , _slice(term_slice::new_instance(bound, inclusive, term))
    {
        const auto op = is_start(bound) ? (inclusive ? &operator_type::GTE : &operator_type::GT)
                : (inclusive ? &operator_type::LTE : &operator_type::LT);
        expression = wip::binary_operator{std::vector{wip::column_value(&column_def)}, op, std::move(term)};
    }

    slice(const column_definition& column_def, term_slice slice)
        : single_column_restriction(column_def)
        , _slice(slice)
    { }

#if 0
    virtual void addIndexExpressionTo(List<IndexExpression> expressions, override
                                     QueryOptions options) throws InvalidRequestException
    {
        for (statements::bound b : {statements::bound::START, statements::bound::END})
        {
            if (has_bound(b))
            {
                ByteBuffer value = validateIndexedValue(columnDef, _slice.bound(b).bindAndGet(options));
                Operator op = _slice.getIndexOperator(b);
                // If the underlying comparator for name is reversed, we need to reverse the IndexOperator: user operation
                // always refer to the "forward" sorting even if the clustering order is reversed, but the 2ndary code does
                // use the underlying comparator as is.
                op = columnDef.isReversedType() ? op.reverse() : op;
                expressions.add(new IndexExpression(columnDef.name.bytes, op, value));
            }
        }
    }

    virtual bool isSupportedBy(SecondaryIndex index) override
    {
        return _slice.isSupportedBy(index);
    }
#endif
};

class single_column_restriction::LIKE final : public single_column_restriction {
private:
    /// Represents `col LIKE val1 AND col LIKE val2 AND ... col LIKE valN`.
    std::vector<::shared_ptr<term>> _values;
    /// Each element matches a cell value against a LIKE pattern.
    std::vector<like_matcher> _matchers;
public:
    LIKE(const column_definition& column_def, ::shared_ptr<term> value)
        : single_column_restriction(column_def)
        , _values{value}
    {
        expression = wip::binary_operator{std::vector{wip::column_value(&column_def)}, &operator_type::LIKE, _values[0]};
    }

    virtual void merge_with(::shared_ptr<restriction> other);
};

// This holds CONTAINS, CONTAINS_KEY, and map[key] = value restrictions because we might want to have any combination of them.
class single_column_restriction::contains final : public single_column_restriction {
private:
    std::vector<::shared_ptr<term>> _values;
    std::vector<::shared_ptr<term>> _keys;
    std::vector<::shared_ptr<term>> _entry_keys;
    std::vector<::shared_ptr<term>> _entry_values;
public:
    contains(const column_definition& column_def, ::shared_ptr<term> t, bool is_key)
        : single_column_restriction(column_def) {
        if (is_key) {
            _keys.emplace_back(t);
        } else {
            _values.emplace_back(t);
        }
        expression = wip::binary_operator{
            std::vector{wip::column_value(&column_def)},
            is_key ? &operator_type::CONTAINS_KEY : &operator_type::CONTAINS,
            std::move(t)};
    }

    contains(const column_definition& column_def, ::shared_ptr<term> map_key, ::shared_ptr<term> map_value)
            : single_column_restriction(column_def) {
        expression = wip::binary_operator{
            std::vector{wip::column_value(&column_def, map_key)}, &operator_type::EQ, map_value};
        _entry_keys.emplace_back(std::move(map_key));
        _entry_values.emplace_back(std::move(map_value));
    }

#if 0
        virtual void add_index_expression_to(std::vector<::shared_ptr<index_expression>>& expressions,
                const query_options& options) override {
            add_expressions_for(expressions, values(options), operator_type::CONTAINS);
            add_expressions_for(expressions, keys(options), operator_type::CONTAINS_KEY);
            add_expressions_for(expressions, entries(options), operator_type::EQ);
        }

        private void add_expressions_for(std::vector<::shared_ptr<index_expression>>& target, std::vector<bytes_opt> values,
                                       const operator_type& op) {
            for (ByteBuffer value : values)
            {
                validateIndexedValue(columnDef, value);
                target.add(new IndexExpression(columnDef.name.bytes, op, value));
            }
        }
#endif

    uint32_t number_of_values() const {
        return _values.size();
    }

    uint32_t number_of_keys() const {
        return _keys.size();
    }

    uint32_t number_of_entries() const {
        return _entry_keys.size();
    }

#if 0
        private List<ByteBuffer> keys(const query_options& options) {
            return bindAndGet(keys, options);
        }

        private List<ByteBuffer> entries(QueryOptions options) throws InvalidRequestException
        {
            List<ByteBuffer> entryBuffers = new ArrayList<>(_entry_keys.size());
            List<ByteBuffer> keyBuffers = bindAndGet(_entry_keys, options);
            List<ByteBuffer> valueBuffers = bindAndGet(_entry_values, options);
            for (int i = 0; i < _entry_keys.size(); i++)
            {
                if (valueBuffers.get(i) == null)
                    throw new InvalidRequestException("Unsupported null value for map-entry equality");
                entryBuffers.add(CompositeType.build(keyBuffers.get(i), valueBuffers.get(i)));
            }
            return entryBuffers;
        }
#endif

private:
    /**
     * Binds the query options to the specified terms and returns the resulting values.
     *
     * @param terms the terms
     * @param options the query options
     * @return the value resulting from binding the query options to the specified terms
     * @throws invalid_request_exception if a problem occurs while binding the query options
     */
    static std::vector<bytes_opt> bind_and_get(std::vector<::shared_ptr<term>> terms, const query_options& options) {
        std::vector<bytes_opt> values;
        values.reserve(terms.size());
        for (auto&& term : terms) {
            values.emplace_back(to_bytes_opt(term->bind_and_get(options)));
        }
        return values;
    }
};


}

}

/*
 * Copyright (C) 2019 ScyllaDB
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

#include <fmt/format.h>
#include <functional>
#include <iostream>
#include <seastar/core/sstring.hh>

#include "exceptions/exceptions.hh"

/// Returns a predicate that takes an exception and applies f to its message.
///
/// If f returns true, the predicate also returns true.  But if f returns false, the predicate
/// outputs the exception message into the test log before itself returning false.  This is handy
/// when passing the predicate to, eg, BOOST_REQUIRE_EXCEPTION:
///
/// \code
/// BOOST_REQUIRE_EXCEPTION(
///     some_code,
///     exception_class,
///     make_predicate_on_exception_message(make_contains_predicate("something")));
/// \endcode
///
/// See also REQUIRE_EXCEPTION below.
template <typename Exception>
auto make_predicate_on_exception_message(std::function<bool(const sstring&)> f) {
    return [f = std::move(f)](const Exception& e) {
        const auto& msg = e.what();
        const bool success = f(msg);
        BOOST_CHECK_MESSAGE(success, fmt::format("Exception message was: {}", msg));
        return success;
    };
}

/// Returns a predicate that will check if a string contains the given fragment.
inline auto make_contains_predicate(const sstring& fragment) {
    return [=](const sstring& str) { return str.find(fragment) != sstring::npos; };
}

/// Returns a predicate that will check if a string equals the given text.
inline auto make_equals_predicate(const sstring& text) {
    return [=](const sstring& str) { return str == text; };
}

/// Like BOOST_REQUIRE_EXCEPTION, but also checks that the exception's message exactly equals \c message.
#define REQUIRE_EXCEPTION(expression, exception_type, message) \
    BOOST_REQUIRE_EXCEPTION(expression, exception_type,        \
        make_predicate_on_exception_message<exception_type>(make_equals_predicate(message)))

/// Like BOOST_REQUIRE_EXCEPTION, but also checks that the exception's message contains \c fragment.
#define REQUIRE_EXCEPTION_F(expression, exception_type, fragment) \
    BOOST_REQUIRE_EXCEPTION(expression, exception_type,           \
        make_predicate_on_exception_message<exception_type>(make_contains_predicate(fragment)))

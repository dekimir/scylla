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

#include "bytes.hh"
#include <boost/regex/icu.hpp>
#include <memory>

/// Makes a regular expression corresponding to a CQL LIKE pattern.
///
/// The pattern is UTF-8 text that matches as follows:
/// - '_' matches any single character
/// - '%' matches any substring (including an empty string)
/// - '\' escapes the next pattern character, so it matches verbatim
/// - any other pattern character matches itself
///
/// The whole text must match the pattern; thus <code>'abc' LIKE 'a'</code> doesn't match, but
/// <code>'abc' LIKE 'a%'</code> matches.
boost::u32regex make_regex_from_like_pattern(bytes_view pattern);

/// Implements <code>text LIKE pattern</code>.
class like_matcher {
    class impl;
    std::unique_ptr<impl> _impl;
public:
    /// Compiles \c pattern and stores the result.
    ///
    /// \param pattern UTF-8 encoded pattern with wildcards '_' and '%'.
    explicit like_matcher(bytes_view pattern);

    like_matcher(like_matcher&&) noexcept; // Must be defined in .cc, where class impl is known.

    ~like_matcher();

    /// Runs the compiled pattern on \c text.
    ///
    /// \return true iff text matches constructor's pattern.
    bool operator()(bytes_view text) const;

    /// Resets pattern if different from the current one.
    void reset(bytes_view pattern);
};

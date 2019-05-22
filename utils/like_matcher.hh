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

#include <memory>
#include <string>
#include <vector>

/// Implements <code>text LIKE pattern</code>.
class like_matcher {
public:
    /// Compiles \c pattern and stores the result.
    ///
    /// @param pattern Must not be empty.
    like_matcher(const std::u32string& pattern);

    /// Runs the compiled pattern on \c text.
    ///
    /// @return true iff text matches constructor's pattern.
    bool matches(const std::u32string& text) const; // TODO: stream text in one character at a time.

    static constexpr char32_t escape_char = U'\\';

    /// A state in the nondeterministic finite automaton (NDA) used to evaluate the <code>text LIKE
    /// pattern</code> expression.
    ///
    /// The pattern is translated into a digraph of state nodes forming an NDA.  The NDA is then run
    /// on the text, searching for a match one character at a time.  Each text character results in
    /// a state transition.  If a state indicating a match is reachable, the text matches.
    struct state {
        enum class match_type {
            MATCH,           /// Match a specific character.
            SKIP1,           /// Match any one character.
            MATCH_OR_SKIP,   /// Match or skip a character.
            EOS,             /// Match text end.
            FAIL,            /// Unfixable failure occured.
            SUCC,            /// Success, no matter what happens next.
        };

        match_type _type;

        /// Character to match, if the type requires it.
        char32_t _ch;

        /// Whether ending in this state indicates a successful match.
        bool _match_found;

        /// Index of the next state to transition to, under certain circumstances depending on the type.
        size_t _next_state_pos;
    };

private:
    /// Represents the NDA by storing all its possible states.
    std::vector<state> _states;
};

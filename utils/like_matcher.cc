
/*
 * Copyright 2019 ScyllaDB
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


#include "like_matcher.hh"

#include <boost/algorithm/cxx11/any_of.hpp>
#include <cassert>
#include <unordered_set>

namespace {
    /// The vector of NDA states always contains at least these positions:
    enum fixed_indices { IFAIL, IEOS, MIN_SIZE };
} // anonymous namespace

using mt = like_matcher::state::match_type;

like_matcher::like_matcher(const std::u32string& pattern) : _states(MIN_SIZE) {
    assert(!pattern.empty());
    _states[IFAIL] = state{mt::FAIL, 0, false, IFAIL};
    _states[IEOS] = state{mt::EOS, 0, true, IFAIL};
    bool escaping = false;
    auto cur = pattern.cbegin();
    while (cur != pattern.cend()) {
        bool pattern_ends = (cur + 1 == pattern.cend());
        if (!escaping && *cur == escape_char && !pattern_ends) {
            escaping = true;
            ++cur;
            continue;
        }
        size_t next_state_pos = _states.size() + 1;
        if (*cur == U'_' && !escaping) {
            // At this point in the text, expect a single character of any value.  Transition to
            // the next pattern element, if it exists; otherwise, expect the text end.
            _states.push_back(state{mt::SKIP1, 0, false, pattern_ends ? IEOS : next_state_pos});
        } else if (*cur == U'%' && !escaping) {
            // Skip any glued % patterns:
            while (cur != pattern.cend() && *cur == U'%') {
                ++cur;
            }
            if (cur == pattern.cend()) {
                // Pattern ends in '%': whatever the rest of the text is, a match occurred by now.
                _states.push_back(state{mt::SUCC, 0, true, 0});
                break;
            } else {
                char32_t next_pattern = *cur;
                // TODO: handle next_pattern=='_'.
                pattern_ends = (cur + 1 == pattern.cend());
                if (next_pattern == escape_char && !pattern_ends) {
                    next_pattern = *++cur;
                    pattern_ends = (cur + 1 == pattern.cend());
                }
                // Expect the next pattern element, but skip anything else.
                _states.push_back(
                    state{mt::MATCH_OR_SKIP, next_pattern, false, pattern_ends ? IEOS : next_state_pos});
            }
        } else {
            _states.push_back(state{mt::MATCH, *cur, false, pattern_ends ? IEOS : next_state_pos});
        }
        escaping = false;
        ++cur;
    }
}

bool like_matcher::matches(const std::u32string& text) const {
    using boost::algorithm::any_of;
    /// States we may currently be in (represented as _states indices).
    std::unordered_set<size_t> current_states(_states.size());
    assert(_states.size() > MIN_SIZE);
    current_states.insert(MIN_SIZE); // The beginning state is after fixed_indices.
    for (const char32_t ch : text) {
        /// States we may be in next (represented as _states indices).
        std::unordered_set<size_t> next_states(_states.size());
        for (const size_t si : current_states) {
            const auto st = _states[si];
            switch (st._type) {
            case mt::SKIP1:
                next_states.insert(st._next_state_pos);
                break;
            case mt::MATCH:
                // TODO: handle case sensitivity in `==` below.
                next_states.insert(ch == st._ch ? st._next_state_pos : IFAIL);
                break;
            case mt::MATCH_OR_SKIP:
                // This is where nondeterminism kicks in:
                next_states.insert(si); // Possibly skip ch and look downstream for next pattern.
                if (ch == st._ch) { // TODO: handle case sensitivity.
                    next_states.insert(st._next_state_pos); // Possibly ch starts the next pattern.
                }
                break;
            case mt::EOS:
                // Text has at least one extra character beyond the pattern => no match.
                next_states.insert(IFAIL);
                break;
            default: // SUCC or FAIL: both are permanent.
                next_states.insert(si);
                break;
            }
        }
        if (any_of(next_states, [this](size_t i) { return _states[i]._type == mt::SUCC; })) {
            // Prefix match has occurred.
            return true;
        }
        if (next_states.size() == 1 && next_states.count(IFAIL)) {
            // A mismatch has occurred.
            return false;
        }
        current_states = next_states;
    }
    return any_of(current_states, [this](size_t i) { return _states[i]._match_found; });
}

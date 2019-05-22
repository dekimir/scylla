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

#define BOOST_TEST_MODULE core

#include <boost/test/unit_test.hpp>

#include "utils/like_matcher.hh"

BOOST_AUTO_TEST_CASE(test_literal) {
    like_matcher m(U"abc");
    BOOST_TEST(m.matches(U"abc"));
    BOOST_TEST(!m.matches(U""));
    BOOST_TEST(!m.matches(U"a"));
    BOOST_TEST(!m.matches(U"b"));
    BOOST_TEST(!m.matches(U"ab"));
    BOOST_TEST(!m.matches(U"abcd"));
    BOOST_TEST(!m.matches(U" abc"));
}

BOOST_AUTO_TEST_CASE(test_underscore_start) {
    like_matcher m(U"_a");
    BOOST_TEST(m.matches(U"aa"));
    BOOST_TEST(m.matches(U"Шa"));
    BOOST_TEST(!m.matches(U""));
    BOOST_TEST(!m.matches(U"a"));
    BOOST_TEST(!m.matches(U".aa"));
}

BOOST_AUTO_TEST_CASE(test_underscore_end) {
    like_matcher m(U"a_");
    BOOST_TEST(m.matches(U"aa"));
    BOOST_TEST(m.matches(U"aШ"));
    BOOST_TEST(!m.matches(U""));
    BOOST_TEST(!m.matches(U"a"));
    BOOST_TEST(!m.matches(U"aa."));
}

BOOST_AUTO_TEST_CASE(test_underscore_middle) {
    like_matcher m(U"a_c");
    BOOST_TEST(m.matches(U"abc"));
    BOOST_TEST(m.matches(U"aШc"));
    BOOST_TEST(!m.matches(U""));
    BOOST_TEST(!m.matches(U"ac"));
    BOOST_TEST(!m.matches(U"abcd"));
    BOOST_TEST(!m.matches(U"abb"));
}

BOOST_AUTO_TEST_CASE(test_underscore_consecutive) {
    like_matcher m(U"a__d");
    BOOST_TEST(m.matches(U"abcd"));
    BOOST_TEST(m.matches(U"a__d"));
    BOOST_TEST(m.matches(U"aШШd"));
    BOOST_TEST(!m.matches(U""));
    BOOST_TEST(!m.matches(U"abcde"));
    BOOST_TEST(!m.matches(U"a__e"));
    BOOST_TEST(!m.matches(U"e__d"));
}

BOOST_AUTO_TEST_CASE(test_underscore_multiple) {
    like_matcher m2(U"a_c_");
    BOOST_TEST(m2.matches(U"abcd"));
    BOOST_TEST(m2.matches(U"arc."));
    BOOST_TEST(m2.matches(U"aШcШ"));
    BOOST_TEST(!m2.matches(U""));
    BOOST_TEST(!m2.matches(U"abcde"));
    BOOST_TEST(!m2.matches(U"abdc"));
    BOOST_TEST(!m2.matches(U"4bcd"));

    like_matcher m3(U"_cyll_D_");
    BOOST_TEST(m3.matches(U"ScyllaDB"));
    BOOST_TEST(m3.matches(U"ШcyllaD2"));
    BOOST_TEST(!m3.matches(U""));
    BOOST_TEST(!m3.matches(U"ScyllaDB2"));
}

BOOST_AUTO_TEST_CASE(test_percent_start) {
    like_matcher m(U"%bcd");
    BOOST_TEST(m.matches(U"bcd"));
    BOOST_TEST(m.matches(U"abcd"));
    BOOST_TEST(m.matches(U"ШШabcd"));
    BOOST_TEST(!m.matches(U""));
    BOOST_TEST(!m.matches(U"bcde"));
    BOOST_TEST(!m.matches(U"abcde"));
    BOOST_TEST(!m.matches(U"aaaaaaaaaaaaabce"));
}

BOOST_AUTO_TEST_CASE(test_percent_end) {
    like_matcher m(U"abc%");
    BOOST_TEST(m.matches(U"abc"));
    BOOST_TEST(m.matches(U"abcd"));
    BOOST_TEST(m.matches(U"abccccccccccccccccccccc"));
    BOOST_TEST(m.matches(U"abcdШШ"));
    BOOST_TEST(!m.matches(U""));
    BOOST_TEST(!m.matches(U"a"));
    BOOST_TEST(!m.matches(U"ab"));
    BOOST_TEST(!m.matches(U"abd"));
}

BOOST_AUTO_TEST_CASE(test_percent_middle) {
    like_matcher m(U"a%z");
    BOOST_TEST(m.matches(U"az"));
    BOOST_TEST(m.matches(U"aaz"));
    BOOST_TEST(m.matches(U"aШШz"));
    BOOST_TEST(m.matches(U"a...................................z"));
    BOOST_TEST(!m.matches(U""));
    BOOST_TEST(!m.matches(U"a"));
    BOOST_TEST(!m.matches(U"ab"));
    BOOST_TEST(!m.matches(U"aza"));
    BOOST_TEST(!m.matches(U"aШШШШШШШШШШza"));

    like_matcher und(U"a%_");
    BOOST_TEST(und.matches(U"a_"));
    // TODO: uncomment when this case is fixed.
    //BOOST_TEST(und.matches(U"ab"));
    //BOOST_TEST(und.matches(U"aШШШШШШШШШШ"));
    BOOST_TEST(!und.matches(U""));
    BOOST_TEST(!und.matches(U"a"));
    BOOST_TEST(!und.matches(U"b_"));
}

BOOST_AUTO_TEST_CASE(test_percent_multiple) {
    like_matcher cons(U"a%%z");
    BOOST_TEST(cons.matches(U"az"));
    BOOST_TEST(cons.matches(U"aaz"));
    BOOST_TEST(cons.matches(U"aШШz"));
    BOOST_TEST(cons.matches(U"a...................................z"));
    BOOST_TEST(!cons.matches(U""));
    BOOST_TEST(!cons.matches(U"a"));
    BOOST_TEST(!cons.matches(U"ab"));
    BOOST_TEST(!cons.matches(U"aza"));
    BOOST_TEST(!cons.matches(U"aШШШШШШШШШШza"));

    like_matcher spread(U"|%|%|");
    BOOST_TEST(spread.matches(U"|||"));
    BOOST_TEST(spread.matches(U"|a||"));
    BOOST_TEST(spread.matches(U"||b|"));
    BOOST_TEST(spread.matches(U"|a|b|"));
    BOOST_TEST(spread.matches(U"|||||||"));
    BOOST_TEST(spread.matches(U"|ШШШШШШШШШШza||"));
    BOOST_TEST(spread.matches(U"||ШШШШШШШШШШza|"));
    BOOST_TEST(spread.matches(U"|ШШШШШШШШШШza|....................|"));
    BOOST_TEST(!spread.matches(U""));
    BOOST_TEST(!spread.matches(U"|"));
    BOOST_TEST(!spread.matches(U"|+"));
    BOOST_TEST(!spread.matches(U"|+++++"));
    BOOST_TEST(!spread.matches(U"||"));
    BOOST_TEST(!spread.matches(U"|.......................|"));
    BOOST_TEST(!spread.matches(U"|.......................|++++++++++"));

    like_matcher bookends(U"%ac%");
    BOOST_TEST(bookends.matches(U"ac"));
    BOOST_TEST(bookends.matches(U"ack"));
    BOOST_TEST(bookends.matches(U"lac"));
    BOOST_TEST(bookends.matches(U"sack"));
    BOOST_TEST(bookends.matches(U"stack"));
    BOOST_TEST(bookends.matches(U"backend"));
    BOOST_TEST(!bookends.matches(U""));
    BOOST_TEST(!bookends.matches(U"a"));
    BOOST_TEST(!bookends.matches(U"c"));
    BOOST_TEST(!bookends.matches(U"abc"));
    BOOST_TEST(!bookends.matches(U"stuck"));
    BOOST_TEST(!bookends.matches(U"dark"));
}

BOOST_AUTO_TEST_CASE(test_escape_underscore) {
    like_matcher last(UR"(a\_)");
    BOOST_TEST(last.matches(U"a_"));
    BOOST_TEST(!last.matches(U"ab"));

    like_matcher mid(UR"(a\__)");
    BOOST_TEST(mid.matches(U"a_Ш"));
    BOOST_TEST(!mid.matches(U"abc"));

    like_matcher first(UR"(\__)");
    BOOST_TEST(first.matches(U"_Ш"));
    BOOST_TEST(!first.matches(U"a_"));
}

BOOST_AUTO_TEST_CASE(test_escape_percent) {
    like_matcher last(UR"(a\%)");
    BOOST_TEST(last.matches(U"a%"));
    BOOST_TEST(!last.matches(U"ab"));
    BOOST_TEST(!last.matches(U"abc"));

    like_matcher perc2(UR"(a%\%)");
    BOOST_TEST(perc2.matches(U"a%"));
    BOOST_TEST(perc2.matches(U"ab%"));
    BOOST_TEST(perc2.matches(U"aШШШШШШШШШШ%"));
    BOOST_TEST(!perc2.matches(U"a"));
    BOOST_TEST(!perc2.matches(U"abcd"));

    like_matcher mid1(UR"(a\%z)");
    BOOST_TEST(mid1.matches(U"a%z"));
    BOOST_TEST(!mid1.matches(U"az"));
    BOOST_TEST(!mid1.matches(U"a.z"));
    BOOST_TEST(!mid1.matches(U"a%.z"));

    like_matcher mid2(UR"(a%\%z)");
    BOOST_TEST(mid2.matches(U"a%z"));
    BOOST_TEST(mid2.matches(U"aa%z"));
    BOOST_TEST(mid2.matches(U"aШШШШШШШШШШza%z"));
    BOOST_TEST(!mid2.matches(U"az"));
    BOOST_TEST(!mid2.matches(U"a.z"));
    BOOST_TEST(!mid2.matches(U"a%.z"));

    like_matcher mid3(UR"(%\%\%.)");
    BOOST_TEST(mid3.matches(U"%%."));
    BOOST_TEST(mid3.matches(U".%%."));
    BOOST_TEST(mid3.matches(U"abcdefgh%%."));
    BOOST_TEST(!mid3.matches(U"%%"));
    BOOST_TEST(!mid3.matches(U"%."));
    BOOST_TEST(!mid3.matches(U".%%.extra"));

    like_matcher first1(UR"(\%%)");
    BOOST_TEST(first1.matches(U"%"));
    BOOST_TEST(first1.matches(U"%."));
    BOOST_TEST(first1.matches(U"%abcdefgh"));
    BOOST_TEST(first1.matches(U"%ШШШШШШШШШШ%"));
    BOOST_TEST(!first1.matches(U""));
    BOOST_TEST(!first1.matches(U"a%"));
    BOOST_TEST(!first1.matches(U"abcde"));

    like_matcher first2(UR"(\%a%z)");
    BOOST_TEST(first2.matches(U"%az"));
    BOOST_TEST(first2.matches(U"%azzzzzzz"));
    BOOST_TEST(first2.matches(U"%a.z"));
    BOOST_TEST(!first2.matches(U""));
    BOOST_TEST(!first2.matches(U"%"));
    BOOST_TEST(!first2.matches(U"%a"));

    like_matcher cons(UR"(a\%\%z)");
    BOOST_TEST(cons.matches(U"a%%z"));
    BOOST_TEST(!cons.matches(U"a%+%z"));
}

BOOST_AUTO_TEST_CASE(test_escape_any_char) {
    like_matcher period(UR"(a\.)");
    BOOST_TEST(period.matches(U"a."));
    BOOST_TEST(!period.matches(U"az"));

    like_matcher b(UR"(\bc)");
    BOOST_TEST(b.matches(U"bc"));
    BOOST_TEST(!b.matches(UR"(\bc)"));

    like_matcher sh(UR"(\Ш)");
    BOOST_TEST(sh.matches(U"Ш"));
    BOOST_TEST(!sh.matches(U"ШШ"));
    BOOST_TEST(!sh.matches(UR"(\Ш)"));

    like_matcher backslash1(UR"(a\\c)");
    BOOST_TEST(backslash1.matches(UR"(a\c)"));
    BOOST_TEST(!backslash1.matches(UR"(a\\c)"));

    like_matcher backslash2(UR"(a\\)");
    BOOST_TEST(backslash2.matches(UR"(a\)"));
    BOOST_TEST(!backslash2.matches(UR"(a\\)"));
}

BOOST_AUTO_TEST_CASE(test_single_backslash_at_end) {
    like_matcher m(UR"(a%\)");
    BOOST_TEST(m.matches(UR"(a\)"));
    BOOST_TEST(m.matches(UR"(az\)"));
    BOOST_TEST(m.matches(UR"(aaaaaaaaaaaaaaaaaaaaaa\)"));
    BOOST_TEST(!m.matches(U"a"));
    BOOST_TEST(!m.matches(U"az"));
    BOOST_TEST(!m.matches(UR"(a\\a)"));
}

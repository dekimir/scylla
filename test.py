#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 ScyllaDB
#

#
# This file is part of Scylla.
#
# Scylla is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Scylla is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
#
import os
import sys
import argparse
import subprocess
import concurrent.futures
import io
import multiprocessing
import xml.etree.ElementTree as ET

boost_tests = [
    'bytes_ostream_test',
    'chunked_vector_test',
    'compress_test',
    'continuous_data_consumer_test',
    'types_test',
    'keys_test',
    'mutation_test',
    'mvcc_test',
    'schema_registry_test',
    'range_test',
    ('mutation_reader_test', '-c{} -m2G'.format(min(os.cpu_count(), 3))),
    'serialized_action_test',
    'cdc_test',
    'cql_query_test',
    'user_types_test',
    'user_function_test',
    'secondary_index_test',
    'json_cql_query_test',
    'filtering_test',
    'storage_proxy_test',
    'schema_change_test',
    'sstable_mutation_test',
    'sstable_resharding_test',
    'commitlog_test',
    'hash_test',
    'test-serialization',
    'cartesian_product_test',
    'allocation_strategy_test',
    'UUID_test',
    'compound_test',
    'murmur_hash_test',
    'partitioner_test',
    'frozen_mutation_test',
    'canonical_mutation_test',
    'gossiping_property_file_snitch_test',
    'row_cache_test',
    'cache_flat_mutation_reader_test',
    'network_topology_strategy_test',
    'query_processor_test',
    'batchlog_manager_test',
    'logalloc_test',
    'log_heap_test',
    'crc_test',
    'checksum_utils_test',
    'flush_queue_test',
    'config_test',
    'dynamic_bitset_test',
    'gossip_test',
    'managed_vector_test',
    'map_difference_test',
    'memtable_test',
    'mutation_query_test',
    'snitch_reset_test',
    'auth_test',
    'idl_test',
    'range_tombstone_list_test',
    'mutation_fragment_test',
    'flat_mutation_reader_test',
    'anchorless_list_test',
    'database_test',
    'input_stream_test',
    'nonwrapping_range_test',
    'virtual_reader_test',
    'counter_test',
    'cell_locker_test',
    'view_schema_test',
    'view_build_test',
    'view_complex_test',
    'clustering_ranges_walker_test',
    'vint_serialization_test',
    'duration_test',
    'loading_cache_test',
    'castas_fcts_test',
    'big_decimal_test',
    'aggregate_fcts_test',
    'role_manager_test',
    'caching_options_test',
    'auth_resource_test',
    'cql_auth_query_test',
    'enum_set_test',
    'extensions_test',
    'cql_auth_syntax_test',
    'querier_cache',
    'limiting_data_source_test',
    ('sstable_test', '-c1 -m2G'),
    ('sstable_datafile_test', '-c1 -m2G'),
    'broken_sstable_test',
    ('sstable_3_x_test', '-c1 -m2G'),
    'meta_test',
    'reusable_buffer_test',
    'mutation_writer_test',
    'observable_test',
    'transport_test',
    'fragmented_temporary_buffer_test',
    'auth_passwords_test',
    'multishard_mutation_query_test',
    'top_k_test',
    'utf8_test',
    'small_vector_test',
    'data_listeners_test',
    'truncation_migration_test',
    'like_matcher_test',
    'enum_option_test',
]

other_tests = [
    'memory_footprint',
]

long_tests = [
    ('lsa_async_eviction_test', '-c1 -m200M --size 1024 --batch 3000 --count 2000000'),
    ('lsa_sync_eviction_test', '-c1 -m100M --count 10 --standard-object-size 3000000'),
    ('lsa_sync_eviction_test', '-c1 -m100M --count 24000 --standard-object-size 2048'),
    ('lsa_sync_eviction_test', '-c1 -m1G --count 4000000 --standard-object-size 128'),
    ('row_cache_alloc_stress', '-c1 -m2G'),
    ('row_cache_stress_test', '-c1 -m1G --seconds 10'),
]

CONCOLORS = {'green': '\033[1;32m', 'red': '\033[1;31m', 'nocolor': '\033[0m'}

def colorformat(msg, **kwargs):
    fmt = dict(CONCOLORS)
    fmt.update(kwargs)
    return msg.format(**fmt)

def status_to_string(success):
    if success:
        status = colorformat("{green}PASSED{nocolor}") if os.isatty(sys.stdout.fileno()) else "PASSED"
    else:
        status = colorformat("{red}FAILED{nocolor}") if os.isatty(sys.stdout.fileno()) else "FAILED"

    return status

class UnitTest:
    standard_args = '--overprovisioned --unsafe-bypass-fsync 1 --blocked-reactor-notify-ms 2000000 --collectd 0'.split()
    seastar_args = '-c2 -m2G'

    def __init__(self, test_no, name, opts, kind, mode, options):
        if opts is None:
            opts = UnitTest.seastar_args
        self.id = test_no
        self.name = name
        self.mode = mode
        self.path = os.path.join('build', self.mode, 'tests', self.name)
        self.kind = kind
        self.args = opts.split() + UnitTest.standard_args

        if self.kind == 'boost':
            boost_args = []
            if options.jenkins:
                mode = 'debug' if self.mode == 'debug' else 'release'
                xmlout = options.jenkins + "." + mode + "." + self.name + "." + str(self.id) + ".boost.xml"
                boost_args += ['--report_level=no', '--logger=HRF,test_suite:XML,test_suite,' + xmlout]
            boost_args += ['--']
            self.args = boost_args + self.args


def print_progress(test, success, cookie, verbose):
    if type(cookie) is int:
        cookie = (0, 1, cookie)

    last_len, n, n_total = cookie
    msg = "[{}/{}] {} {} {}".format(n, n_total, status_to_string(success), test.path, ' '.join(test.args))
    if verbose == False and sys.stdout.isatty():
        print('\r' + ' ' * last_len, end='')
        last_len = len(msg)
        print('\r' + msg, end='')
    else:
        print(msg)

    return (last_len, n + 1, n_total)


def run_test(test, options):
    file = io.StringIO()

    def report_error(out):
        print('=== stdout START ===', file=file)
        print(out, file=file)
        print('=== stdout END ===', file=file)
    success = False
    try:
        subprocess.check_output([test.path] + test.args,
                stderr=subprocess.STDOUT,
                timeout=options.timeout,
                env=dict(os.environ,
                    UBSAN_OPTIONS='print_stacktrace=1',
                    BOOST_TEST_CATCH_SYSTEM_ERRORS='no'),
                preexec_fn=os.setsid)
        success = True
    except subprocess.TimeoutExpired as e:
        print('  timed out', file=file)
        report_error(e.output.decode(encoding='UTF-8'))
    except subprocess.CalledProcessError as e:
        print('  with error code {code}\n'.format(code=e.returncode), file=file)
        report_error(e.output.decode(encoding='UTF-8'))
    except Exception as e:
        print('  with error {e}\n'.format(e=e), file=file)
        report_error(e)
    return (test, success, file.getvalue())


class Alarm(Exception):
    pass


def alarm_handler(signum, frame):
    raise Alarm


def parse_cmd_line():
    """ Print usage and process command line options. """
    all_modes = ['debug', 'release', 'dev', 'sanitize']
    sysmem = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
    testmem = 2e9
    cpus_per_test_job = 1
    default_num_jobs_mem = ((sysmem - 4e9) // testmem)
    default_num_jobs_cpu = multiprocessing.cpu_count() // cpus_per_test_job
    default_num_jobs = min(default_num_jobs_mem, default_num_jobs_cpu)

    parser = argparse.ArgumentParser(description="Scylla test runner")
    parser.add_argument('--fast', action="store_true",
                        help="Run only fast tests")
    parser.add_argument('--name', action="store",
                        help="Run only test whose name contains given string")
    parser.add_argument('--mode', choices=all_modes, action="append", dest="modes",
                        help="Run only tests for given build mode(s)")
    parser.add_argument('--repeat', action="store", default="1", type=int,
                        help="number of times to repeat test execution")
    parser.add_argument('--timeout', action="store", default="3000", type=int,
                        help="timeout value for test execution")
    parser.add_argument('--jenkins', action="store",
                        help="jenkins output file prefix")
    parser.add_argument('--verbose', '-v', action='store_true', default=False,
                        help='Verbose reporting')
    parser.add_argument('--jobs', '-j', action="store", default=default_num_jobs, type=int,
                        help="Number of jobs to use for running the tests")
    parser.add_argument('--xunit', action="store",
                        help="Name of a file to write results of non-boost tests to in xunit format")
    args = parser.parse_args()

    if not args.modes:
        out = subprocess.Popen(['ninja', 'mode_list'], stdout=subprocess.PIPE).communicate()[0].decode()
        # [1/1] List configured modes
        # debug release dev
        args.modes = out.split('\n')[1].split(' ')

    return args


def find_tests(options):

    tests_to_run = []

    for mode in options.modes:
        def add_test_list(lst, kind):
            for t in lst:
                tests_to_run.append((t, None, kind, mode) if type(t) == str else (*t, kind, mode))

        add_test_list(other_tests, 'other')
        add_test_list(boost_tests, 'boost')
        if mode in ['release', 'dev']:
            add_test_list(long_tests, 'other')

    if options.name:
        tests_to_run = [t for t in tests_to_run if options.name in t[0]]
        if not tests_to_run:
            print("Test {} not found".format(options.name))
            sys.exit(1)

    tests_to_run = [t for t in tests_to_run for _ in range(options.repeat)]
    tests_to_run = [UnitTest(test_no, *t, options) for test_no, t in enumerate(tests_to_run)]

    return tests_to_run


def run_all_tests(tests_to_run, options):
    failed_tests = []

    executor = concurrent.futures.ThreadPoolExecutor(max_workers=options.jobs)
    futures = []
    for test in tests_to_run:
        futures.append(executor.submit(run_test, test, options))

    results = []
    cookie = len(futures)
    for future in concurrent.futures.as_completed(futures):
        result = future.result()
        results.append(result)
        test, success, out = result
        cookie = print_progress(test, success, cookie, options.verbose)
        if not success:
            failed_tests.append((test, out))
    return failed_tests, results


def print_summary(failed_tests, total_tests):
    if not failed_tests:
        print('\nOK.')
    else:
        print('\n\nOutput of the failed tests:')
        for test, out in failed_tests:
            print("Test {} {} failed:\n{}".format(test.path, ' '.join(test.args), out))
        print('\n\nThe following test(s) have failed:')
        for test, _ in failed_tests:
            print('  {} {}'.format(test.path, ' '.join(test.args)))
        print('\nSummary: {} of the total {} tests failed'.format(len(failed_tests), total_tests))

def write_xunit_report(results):
    other_results = [r for r in results if r[0].kind != 'boost']
    num_other_failed = sum(1 for r in other_results if not r[1])

    xml_results = ET.Element('testsuite', name='non-boost tests',
            tests=str(len(other_results)), failures=str(num_other_failed), errors='0')

    for test, success, out in other_results:
        xml_res = ET.SubElement(xml_results, 'testcase', name=test.path)
        if not success:
            xml_fail = ET.SubElement(xml_res, 'failure')
            xml_fail.text = "Test {} {} failed:\n{}".format(test.path, ' '.join(test.args), out)
    with open(options.xunit, "w") as f:
        ET.ElementTree(xml_results).write(f, encoding="unicode")

if __name__ == "__main__":

    options = parse_cmd_line()

    tests_to_run = find_tests(options)

    failed_tests, results = run_all_tests(tests_to_run, options)

    print_summary(failed_tests, len(tests_to_run))

    if options.xunit:
        write_xunit_report(results)

    if failed_tests:
        sys.exit(1)

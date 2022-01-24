import socket
import time
import os
import sys
from distutils.version import LooseVersion

import pytest
from _pytest.runner import TestReport
from _pytest.terminal import TerminalReporter
import os
#from psutil import Process
from collections import namedtuple
from itertools import groupby
import tracemalloc

from . import invalid_data
from .test_base_class import TestBaseClass
aerospike = pytest.importorskip("aerospike")


# _proc = Process(os.getpid())

# from itertools import groupby

# LEAK_LIMIT = 0

# def get_consumed_ram():
#     return _proc.memory_info().rss

# START = 'START'
# END = 'END'
# ConsumedRamLogEntry = namedtuple('ConsumedRamLogEntry', ('nodeid', 'on', 'consumed_ram'))
# consumed_ram_log = []
# ConsumedTracemallocLogEntry = namedtuple('ConsumedTracemallocLogEntry', ('nodeid', 'on', 'consumed_tracemalloc'))
# consumed_tracemalloc_log = []

# tracemalloc.start(10)
# snapshot1 = []
# snapshot2 = []

# @pytest.hookimpl(hookwrapper=True)
# def pytest_terminal_summary(terminalreporter):  # type: (TerminalReporter) -> generator
#     yield
#     # you can do here anything - I just print report info
#     print('*' * 8 + 'HERE CUSTOM LOGIC' + '*' * 8)

#     for failed in terminalreporter.stats.get('failed', []):  # type: TestReport
#         print('failed! node_id:%s, duration: %s' % (failed.nodeid,
#                                                                  failed.duration))

#     for passed in terminalreporter.stats.get('passed', []):  # type: TestReport
#         print('passed! node_id:%s, duration: %s, details: %s' % (passed.nodeid,
#                                                                  passed.duration,
#                                                                  str(passed.longrepr)))

#     grouped = groupby(consumed_ram_log, lambda entry: entry.nodeid)
#     for nodeid, (start_entry, end_entry) in grouped:
#         leaked = end_entry.consumed_ram - start_entry.consumed_ram
#         if leaked > LEAK_LIMIT:
#             terminalreporter.write('LEAKED {}KB in {}\n'.format(
#                 leaked / 1024, nodeid))

#     tmgrouped = groupby(consumed_tracemalloc_log, lambda entry: entry.nodeid)
#     for nodeid, (start_entry, end_entry) in tmgrouped:
#         stats = end_entry.consumed_tracemalloc.compare_to(start_entry.consumed_tracemalloc, 'lineno')
#         print(f"{nodeid}:");
#         for stat in stats[:3]:
#             print(stat);
#             #terminalreporter.write(stats)

# def pytest_runtest_setup(item):
#     log_entry = ConsumedRamLogEntry(item.nodeid, START, get_consumed_ram())
#     consumed_ram_log.append(log_entry)

#     tmlog_entry = ConsumedTracemallocLogEntry(item.nodeid, START, tracemalloc.take_snapshot())
#     consumed_tracemalloc_log.append(tmlog_entry)



# def pytest_runtest_teardown(item):
#     log_entry = ConsumedRamLogEntry(item.nodeid, END, get_consumed_ram())
#     consumed_ram_log.append(log_entry)

#     tmlog_entry = ConsumedTracemallocLogEntry(item.nodeid, END, tracemalloc.take_snapshot())
#     consumed_tracemalloc_log.append(tmlog_entry)
 

def compare_server_versions(version1, version2):
    '''
    Compare two strings version1 and version 2

    Returns:
    -1 if version1 < version2
    0 if version1 == version2
    1 if version1 > version2
    '''
    version1_pre = 'pre' in version1
    version2_pre = 'pre' in version2

    # Remove any suffix and build version of that
    loose_version1 = LooseVersion(version1.split('-')[0])
    loose_version2 = LooseVersion(version2.split('-')[0])

    if loose_version1 < loose_version2:
        return -1
    elif loose_version1 > loose_version2:
        return 1

    # Versions without suffix match

    # Both are preleases or neither is a prelease
    if version1_pre == version2_pre:
        return 0

    # Version 1 is pre release
    if version1_pre:
        return -1

    return 1

def wait_for_port(address, port, interval=0.1, timeout=60):
    """Wait for a TCP / IP port to accept a connection.

    : param port: The port to check.
    : param interval: The interval(seconds) between checks.
    : param timeout: The total time(seconds) to check before quiting.
    """
    start = time.time()
    while True:
        current = time.time()
        if current - start >= timeout:
            break
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((address, port))
            s.close()
            return True
        except Exception as e:
            pass
        time.sleep(interval)
    return False


@pytest.fixture(scope="class")
def as_connection(request):
    config = TestBaseClass.get_connection_config()
    lua_user_path = os.path.join(sys.exec_prefix, "aerospike", "usr-lua")
    lua_info = {'user_path': lua_user_path}
    config['lua'] = lua_info
    # print(config)
    as_client = None
    if len(config['hosts']) == 2:
        for (a, p) in config['hosts']:
            wait_for_port(a, p)
    # We are using tls otherwise, so rely on the server being ready

    if config['user'] is None and config['password'] is None:
        as_client = aerospike.client(config).connect()
    else:
        as_client = aerospike.client(config).connect(config['user'], config['password'])

    request.cls.skip_old_server = True
    request.cls.server_version = []
    versioninfo = as_client.info_all('build')
    for keys in versioninfo:
        for value in versioninfo[keys]:
            if value is not None:
                version_str = value.strip()
                versionlist = version_str.split('.')
                request.cls.string_server_version = version_str
                request.cls.server_version = [int(n) for n in versionlist[:2]]
                if (
                        (int(versionlist[0]) > 3) or
                        (int(versionlist[0]) == 3 and int(versionlist[1]) >= 7)):
                    request.cls.skip_old_server = False
                TestBaseClass.major_ver = int(versionlist[0])
                TestBaseClass.minor_ver = int(versionlist[1])

    request.cls.as_connection = as_client

    def close_connection():
        as_client.close()

    request.addfinalizer(close_connection)
    return as_client


@pytest.fixture(scope='class')
def connection_with_udf(request, as_connection):
    """
    Injects an as_client() as a dependency, loads a udf to it,
    and at the end of the test class removes it.
    Note: if the requesting class does not have a class attr:
    `udf_to_load`, this is essentially a noop
    """
    udf_status = {'loaded': False, 'name': None}
    # if the class doesn't have the correct information,
    # don't bother loading a UDF
    if hasattr(request.cls, 'udf_to_load'):
        udf_status['name'] = request.cls.udf_to_load
        as_connection.udf_put(udf_status['name'], 0, {})
        udf_status['loaded'] = True

    # Yield to the requesting context
    yield as_connection

    # If a UDF has been loaded, remove it
    if udf_status['loaded']:
        try:
            as_connection.udf_remove(udf_status['name'])
        except:  # If this fails, it has already been removed
            pass


@pytest.fixture(scope='class')
def connection_with_config_funcs(request, as_connection):
    """
    Injects a connected as_client() as a dependency, and runs arbitrary
    setup functions on it, ideally these would be expensive functions
    which should only be run once per test category: indexing
    loading udfs. Then yields the connection back to the requesting context
    """
    setup_status = False
    if hasattr(request.cls, 'connection_setup_functions'):
        for func in request.cls.connection_setup_functions:
            func(as_connection)
        setup_status = True

    # Yield to the requesting context
    yield as_connection

    # If any setup was done, run the corresponding teardown functions
    if setup_status and hasattr(request.cls, 'connection_teardown_functions'):
        for func in request.cls.connection_teardown_functions:
            func(as_connection)
        setup_status = False


@pytest.fixture()
def put_data(request):
    put_data.key = None
    put_data.keys = []
    put_data.client = None

    def put_key(client, _key, _record, _meta=None, _policy=None):
        put_data.key = _key
        put_data.client = client
        try:
            client.remove(_key)
        except:
            pass
        put_data.keys.append(put_data.key)
        return client.put(_key, _record, _meta, _policy)

    def remove_key():
        try:
            # pytest.set_trace()
            for key in put_data.keys:
                put_data.client.remove(key)
        except:
            pass

    request.addfinalizer(remove_key)
    return put_key


@pytest.fixture(scope="class")
def connection_config(request):
    """
    Sets the class attribute to be the config object passed in
     to create the as_connection
    """
    config = TestBaseClass.get_connection_config()
    request.cls.connection_config = config


@pytest.fixture(params=invalid_data.INVALID_KEYS)
def invalid_key(request):
    yield request.param

# aerospike.set_log_level(aerospike.LOG_LEVEL_DEBUG)
# aerospike.set_log_handler(None)
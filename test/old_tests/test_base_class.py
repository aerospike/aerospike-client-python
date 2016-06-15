
import socket
import time

try:
    import configparser
except ImportError:
    import ConfigParser as configparser


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


class TestBaseClass(object):
    hostlist = []
    user = None
    password = None
    has_ldt = None
    has_geo = None

    @staticmethod
    def get_hosts():
        config = configparser.ConfigParser()
        config.read("config.conf")
        if config.has_option('enterprise-edition', 'hosts'):
            hosts_str = config.get('enterprise-edition', 'hosts')
            if hosts_str != 'None':
                TestBaseClass.hostlist = TestBaseClass.parse_hosts(hosts_str)
        if len(TestBaseClass.hostlist) > 0:
            if config.has_option('enterprise-edition', 'user'):
                TestBaseClass.user = config.get('enterprise-edition', 'user')
            if config.has_option('enterprise-edition', 'password'):
                TestBaseClass.password = config.get(
                    'enterprise-edition', 'password')
        else:
            TestBaseClass.hostlist = TestBaseClass.parse_hosts(
                config.get('community-edition', 'hosts'))

        for (a, p) in TestBaseClass.hostlist:
            wait_for_port(a, p)

        return (TestBaseClass.hostlist, TestBaseClass.user,
                TestBaseClass.password)

    @staticmethod
    def parse_hosts(host_str):
        hosts = []
        host_str = host_str.strip()
        hlist = host_str.split(';')
        for host in hlist:
            host = host.strip().split(':')
            if len(host) == 2:
                host[1] = int(host[1])
                hosts.append(tuple(host))
        return hosts

    @staticmethod
    def has_ldt_support():
        if TestBaseClass.has_ldt is not None:
            return TestBaseClass.has_ldt
        import aerospike
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            client = aerospike.client(config).connect()
        else:
            client = aerospike.client(config).connect(user, password)
        response = client.info(
            'get-config:context=namespace;id=test', config['hosts'])
        namespace_config = list(response.values())[0][1]
        if namespace_config.find('ldt-enabled=true') == -1:
            TestBaseClass.has_ldt = False
        else:
            TestBaseClass.has_ldt = True
        client.close()
        return TestBaseClass.has_ldt

    @staticmethod
    def has_geo_support():
        if TestBaseClass.has_geo is not None:
            return TestBaseClass.has_geo
        import aerospike
        hostlist, user, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        if user is None and password is None:
            client = aerospike.client(config).connect()
        else:
            client = aerospike.client(config).connect(user, password)
        TestBaseClass.has_geo = client.has_geo()
        client.close()
        return TestBaseClass.has_geo

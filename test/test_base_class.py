import pytest
import ConfigParser


class TestBaseClass(object):
    hostlist = []
    user = None
    password = None

    @staticmethod
    def get_hosts():
        config = ConfigParser.ConfigParser()
        config.read("config.conf")
        if config.has_option('enterprise-edition', 'hosts'):
            hosts_str = config.get('enterprise-edition','hosts')
            if hosts_str != 'None':
                TestBaseClass.hostlist = TestBaseClass.parse_hosts(hosts_str)
        if len(TestBaseClass.hostlist) > 0:
            if config.has_option('enterprise-edition', 'user'):
                TestBaseClass.user = config.get('enterprise-edition','user')
            if config.has_option('enterprise-edition', 'password'):
                TestBaseClass.password = config.get('enterprise-edition','password')
        else:
            TestBaseClass.hostlist = TestBaseClass.parse_hosts(
                    config.get('community-edition','hosts'))
        return TestBaseClass.hostlist, TestBaseClass.user, TestBaseClass.password

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


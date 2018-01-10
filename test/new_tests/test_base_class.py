import os
import pytest
try:
    import ConfigParser as configparser
except:
    import configparser
import aerospike


class TestBaseClass(object):
    hostlist = []
    user = None
    password = None
    has_geo = None
    using_tls = False
    using_auth = False
    should_xfail = False

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
                TestBaseClass.using_auth = True
        else:
            TestBaseClass.hostlist = TestBaseClass.parse_hosts(
                config.get('community-edition', 'hosts'))
        print(TestBaseClass.using_auth)
        return TestBaseClass.hostlist, TestBaseClass.user, TestBaseClass.password

    @staticmethod
    def temporary_xfail():
        if TestBaseClass.should_xfail:
            return True
        else:
            env_val = os.environ.get('TEMPORARY_XFAIL')
            TestBaseClass.should_xfail = bool(env_val)
        return TestBaseClass.should_xfail

    @staticmethod
    def get_tls_info():
        tls_dict = {}
        config = configparser.ConfigParser()
        config.read("config.conf")

        if (config.has_option('tls', 'enable') and (
           config.get('tls', 'enable') == 'True')):
            TestBaseClass.using_tls = True
            tls_dict['enable'] = True

            if config.has_option('tls', 'cafile'):
                tls_dict['cafile'] = config.get('tls', 'cafile')

            if config.has_option('tls', 'capath'):
                tls_dict['capath'] = config.get('tls', 'capath')

            if config.has_option('tls', 'protocols'):
                tls_dict['protocols'] = config.get('tls', 'protocols')

            if config.has_option('tls', 'cipher_suite'):
                tls_dict['cipher_suite'] = config.get('tls', 'cipher_suite')

            if config.has_option('tls', 'keyfile'):
                tls_dict['keyfile'] = config.get('tls', 'keyfile')

            if config.has_option('tls', 'certfile'):
                tls_dict['certfile'] = config.get('tls', 'certfile')

            if config.has_option('tls', 'crl_check'):
                if config.get('tls', 'crl_check') == 'True':
                    tls_dict['crl_check'] = True

            if config.has_option('tls', 'crl_check_all'):
                if config.get('tls', 'crl_check_all') == 'True':
                    tls_dict['crl_check_all'] = True

            if config.has_option('tls', 'log_session_info'):
                if config.get('tls', 'log_session_info') == 'True':
                    tls_dict['log_session_info'] = True

            if config.has_option('tls', 'max_socket_idle'):
                tls_dict['max_socket_idle'] = int(
                    config.get('tls', 'max_socket_idle'))
        else:
            return tls_dict
        return tls_dict

    @staticmethod
    def parse_hosts(host_str):
        hosts = []
        host_str = host_str.strip()

        hlist = host_str.split(';')
        for host in hlist:
            tls_name = None
            if host.rfind('|') != -1:
                index = host.rfind('|')
                tls_name = host[index + 1:]
                host = host[:index]

            host = host.strip()
            last_colon = host.rfind(':')
            if last_colon != -1:
                host = [host[:last_colon], host[last_colon + 1:]]
            else:
                host = [host]
            if len(host) == 2:
                host[1] = int(host[1])
                if tls_name:
                    host.append(tls_name)
                hosts.append(tuple(host))
        return hosts

    @staticmethod
    def has_geo_support():
        if TestBaseClass.has_geo is not None:
            return TestBaseClass.has_geo

        client = TestBaseClass.get_new_connection()
        TestBaseClass.has_geo = client.has_geo()
        client.close()
        return TestBaseClass.has_geo

    @staticmethod
    def get_new_connection(add_config=None):
        '''
        Merge the add_config dict with that in the default configuration
        and return a newly connected client
        '''
        add_config = add_config if add_config else {}
        hostlist, username, password = TestBaseClass.get_hosts()
        config = {'hosts': hostlist}
        tls_dict = {}
        tls_dict = TestBaseClass.get_tls_info()

        config['tls'] = tls_dict
        for key in add_config:
            config[key] = add_config[key]

        client = aerospike.client(config)
        if username and password:
            client.connect(username, password)
        else:
            client.connect()

        return client

    @staticmethod
    def tls_in_use():
        if TestBaseClass.using_tls:
            return True
        else:
            TestBaseClass.get_tls_info()

        return TestBaseClass.using_tls

    @staticmethod
    def auth_in_use():
        if TestBaseClass.using_auth:
            return True
        else:
            TestBaseClass.get_hosts()
        return TestBaseClass.using_auth

    @staticmethod
    def get_connection_config():
        config = {}
        hosts, _, _ = TestBaseClass.get_hosts()
        tls_conf = TestBaseClass.get_tls_info()
        config['hosts'] = hosts
        config['tls'] = tls_conf
        return config

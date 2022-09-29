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
    using_tls = False
    using_auth = False
    should_xfail = False
    using_enterprise = False
    major_ver=0
    minor_ver=0

    @staticmethod
    def get_hosts():
        config = configparser.ConfigParser()
        config.read("config.conf")
        if config.has_option('enterprise-edition', 'hosts'):
            hosts_str = config.get('enterprise-edition', 'hosts')
            if hosts_str != 'None':
                TestBaseClass.hostlist = TestBaseClass.parse_hosts(hosts_str)
                if TestBaseClass.hostlist:
                    TestBaseClass.using_enterprise = True
        if len(TestBaseClass.hostlist) > 0:
            if config.has_option('enterprise-edition', 'user'):
                TestBaseClass.user = config.get('enterprise-edition', 'user')
            if config.has_option('enterprise-edition', 'password'):
                TestBaseClass.password = config.get(
                    'enterprise-edition', 'password')

                # If the password is empty, assume we aren't using authentication
                if not TestBaseClass.password.strip():
                    TestBaseClass.password = None
                    TestBaseClass.user = None
                else:
                    TestBaseClass.using_auth = True
        else:
            TestBaseClass.hostlist = TestBaseClass.parse_hosts(
                config.get('community-edition', 'hosts'))

        return TestBaseClass.hostlist

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
                config.get('tls', 'enable') != '' and
                config.get('tls', 'enable') != '0')):
            TestBaseClass.using_tls = True
            tls_dict['enable'] = True

            if config.has_option('tls', 'cafile'):
                if config.get('tls', 'cafile') != '':
                    tls_dict['cafile'] = config.get('tls', 'cafile')

            if config.has_option('tls', 'capath'):
                if config.get('tls', 'capath') != '':
                    tls_dict['capath'] = config.get('tls', 'capath')

            if config.has_option('tls', 'protocols'):
                if config.get('tls', 'protocols') != '':
                    tls_dict['protocols'] = config.get('tls', 'protocols')

            if config.has_option('tls', 'cipher_suite'):
                if config.get('tls', 'cipher_suite') != '':
                    tls_dict['cipher_suite'] = config.get('tls', 'cipher_suite')

            if config.has_option('tls', 'keyfile'):
                if config.get('tls', 'keyfile') != '':
                    tls_dict['keyfile'] = config.get('tls', 'keyfile')

            if config.has_option('tls', 'cert_blacklist'):
                if config.get('tls', 'cert_blacklist') != '':
                    tls_dict['cert_blacklist'] = config.get('tls', 'cert_blacklist')

            if config.has_option('tls', 'certfile'):
                if config.get('tls', 'certfile') != '':
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
    def get_policies_info():
        policies_dict = {}
        config = configparser.ConfigParser()
        config.read("config.conf")

        policies_dict['auth_mode'] = int(0)
        if config.has_option('policies', 'auth_mode'):
            auth_mode = config.get('policies', 'auth_mode')
            if auth_mode != 'None' and auth_mode.isdigit():
                policies_dict['auth_mode'] = int(auth_mode)

        return policies_dict

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
    def get_new_connection(add_config=None):
        '''
        Merge the add_config dict with that in the default configuration
        and return a newly connected client
        '''
        add_config = add_config if add_config else {}
        config = TestBaseClass.get_connection_config()
        for key in add_config:
            config[key] = add_config[key]

        client = aerospike.client(config)
        if config['user'] is None and config['password'] is None:
            client.connect()
        else:
            client.connect(config['user'], config['password'])

        if client is not None:
            build_info = client.info_all("build")
            res = []
            for _, (error, result) in list(build_info.items()):
                res = None if error is not None else result.strip().strip(';').strip(':')
                res = None if res is None or len(res) == 0 else res
                if res is not None:
                    break
            res = res.split('.')
            major_ver = res[0]
            minor_ver = res[1]
            # print("major_ver:", major_ver, "minor_ver:", minor_ver)

        return client

    @staticmethod
    def tls_in_use():
        if TestBaseClass.using_tls:
            return True
        else:
            return True

    @staticmethod
    def auth_in_use():
        if TestBaseClass.using_auth:
            return True
        else:
            return False

    @staticmethod
    def enterprise_in_use():
        if TestBaseClass.using_enterprise:
            return True
        else:
            return False

    @staticmethod
    def get_connection_config():
        config = {}
        hosts_conf = TestBaseClass.get_hosts()
        tls_conf = TestBaseClass.get_tls_info()
        policies_conf = TestBaseClass.get_policies_info()
        config['hosts'] = hosts_conf
        config['tls'] = tls_conf
        config['policies'] = policies_conf
        config['user'] = TestBaseClass.user
        config['password'] = TestBaseClass.password
        # print(config)
        return config

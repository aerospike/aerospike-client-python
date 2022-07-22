# import the module
from __future__ import print_function
import aerospike
from aerospike import exception as ex
import time
import psutil
import os
import sys

first_ref_count = 0
last_ref_count = 0

def run():
    # write_root_cert()

    # Connect once to establish a memory usage baseline.
    connect_to_cluster()

    first_ref_count = sys.gettotalrefcount()
    last_ref_count = first_ref_count

    initial_memory_usage = get_memory_usage_bytes()
    print(f'first_ref_count = {first_ref_count}', file=f)
    print(f'initial memory = {initial_memory_usage}', file=f)
    while True:
        connect_to_cluster()
        time.sleep(.1)
        current_usage = get_memory_usage_bytes()
        print(f'current = {current_usage} / memory increase bytes = {current_usage - initial_memory_usage}', file=f)
        last_ref_count = sys.gettotalrefcount()
        print(f'outstandingref = {last_ref_count-first_ref_count}', file=f)

def write_root_cert():
    root_ca = """-----BEGIN CERTIFICATE-----
MIID3zCCAsegAwIBAgIUBlLiIt5bLzmYTmqMmpFOlorC+4gwDQYJKoZIhvcNAQEL
BQAwfzELMAkGA1UEBhMCVUsxDzANBgNVBAgMBkxvbmRvbjEPMA0GA1UEBwwGTG9u
ZG9uMRYwFAYDVQQKDA1hZXJvc3Bpa2UuY29tMRAwDgYDVQQLDAdTdXBwb3J0MSQw
IgYDVQQDDBtzdXBwb3J0cm9vdGNhLmFlcm9zcGlrZS5jb20wHhcNMjIwMzEwMTEw
MjE4WhcNMzIwMzA3MTEwMjE4WjB/MQswCQYDVQQGEwJVSzEPMA0GA1UECAwGTG9u
ZG9uMQ8wDQYDVQQHDAZMb25kb24xFjAUBgNVBAoMDWFlcm9zcGlrZS5jb20xEDAO
BgNVBAsMB1N1cHBvcnQxJDAiBgNVBAMMG3N1cHBvcnRyb290Y2EuYWVyb3NwaWtl
LmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANcVh1HauyBhOS45
1Uo77MZzNrazhbj2MvAgXnYuH5q0NqYwHa+TNL9XmF1z0s5YEJf00Luz2hKdFkoz
+onRDgUes0Guk7VBxMdiZjcN3OiyIRPhBx2r53jI9hKYKfDh4/Po8V6OQElhaiNT
13lTY2Q3uWPNRiusCfWO8qmVsCnxGL7Ic0luyPLcALyg+eY1cFBuKGowF/1l4UjY
St69FwBtqaW1bYkAyyjFnC6qjsDhwEidLU5fwcGDj29BG/ukn0Lur0FGqipR1+pP
NTF62TxxRaZC9S//N6yf2Kh/oBWMN/o/NQXnccHejcxsQKLs2YtBpAdFwz+2mLDj
d2/FGgMCAwEAAaNTMFEwHQYDVR0OBBYEFPy0HV/n771BfKTewVPH2K3wENg/MB8G
A1UdIwQYMBaAFPy0HV/n771BfKTewVPH2K3wENg/MA8GA1UdEwEB/wQFMAMBAf8w
DQYJKoZIhvcNAQELBQADggEBAA/+sOg6zJ0vCqEC6wcRMKd42pGBGCh/jxd1Vooz
T6Q/uGPrd/yB6YtT+FzilETPTIponuO0i7WP/Mv/h/UuDEH5uciLpxEugkT2Ark/
h2q2sZM4mvQYh15YIMcUOecnPcn3jsuU1s6AHvL9Pl427IgglpvjMHIwxfgCqn1k
Ewvg7l85EATC9Tmf1LuiFzwjREA+4n5NG80IxjS8kGCZ+UxasVww7/W+jXJLemcN
v/wbp0OTEdBWHJY9QKWnBhoBUCaTFHNU38wry5KmWCbQET5x78WFRfAZF2vJSa1W
uoDfEBndiFsBx0kG0uOHERZpEP0XQYq/TBebs/hjj756XdE=
-----END CERTIFICATE-----
"""
    with open("/tmp/root_ca.pem", "w+") as fp:
        fp.write(root_ca)


def get_memory_usage_bytes():
    process = psutil.Process(os.getpid())
    memory_usage = process.memory_info().rss
    return memory_usage


def connect_to_cluster():
    tls_name = 'bob-cluster-a'

    endpoints = [
        ('bob-cluster-a', 4333)]
    aerospike.set_log_level(aerospike.LOG_LEVEL_ERROR)

    hosts = [(address[0], address[1], tls_name) for address in endpoints]

#    print(f'Connecting to {endpoints}')


    config = {
        'hosts': hosts,
        'policies': {'auth_mode': aerospike.AUTH_INTERNAL},
        'tls': {
            'enable': True,
            'cafile': "/code/src/aerospike/enterprise/as-dev-infra/certs/Platinum/cacert.pem",
            'certfile': "/code/src/aerospike/enterprise/as-dev-infra/certs/Client-Chainless/cert.pem",
            'keyfile': "/code/src/aerospike/enterprise/as-dev-infra/certs/Client-Chainless/key.pem",
            'for_login_only': True,
        }
    }
    client = aerospike.client(config).connect('generic_client', 'generic_client')

    client.close()


if __name__ == "__main__":
    f = open('log.txt', 'w')
    run()

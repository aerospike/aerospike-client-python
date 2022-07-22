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

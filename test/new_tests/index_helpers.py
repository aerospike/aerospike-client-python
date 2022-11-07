import aerospike
from aerospike import exception as e
import time


def index_found_in_info_res(res, index_name):
    found = False
    for key in res:
        value = res[key]
        response = value[1]
        if response:
            if index_name in response:
                return True
    return found


def ensure_dropped_index(client, namespace, index_name):
    try:
        client.index_remove(namespace, index_name)
    except (e.IndexNotFound, e.InvalidRequest):
        pass
    retries = 0
    while retries < 10:
        responses = client.info_all("sindex")
        if not index_found_in_info_res(responses, index_name):
            return
        time.sleep(.5)
        retries += 1

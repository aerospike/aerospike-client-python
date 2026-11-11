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
        time.sleep(0.5)
        retries += 1


def wait_for_index(client, namespace: str, index_name: str) -> None:
    # The client returns before every node has built the index, so wait until each node reports it readable.
    ready = f"indexname={index_name}:"
    retries = 0
    while retries < 10:
        responses = client.info_all(f"sindex-list:ns={namespace}")
        listings = [response or "" for _, response in responses.values()]
        if all(any(ready in entry and "state=RW" in entry for entry in listing.split(";")) for listing in listings):
            return
        time.sleep(0.5)
        retries += 1
    raise TimeoutError(f"index {index_name} is not readable on every node")

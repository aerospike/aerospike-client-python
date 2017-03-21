# -*- coding: utf-8 -*-
"""
Helper functions for dealing with the delayed nature of adding and removing UDFs
"""
import time

def wait_for_udf_removal(connection, udf_name, sleep_time=0.25, attempts=8):
    """
    This should be called after a udf_removal to wait for the removal to take place
    If this takes more than sleep_time * attempts seconds, it will quit and return False
    Sets the udf_removed flag on the class, so the removal can be skipped on the teardown method
    """
    iterations = 0
    present = True

    # While the UDF is still in the UDF List, loop
    while present:

        # Break when this has run beyond the attempts threshold
        if iterations > attempts:
            return False

        udf_list = connection.udf_list({'timeout': 100})

        found = False
        # Check if the UDF is in the returned list
        for udf in udf_list:
            if udf['name'] == udf_name:
                found = True
                break

        present = found

        # If it is still present, increment our iterations and pause
        if present:
            iterations += 1
            time.sleep(sleep_time)

    return True


def wait_for_udf_to_exist(connection, udf_name, sleep_time=0.25, attempts=8):
    """
    This should be called after a udf_put to wait for the udf to be stored on the server.
    """
    iterations = 0
    exists = False
    
    # Loop while the UDF is not found on the server
    while not exists:

        # if we have tried too many times, exit
        if iterations > attempts:
            return False
        
        udf_list = connection.udf_list({'timeout': 100})

        for udf in udf_list:

            # if the UDF now exists, exit the method
            if udf['name'] == udf_name:
                exists = True
                return True
        
        # increment and pause
        iterations += 1
        time.sleep(sleep_time)

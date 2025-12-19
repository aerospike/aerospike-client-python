# -*- coding: utf-8 -*-
"""
Helper functions for dealing with the delayed nature of adding and removing UDFs
"""
import time


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

        udf_list = connection.udf_list({"timeout": 100})

        for udf in udf_list:

            # if the UDF now exists, exit the method
            if udf["name"] == udf_name:
                exists = True
                return True

        # increment and pause
        iterations += 1
        time.sleep(sleep_time)

# Enable the logging at application start, before connecting to the server.
import aerospike


## SETTING THE LOG HANDLER ##

# Set log handler to print to the data to the console
aerospike.set_log_handler()

# Clears saved log handler and disable logging
aerospike.set_log_handler(None)

def log_callback(level, func, path, line, msg):
    print("[{}] {}".format(func, msg))

# Set log handler to custom callback function (defined above)
aerospike.set_log_handler(log_callback)


## SETTING THE LOG LEVEL ##

# disables log handling
aerospike.set_log_level(aerospike.LOG_LEVEL_OFF)

# Enables log handling and sets level to LOG_LEVEL_TRACE
aerospike.set_log_level(aerospike.LOG_LEVEL_TRACE)



# Create a client and connect it to the cluster
try:
# This line will print use log_callback to print logs with a log level of TRACE
    client = aerospike.client(config).connect()

except ex.ClientError as e:
    print("Error: {0} [{1}]".format(e.msg, e.code))
    sys.exit(1)

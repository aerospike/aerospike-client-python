import aerospike
import time
import sys
config = {
    "hosts": [
        ("127.0.0.1", 3000)
    ]
}
print(config)
if sys.argv[1] == "true":
    config["user"] = "superuser"
    config["password"] = "superuser"
if len(sys.argv) == 3:
    config["app_id"] = sys.argv[2]
client = aerospike.client(config)
while True:
    time.sleep(1)

#!/bin/sh

# Start up Aerospike server in VM
vagrant up
# Wait for server to start
sleep 3
cd test/
# Install test dependencies
python3 -m pip install -r requirements.txt
python3 -m pytest new_tests/

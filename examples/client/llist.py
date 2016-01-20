# -*- coding: utf-8 -*-
##########################################################################
# Copyright 2013-2016 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##########################################################################

from __future__ import print_function

import aerospike
import sys

from optparse import OptionParser
from aerospike import exception as e

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "--help", dest="help", action="store_true",
    help="Displays this message.")

optparser.add_option(
    "-U", "--username", dest="username", type="string", metavar="<USERNAME>",
    help="Username to connect to database.")

optparser.add_option(
    "-P", "--password", dest="password", type="string", metavar="<PASSWORD>",
    help="Password to connect to database.")

optparser.add_option(
    "-h", "--host", dest="host", type="string", default="127.0.0.1", metavar="<ADDRESS>",
    help="Address of Aerospike server.")

optparser.add_option(
    "-p", "--port", dest="port", type="int", default=3000, metavar="<PORT>",
    help="Port of the Aerospike server.")

(options, args) = optparser.parse_args()

if options.help:
    optparser.print_help()
    print()
    sys.exit(1)

##########################################################################
# Client Configuration
##########################################################################

config = {
    'hosts': [(options.host, options.port)],
    'lua': {'user_path': '.'}
}

##########################################################################
# Application
##########################################################################

try:
    client = aerospike.client(config).connect(
        options.username, options.password)
except e.ClientError as exception:
    print("Error: {0} [{1}]".format(exception.msg, exception.code))
    sys.exit(1)

key = ('test', 'articles', 'The Number One Soft Drink')
tags = client.llist(key, 'tags')
try:
    print("Demonstrating an LList with string type elements")
    print("================================================")
    tags.add("soda")
    tags.add_many(
        ["slurm", "addictive", "prizes", "diet", "royal slurm", "glurmo"])
except e.LDTError as exception:
    print("Error while adding tags: {0} [{1}]".format(
        exception.msg, exception.code))

print("The entire list of elements:")
print(tags.filter())
print("The first two elements:")
print(tags.find_first(2))
print("Removing the element 'prizes'")
try:
    tags.remove("prizes")
except:
    pass
print("The three elements from the end:")
print(tags.find_last(3))
print("A couple of elements from 'glurmo':")
print(tags.find_from("glurmo", 2))

comments = client.llist(key, 'comments')
try:
    print("\n")
    print("Demonstrating an LList with map (dict) type elements")
    print("====================================================")
    comments.add({'key': 'comment-1', 'user': 'blorgulax', 'body': 'First!'})
    comments.add({'key': 'comment-2', 'user': 'fry',
                  'body': 'You deserve a Slurmie', 'parent': 'comment-1'})
    n = comments.size() + 1
    comments.add({'key': 'comment-' + str(n), 'user': 'curlyjoe',
                  'body': 'make it an implosion'})
    comments.add({'key': 'comment-4', 'user': 'queen slurm',
                  'body': "Honey comes out of a bee's behind...", 'parent': 'comment-1'})
except e.LDTError as exception:
    print("Error while adding comments: {0} [{1}]".format(
        exception.msg, exception.code))

print("Getting the first comment:")
print(comments.get("comment-1"))

try:
    # Clean-up
    tags.destroy()
    comments.destroy()
except:
    pass
client.close()

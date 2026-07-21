
##########################################################################
# Copyright 2013-2026 Aerospike, Inc.
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


import aerospike
import json
import re
import sys
import os.path

from optparse import OptionParser
from aerospike import predicates as p

##########################################################################
# Option Parsing
##########################################################################

usage = "usage: %prog [options] where module function [args...]"

optparser = OptionParser(usage=usage, add_help_option=False)

optparser.add_option(
    "-b", "--bins", dest="bins", type="string", action="append",
    help="Bins to select from each record.")

config = {
    'lua': {
        'user_path': os.path.dirname(__file__)
    }
}


def parse_arg(s):
    try:
        return json.loads(s)
    except ValueError:
        return s



# ----------------------------------------------------------------------------
# Perform Operation
# ----------------------------------------------------------------------------

re_bin = "(.{1,14})"
re_str_eq = "\s+=\s*(?:(?:\"(.*)\")|(?:\'(.*)\'))"
re_int_eq = "\s+=\s*(\d+)"
re_int_rg = "\s+between\s+\(\s*(\d+)\s*,\s*(\d+)\s*\)"
re_w = re.compile("%s(?:%s|%s|%s)" %
                    (re_bin, re_str_eq, re_int_eq, re_int_rg))

args.reverse()
where = args.pop()
module = args.pop()
function = args.pop()

# If predicate is provided, then perform a query
q = client.query(namespace, set)
w = re_w.match(where)
if w is not None:
    if w.group(2):
        b = w.group(1)
        v = w.group(2)
        q.where(p.equals(b, v))
    elif w.group(3):
        b = w.group(1)
        v = w.group(3)
        q.where(p.equals(b, v))
    elif w.group(4):
        b = w.group(1)
        v = int(w.group(4))
        q.where(p.equals(b, v))
    elif w.group(5) and w.group(6):
        b = w.group(1)
        l = int(w.group(5))
        u = int(w.group(6))
        q.where(p.between(b, l, u))

if options.bins and len(options.bins) > 0:
    # project specified bins
    q.select(*options.bins)

args.reverse()
argl = list(map(parse_arg, args))
print("argl == ", argl)
q.apply(module, function, *argl)

results = []

# callback to be called for each record read
def callback(result):
    results.append(result)
    print(result)

# invoke the operations, and for each record invoke the callback
q.foreach(callback)

print("---")
if len(results) == 1:
    print("OK, 1 result found.")
else:
    print("OK, %d results found." % len(results))

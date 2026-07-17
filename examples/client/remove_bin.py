
##########################################################################
# Copyright 2013-2021 Aerospike, Inc.
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
import sys

from optparse import OptionParser

##########################################################################
# Options Parsing
##########################################################################

usage = "usage: %prog [options] key bin_names"

from .. import ExampleWithRecord


exitCode = 0

class RemoveBin(ExampleWithRecord):
    def run(self):
        # TODO: both configurable
        # pk
        bin_names = []

        retval = self.client.remove_bin(self.key, bin_names)
        print("Status of bin removal is: %d" % (retval))
        print("OK, bins removed from the record at", self.key)
        # TODO: why RecordNotFound used to map to 602?

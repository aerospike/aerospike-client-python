
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

usage = "usage: %prog [options] module"

        module = args.pop()
        language = aerospike.UDF_TYPE_LUA
        policy = {}

        client.udf_put(module, language, policy)
        udf_contents = client.udf_get(module, language, policy)
        print("Module contents : ")
        print(udf_contents)

    except Exception as e:
        print("error: {0}".format(e), file=sys.stderr)
        exitCode = 2

    # ----------------------------------------------------------------------------
    # Close Connection to Cluster
    # ----------------------------------------------------------------------------

    client.close()

except Exception as eargs:
    print("error: {0}".format(eargs), file=sys.stderr)
    exitCode = 3

##########################################################################
# Exit
##########################################################################

sys.exit(exitCode)

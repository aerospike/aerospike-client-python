from parver import Version
import sys

# Take in a new release version as input
# 15.0.0 -> 15.0.0rc1.dev0
version_string = sys.argv[1]
version = Version.parse(version_string, strict=True)
new_version = version.replace(pre_tag='rc', pre=1, dev=0)
print(new_version)

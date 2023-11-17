from parver import Version
import sys

version_string = sys.argv[1]
version = Version.parse(version_string, strict=True)
new_version = version.replace(pre=None)
print(new_version)

from parver import Version
import sys

# Take in an old dev version as input
# e.g 14.0.0rc1.dev1 -> 14.0.0rc2.dev1 -> 14.0.0rc2.dev0
version_string = sys.argv[1]
version = Version.parse(version_string, strict=True)
new_version = version.bump_pre("rc", by=1)
new_version = new_version.replace(dev=0)
print(new_version)

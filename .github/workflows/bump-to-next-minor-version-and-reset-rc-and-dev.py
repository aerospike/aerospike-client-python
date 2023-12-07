from parver import Version
import sys

# Take in current release version in master as input
# Bump to next minor version and prepare for next RC + dev version
# 15.0.0 -> 15.1.0rc1.dev0
version_string = sys.argv[1]
version = Version.parse(version_string, strict=True)
if version.is_release_candidate:
    print("Must take in a release version", file=sys.stderr)
    exit(1)

version = version.bump_release(index=1)
version = version.replace(pre_tag='rc', pre=1, dev=0)
print(version)

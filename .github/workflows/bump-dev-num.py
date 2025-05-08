from parver import Version
import sys

version_string = sys.argv[1]
version = Version.parse(version_string, strict=True)
if version.is_devrelease:
    version = version.bump_dev()
elif version.is_release_candidate:
    version = version.bump_pre("rc", by=1)
    # Dev numbers should start from 1
    version = version.replace(dev=1)
else:
    # Assume this is a release version
    # Bump to next minor version
    version = version.bump_release(index=1)
    # RC numbers should start from 1
    version = version.replace(pre_tag='rc', pre=1, dev=1)

print(version)

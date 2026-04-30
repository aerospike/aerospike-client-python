from parver import Version
import sys

version_string = sys.argv[1]
version = Version.parse(version_string, strict=True)
if version.is_devrelease:
    version = version.bump_dev()
else:
    # Assume this is a release version
    # Bump dev version to next patch version to be safe
    version = version.bump_release(index=2)
    version = version.replace(dev=1)

print(version)

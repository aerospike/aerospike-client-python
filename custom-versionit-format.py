from parver import Version
import versioningit
from typing import Any, Dict, Union
import pathlib

import versioningit.basics
import versioningit.git
import os

# Take in <version> and <string> as input
# If local version identifier doesn't exist, append +<string> to <version>
# Example: 15.0.0rc1 and dsym as input -> 15.0.0rc1+dsym
# If it does exist, append <version> with .<string>
# Then output new version as str
def append_to_local(version_str: str, value: str) -> str:
    version = Version.parse(version_str, strict=True)
    if version.local == None:
        new_local = value
    else:
        new_local = f"{version.local}.{value}"

    version = version.replace(local=new_local)
    return version.__str__()

def my_vcs(
        project_dir: Union[str, pathlib.Path],
        params: Dict[str, Any]
) -> versioningit.VCSDescription:
    vcs_description = versioningit.git.describe_git(
        project_dir=project_dir,
        params=params
    )
    if vcs_description.state == "exact":
        # We don't want the format step to be skipped
        # Workaround: https://github.com/jwodder/versioningit/issues/42#issuecomment-1235573432
        vcs_description.state = "exact_"
    return vcs_description

def my_format(
        description: versioningit.VCSDescription,
        base_version: str,
        next_version: str,
        params: Dict[str, Any]
) -> str:
    # Even if the repository state matches a tag, we always need to label the version if it's unoptimized or includes
    # dsym for macOS
    if description.state != "exact_":
        version_str = versioningit.basics.basic_format(
            description=description,
            base_version=base_version,
            next_version=next_version,
            params=params
        )
    else:
        version_str = base_version

    if os.getenv("UNOPTIMIZED"):
        version_str = append_to_local(version_str, "unoptimized")
    if os.getenv("INCLUDE_DSYM"):
        version_str = append_to_local(version_str, "dsym")
    if os.getenv("SANITIZER"):
        version_str = append_to_local(version_str, "sanitizer")

    return version_str

import sys

platform_tag_to_runner_name = {
    "manylinux_x86_64": "ubuntu-22.04",
    "manylinux_aarch64": "aerospike_arm_runners_2",
    "macosx_x86_64": "macos-12-large",
    "macosx_arm64": "macos-14",
    "win_amd64": "windows-2022"
}
platform_tag = sys.argv[1]
print(platform_tag_to_runner_name[platform_tag], end=None)

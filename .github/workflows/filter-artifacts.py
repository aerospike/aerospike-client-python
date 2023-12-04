# Must be run in .github/workflows
import os

artifact_tests = {}
artifact_successes = {}

os.chdir("../../matrix-outputs")
file_names = os.listdir()

print("Files in matrix-outputs:", file_names)

for file_name in file_names:
    # File name format:
    # <Artifact name>.<distro name>.txt
    artifact_name, distro_name = file_name.split(".")[0:2]
    if artifact_name not in artifact_tests:
        artifact_tests[artifact_name] = 1
    else:
        artifact_tests[artifact_name] += 1

    with open(file_name) as file:
        test_outcome = file.read().strip()
        # The file may end with a new line, so strip whitespace
        print(f"{artifact_name}, {distro_name}: {test_outcome}")
        if test_outcome == "success":
            if artifact_name not in artifact_successes:
                artifact_successes[artifact_name] = 1
            else:
                artifact_successes[artifact_name] += 1
        else:
            if artifact_name not in artifact_successes:
                artifact_successes[artifact_name] = 0

print(f"Artifact successful test count: {artifact_successes}")
print(f"Artifact test count: {artifact_tests}")

os.chdir("../.github/workflows")

with open("failed_artifacts.txt", "w") as failed_artifacts_file:
    failed_artifact_names = [artifact_name for artifact_name in artifact_tests if artifact_successes[artifact_name] != artifact_tests[artifact_name]]
    print(f"Failed artifacts: {failed_artifact_names}")
    for name in failed_artifact_names:
        failed_artifacts_file.write(f"{name}\n")

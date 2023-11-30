import os

artifact_tests = {}
artifact_successes = {}

os.chdir("matrix-outputs")
file_names = os.listdir()
for file_name in file_names:
    # File name format:
    artifact_name, distro_name = file_name.split(":")
    if artifact_name not in artifact_tests:
        artifact_tests[artifact_name] = 1
    else:
        artifact_tests[artifact_name] += 1

    with open(file_name) as file:
        test_outcome = file.read()
        if test_outcome == "success":
            if artifact_name not in artifact_successes:
                artifact_successes[artifact_name] = 1
            else:
                artifact_successes[artifact_name] += 1
        else:
            if artifact_name not in artifact_successes:
                artifact_successes[artifact_name] = 0

os.chdir("..")

failed_artifact_names = [artifact_name for artifact_name in artifact_tests if artifact_tests[artifact_name] - artifact_successes[artifact_name] > 0]
for name in failed_artifact_names:
    print(name)

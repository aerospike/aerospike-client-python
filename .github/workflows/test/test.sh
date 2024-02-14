# First argument is the branch to test the workflows on
# Second argument is whether to test build wheels or not (true/false. Default: true)
# Third argument is whether to test stage tests or not (true/false. Default: true)

if [[ $1 != 'false' ]]; then
    # echo "Testing build wheels"
    gh workflow run build-wheels.yml --ref $1
    gh workflow run build-wheels.yml -f run_tests=true --ref $1
    gh workflow run build-wheels.yml -f run_tests=true -f use-server-rc=true --ref $1
    gh workflow run build-wheels.yml -f run_tests=true -f server-tag=6.3.0.15 --ref $1
    gh workflow run build-wheels.yml -f run_tests=true -f test-macos-x86=true --ref $1
fi

if [[ $2 != 'false' ]]; then
    # echo "Testing stage tests"
    gh workflow run stage-tests.yml --ref $1
    gh workflow run stage-tests.yml -f use-server-rc=true --ref $1
    gh workflow run stage-tests.yml -f server-tag=6.3.0.15 --ref $1
    gh workflow run stage-tests.yml -f test-macos-x86=true --ref $1
fi

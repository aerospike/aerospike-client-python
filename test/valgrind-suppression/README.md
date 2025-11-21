# Generating valgrind suppressions

## 1. Run the valgrind action with the following arguments

```
--error-limit=no --leak-check=full --gen-suppressions=all --num-callers=350

```

--error-limit=no: Disable the limit of the numbers errors that can be reported

--leak-check=full: Show details for each individual memory error

--gen-suppressions=all: generate suppressions blocks alongside all memory errors

--num-callers=350: include 350 stack frames of context for each error and suppression.

**Note:** Additional stack frames decrease the likelihood of over suppression by increasing the specificity of each suppression.
However, more stack frames also reduce the speed of the test runs. 
Suppressions may not exceed 500 stack frames or else the valgrind will fail with the following error:

```
too many callers in stack trace
```

When --num-callers=500, suppressions may end up having 600+ frames due to inline frames not counting towards the total number of callers.

Setting --num-callers=350 forces the total number of suppressions frames to be below 500 and avoids this error

## 2. Download the results of the valgrind run step

Valgrind action runs available [here]https://github.com/aerospike/aerospike-client-python/actions/workflows/valgrind.yml

## 3. Extract suppressions from logs

Run `extract_suppressions.py` from the `valgrind-suppressions` directory to extract any unique expressions from the valgrind logs.

Script usage:

```
python3 extract_suppressions.py log_input.txt new_suppressions.supp
```

For the full-suite, name the output file new_tests.supp

For a single file, name the output file after the name of the test (test_log.py.supp)

## 4. Add suppression file to valgrind argument

Add the following argument to the valgrind arguments:

```
--suppressions=./valgrind-suppressions/new_suppressions.supp
```

**Note:** If the suppressions are not able to be parsed when running valgrind, look for timestamps in the suppression file.
The script should strip them out, but if the logs are corrupted, they might not be stripped out correctly

## 5. Add any additional suppressions from additional valgrind runs

While the suppressions generated should cover most memory issues, some errors show up intermittendly.

When new false positive error cases pop up in valgrind logs, simply add the newly generated suppressions to the suppression file.

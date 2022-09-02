#!/bin/bash
 
EXPECTED_WHEEL_COUNT=9
 
publish () {
  source ~/pythonenvs/python27/bin/activate
  pushd aerospike-client-python
  twine upload -r pypi dist/*
  popd
  deactivate
}
 
publishTest () {
  source ~/pythonenvs/python27/bin/activate
  pushd aerospike-client-python
  twine upload -r test dist/*
  popd
  deactivate
}
 
###################################
# MAIN
#---------------------------------
 
if [[ $(ls aerospike-client-python/dist/ | wc -l) -ne EXPECTED_WHEEL_COUNT ]]; then
  echo "Wheel count does not match, check that aerospike-client-python/dist has correct artifacts."
  exit 1
fi
 
if [ -n "$1" ]; then
  if [ $1 = "-t" ]; then
    publishTest
  else 
    echo "Use 'publish.sh -t' to publish to test pypy"
  fi
else
  publish
fi

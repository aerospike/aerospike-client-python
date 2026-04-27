import doctest
import unittest
import runpy


def custom_setup(test):
    # This code runs before every doctest in the suite
    # test.globs['shared_data'] = [1, 2, 3]
    # TODO: should use file location and not cwd
    runpy.run_path('./examples/boilerplate.py')

def load_tests(loader, tests, ignore):
    # Add setup and teardown logic here
    # TODO: should use file location and not cwd
    tests.addTests(doctest.DocFileSuite(["./aerospike.rst"]))
    tests.addTests(doctest.DocFileSuite(["./client.rst"], setUp=custom_setup))
    return tests

if __name__ == "__main__":
    unittest.main()

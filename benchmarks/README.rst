Python client Benchmarks
=========================


Prerequisites
~~~~~~~~~~~~~~
To run the benchmarks the python module 'tabulate' needs to be installed. In order to display heap information the module `guppy` must be installed.
Note that `guppy` is only available for Python2. If `guppy` is not installed the benchmarks will still be runnable, but no heap information
will be displayed.

Available Benchmarks
~~~~~~~~~~~~~~~~~~~~~
There are currently two benchmarks provided for the Aerospike Python client:

keygen.py
-------------------
This benchmark will write small distinct records to the database.
Command line usage help is available by running.
::
	python keygen.py --help

It will report:
- Numer of keys generated
- Number of operations
- Runtime
- Operations per second


kvs.py
-------
This benchmark will perform writes and reads to the database. It is possible to specify a read and write ratio.
Command line usage help is available by running.
::
	python kvs.py --help

It will report
- Numer of keys generated
- Number of operations
- Runtime
- Operations per second
- Latency statistics for read and write operations


Example Usage
~~~~~~~~~~~~~~
To run keygen.py against a server located at 127.0.0.1 listening on port 3000 to the set named "benchmark"
::
	python keygen.py -h "127.0.0.1" -p 3000 -s "benchmark"

To run kvs.py against a server located at 127.0.0.1 listening on port 3000 to the set named "benchmark" with 75% reads
and 25% writes of string data with minimum length of 15 and maximum length of 20 characters
::
	python kvs.py -h "127.0.0.1" -p 3000 -s "benchmark" --reads 75 --writes 25 --gen "str" --str-min 15 --str-max 20

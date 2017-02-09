# vertica-udfs
Scalability Analytics Platforms custom UDFS for Vertica, written in Java / C++.

### HyperLogLog
Made of two C++ UDAF (user-defined aggregate functions)

 - HllDistinctCount(VARBINARY)
 - HllCreateSynopsis(VARBINARY)

### How to build this project?
This project relies on cmake as the main building tool. Typically, you create a separate build directory where you run cmake and create target binaries.

```bash
$ git clone git@gitlab.criteois.com:f.jehl/vertica-udfs.git
$ cd vertica-udfs
$ mkdir build
$ cd build
$ cmake ..
$ make
$ file libhll.so 
libhll.so: ELF 64-bit LSB shared object, x86-64
```

### How to test it?
This code comes with a handful of tests based on the Google C++ Testing Framework. To build the tests together with the main code one have to enable BUILD_TESTS in cmake. This can be either done via ccmake, an ncurses interface to cmake...

```bash
$ cd build
$ ccmake ..
# toggle tests to ON, then press 'c', then press 'g'
$ make
$ file hll_test
hll_test: ELF 64-bit LSB executable, x86-64
```
... or directly from the command line:
```bash
$ cd build
$ cmake .. -DBUILD_TESTS=1
$ make
$ file hll_test
hll_test: ELF 64-bit LSB executable, x86-64
```


Once the tests are built, it's enough to run the output binary:
```
$ ./hll_test
[==========] Running 10 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 10 tests from HllTest
[ RUN      ] HllTest.TestErrorWithinRangeForDifferentBucketMasks
[       OK ] HllTest.TestErrorWithinRangeForDifferentBucketMasks (87 ms)
...

[----------] 10 tests from HllTest (486 ms total)

[----------] Global test environment tear-down
[==========] 10 tests from 1 test case ran. (486 ms total)
[  PASSED  ] 10 tests.
```

### How to run integration tests with Docker?
This repository defines a derived Vertica docker container able to run a set of SQL queries put in the /tests/integration folder.
As of today, the only outcome of those tests are the following: they whether run or not (return exit code 0 or not from vsql).
To run the tests in a Docker container and auto destroy them, run the following command:

```
docker run -v <full_path_of_vertica_rpm>:/tmp/vertica.rpm -v docker-vertica-itests:/opt/vertica --cap-add SYS_NICE --cap-add SYS_RESOURCE --name docker-vertica-itests -ti fjehl/docker-vertica-itests && docker rm docker-vertica-itests
```

If the container gets somehow corrupted, destroy the volume with persistent storage (named docker-vertica-itests) with the following command:

```
docker volume rm docker-vertica-itests
```

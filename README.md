# HyperLogLog
This repository contains C++ code of the HyperLogLog algorithm as a User Defined Function (UDF) for HP Vertica. It was created by the Scalability Analytics Platforms team at Criteo.

The algorithm is implemented as two C++ UDAFs (User Defined Aggregate Function):

 - HllDistinctCount(VARBINARY)
 - HllCreateSynopsis(INT)

In the following sections we describe HyperLogLog together with the tweaks to the original algorithm, so that even someone not acquainted with the algorithm might easily get understanding of how it works.

## Introduction

HyperLogLog is an algorithm used for estimating cardinality of a multiset. Multiset is a bag values which don't have to unique. There might be repetitions in a mulitset, meaning that the total number of elements might be different from the number of unique elements.
It's commonly known that cardinality of every data set can be easily calculated with a trivial algorithm whose complexity is linear with the number of elements. However, given the size of some of the data sets in use at Criteo, this approach might be a no go, since we would not be able to fit the data in the memory. This is why we opted to use HyperLogLog, or actually one of its variations, originally described by Flajolet et al. in [1]. Our implementation is strongly based on the HyperLogLog++ algorithm (described in [2]), as well as on some improvements implemented in Redis ([4], [5]) and Druid ([2],[3]).

## How HyperLogLog works?

HyperLogLog provides a way to compute approximate DISTINCT COUNT, known more generally as cardinality of a data set (or multiset). The algorithm achieves this in two phases. First, it aggregates the actual data in a single run over the whole set, creating a so called synopsis. Second, it uses it to give an estimate of the cardinality of the data set with a very low median relative error. An invaluable advantage of the algorithm is that it uses constant space, as opposed to linear space required by the trivial algorithm.

The algorithm is based on a few observations. Imagine that we draw numbers of a fixed length. Without loss of generality we might assume they are 64 bits long. Now, let's look at the bits they start with. If the numbers are distributed uniformly, then: 50% of all numbers will start with 0 and the remaining 50% will start with 1. Along these lines, 25% of the numbers will start with 11, 12.5% will start with 111, 6.125% will start with 1111 etc.

Intuitively, if someone tells you that she has been drawing 64-bit numbers and the longest 0-sequence at the beginning was a single zero, we'd say she has been doing that only for short time. Conversely, if she says the longest sequence was 59 zeros in a row, we'd say she put a lot of effort and time to draw the numbers. Generally speaking, if the longest observed sequence of zeros was q, then a good estimate of the total count of numbers drawn would be 2^(q+1).

Of course, if we are unlucky, we will get a long sequence of leading zeros right with the first number. In turn, this will heavily impact accuracy of our guess. In order to reduce variability we can split the stream of numbers into *m* substreams. To this end, we will take the first *p* bits of a number as a stream's index, and then look at the remaining *64-p bits* (note: *2^p = m*). This technique is called stochastic averaging and was depicted in details in [1]. Every bucket will contain length *q* of the longest encountered 000...01 sequence, i.e. *q-1* zero bits followed by a one bit. If we don't see a hash falling in a particular bucket, its value will be equal to zero. Intuitively, increasing the number of buckets increases accuracy of the estimate.

### An example of HyperLogLog

Imagine that we bake cookies :cookie: A lot of them. We have a log like this where we keep tracks of the cookies baked so far:

timestamp|factory_name|cookie_name
---------|------------|-----------
2016-04-02T12:23:24 | Paris	| Chocolate Chip
2016-04-02T12:23:32	| New York | Peanut Butter and Oatmeal
2016-04-02T12:23:56 | Paris	| Caramel Chip
...	| ... | ...

We want to provide distinct count of cookies by factory or any grain of timestamps above a day in a table that does not have the cookie_name column. Instead, we keep an aggregation of that column as follows:

day | factory_name | cookies_summary
----|--------------|---------
2016-04-02 | Paris | `<some magic summary of data>`
2016-04-02 | New York | `<another magic summary of data>`
... | ... | ...


Using the HyperLogLog algorithm we might estimate cardinality of different cookies made at various factories on a particular day. Below we describe steps necessary to create a synopsis and to calculate distinct count.

#### 1. Hash

First we hash the cookies grouped by day and factory_name using hashing function. Each cookie name is transformed via this function to a 64 bit random value. Thus, for every combination of factory_name and day we have a set of 64 bit hashes.

#### 2. Bucketize and count leading zeroes

For every combination of (factory_name, day) we are going to summarize this set of hashes to a single array of buckets, called a synopsis. Number of buckets is tightly coupled with accuracy of the estimate: the higher it is, the closer the estimate is to the actual cardinality. Let's imagine we pick 16 buckets (substreams). Since log_2(16) = 4, for each hash we will look at the first 4 bits to assign it to a respective bucket.

For the following hash:  *0001* 00000 **1** 11111101010010010
We have:
* In italic, the bucket (1),
* In bold, the first occurrence of 1 in the value bits.

Hence, in the bucket number 1 we store 6, since this is length of the 00...001 pattern in the part following the bucket index bits.

0| 1| 2| 3| 4| 5| 6| 7| 8| 9| 10| 11| 12| 13| 14| 15
---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---
0| 6| 0| 0| 0| 0| 0| 0| 0| 0|  0|  0|  0|  0|  0|  0


As we process the hashes, every time we see a hash having a bigger number of leading zeros than the length stored in the corresponding bucket, we update the value. Once the whole data set is processed, we obtain a filled array.

0| 1| 2| 3| 4| 5| 6| 7| 8| 9| 10| 11| 12| 13| 14| 15
---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---
3| 6| 3| 2| 4| 5| 9| 9| 7| 6|  5|  3|  4|  3|  4|  1

This array is a **synopsis** of the whole data set.

#### 3. Compute the estimate

Our estimate is a bias corrected harmonic mean calculated with the following formula:

![HLL formula](https://adriancolyer.files.wordpress.com/2016/03/hll-harmonic-mean.png?w=600)

In the above formula:
* `E` is the estimate
* `alpha_m` is a constant to correct the bias
* `m` is total number of buckets,
* `M[j]` is value stored in the j-th bucket, i.e. length of the longest 0..01 sequence that fell into the j-th bucket

## Vertica's native implementation

HP's Vertica boasts a HyperLogLog implementation described in [6] and [7]. In order to estimate its performance we ran a couple of benchmarks. **TODO: describe the benchmarks**.

Using Vertica's own implementation turned out to be not acceptable for us for three of reasons.
1. It does not allow creating synopses outside of Vertica. Our goal is to be able to run the first step (synopsis creation) in Hadoop, while there is no obvious way how this could be achieved.
2. Basic benchmarking job we did proved its performance to be disappointing.
3. Vertica's implementation does not allow any elasticity in terms of precision or bucket compression used. As a consequence, the synopsis has constant size of 49154 bytes, which is equivalent to 16 bits of precision with a 2-byte header. In our opinion this precision is an overkill in many applications. Further in this readme we show that we are able to achieve better accuracy than the native implementation with as little as 1/5 of required storage.

## Modification's to the original HLL algorithm

As pointed out in [5] HyperLogLog has a large error for small cardinalities. For example, when number of buckets is equal to 16384 (i.e. precision is 14 bits), the algorithm will yield an estimate of ~11000 for **no elements**, i.e. when all the buckets are equal to zero. In other words, the smallest possible estimate given by the algorithm is 11k.

An important observation made by Google in [6] is that the algorithm always overestimates the real cardinality for smaller sets. In our implementation we took into account the remedy suggested by Google, i.e. we composed raw HyperLogLog, BiasCorrection and LinearCounting together. To return an estimate we first calculate a raw HyperLogLog estimate and then apply the following logic (note: *m* means the number of buckets):


1. If the raw estimate is between +Inf and 5m, we leave it as it.
2. If the raw estimate is below 5m, but above a certain threshold determined experimentally, we subtract a bias from the raw estimate. Below we describe in details how this is achieved step by step.
3. Otherwise, we try to use LinearCounting if it's applicable. LinearCounting returns a valuable result when at least one of the original HLL buckets is empty. If it's not the case, our final result is a raw estimate shifted by bias.

### Hash function

According to [6] there is no significant difference between various hash functions. However, length of the hash makes a huge difference when the cardinality approaches the number of unique values it can generate. For instance for 32 bit hashes and cardinalities close to *2^32* the hash collisions would become more common. As a result, an accurate estimation would become impossible. Following [6], In our HyperLogLog implementation we use 64 bit MurMurHash.

### LinearCounting

LinearCounting is a different cardinality estimator that uses a bitmap of N bits. For every new element a hash is computed and its log2(N) first bits are used to index a bit in the bitmap and flip it to 1. In our case LinearCounting relies on the original buckets from HyperLogLog, since as a side effect they work like the LC's bitmap. To calculate the estimate, we look at every bucket and check whether its value is equal to zero or greater than zero. In fact, in our case LinearCounting it just another formula applied to the buckets from HyperLogLog, which yields better results for small cardinalities, i.e. when many buckets are empty.

The formula is defined as follows:

	E = U * log(m/U)

where:

* `E` is the estimate
* `U` is number of empty buckets (i.e. those buckets where there were no hashes falling into)
* `m` is number of buckets
* `log` is a natural logarithm

LinearCounting is inaccurate for high cardinalities, but for the small ones is just right. This means that as the cardinality grows, we have to switch from LinearCounting to raw HyperLogLog. In [1] the authors set a threshold of 2.5m, but above this threshold the result is sill highly biased. To prevent it we implemented another improvement proposed the Google engineers [2] called...

### Bias Correction

Google Engineers studied the problem of the bias in their paper. Below one can see how the raw estimate (red) is shifted from the real cardinality (black).

![HLL Bias](http://oi66.tinypic.com/23l12rm.jpg)

In the appendix [7] to [6] Google published empirical bias values. In our implementation we use these data to return a more accurate estimate. To this end, we keep two arrays: `rawEstimateData[][]` and `biasData[][]` whose values are taken from [7]. For the given number of precision bits *p* we look in `rawEstimateData[p][]` for the 6 values that are closest to the raw estimate. Then, we use their indices to look for corresponding bias values in `biasData[p][]`. We calculate mean value of these 6 values and subtract it from the raw estimate.

### Compacting registers

#### 6 bits per bucket
In our baseline implementations we used 8 bits per bucket. This approach makes the algorithm easier to debug, but is also an unnecessary loss of space. Recall that every bucket keeps length of the longest sequence of zeros among hashes falling into that bucket. Since we use 64 bit hashes, out of which *p* first bits indicate which bucket to use, each bucket will store a value between 0 and *64-p*. The highest number we could represent with 6 bits is 63. This means that without impacting accuracy we can allocate 6 bits per bucket effectively reducing the storage space by 1/4.

The idea is to split the buckets into groups of 4 and to store them in 3 bytes (since 4 * 6 bits = 24 bits = 3 bytes).
In order to deserialize an array of bytes into buckets, we iterate over groups of three bytes which get put in four buckets.
Likewise, to serialize the buckets, we iterate over groups of 4 buckets which get stored in three bytes. In our implementation we assume that the synopsis will be serialized and deserialized on little endian machines.

We lay out the compacted buckets in memory as follows.

```
 Byte 0   Byte 1   Byte 2  
+--------+--------+--------+---//
|00000011|11112222|22333333|4444
+--------+--------+--------+---//
```


### Adding some more compactness

As pointed out in [2] (and originally in [11]), buckets' values increase in similar pace. For instance, if we had 8 buckets and we stored there 8*2^10 unique values, each register would store a value close to 10. Obviously, it's not entirely true, because, unless we are incredibly lucky, there will be some deviation, but we bet it won't be significant.

Following [2] we came up with the idea of compacting the buckets to 5 and 4 bits. At first glance these buckets will be too short, because they can represent sequences of *2^5 = 32* and *2^4 = 16* respectively, i.e. *2^31* and *2^15 = 32,768* uniques. The latter case is particularly worrying because it would clearly put strong limitations on where HyperLogLog could by applied. When serializing the synopsis with 4 bits per bucket, if we encountered bucket values higher than 15, we would have to clip them to 15 anyway, because this would be the highest value we could represent. In turn, this would impact the estimate and effectively mean losing unique values. For instance, even with 16k registers we couldn't get higher estimate than ~536M.

A solution to this problem suggested by the Druid folks in [2] is to introduce an offset value and store positive differences from it. The offset value itself also has to be stored in the synopsis. To this end, we had to introduce a header like follows.

```C
struct HLLHdr {
  char magic[2] = {'H','L'};
  uint8_t format;
  uint8_t offset;
  char padding[4] = {'\0','\0','\0','\0'}; // padding to reach 8 bytes in length
};
```

The reference value is stored as `uint8_t offset` in the header during synopsis' serialization and is calculated as the lowest value among all the buckets. When deserializing a synopsis, in order to calculate effective value of a bucket, one has to sum up its value with the offset.

For instance, if we had 4 registers with values 3,5,7 and 4, the offset would be 3 and we would store 0,2,4,1 in each respective bucket. If the variance of bucket values is small, i.e. if the spread is smaller than 32 and 16 for 6 and 5 bits respectively, this solution should prevent bucket clipping. Conversely, if any of the buckets is equal to zero, the offset will bring no profit at all. Later on we present results of queries run on real data in order to check whether this impacts the accuracy.

#### 5 and 4 bits per bucket

To fit the buckets nicely in the array of bytes when using 5 bits per bucket, we split the buckets into groups of 8 and arrange them in 5 bytes as depicted below. Please note that serialization and deserialization operations with 5 bits per bucket doesn't get automatically vectorized by gcc 4.8.5 (as opposed to 8, 6 and 4-bits-per-bucket operations).

```
Byte 0   Byte 1   Byte 2   Byte 3   Byte 4
+--------+--------+--------+--------+--------+---//
+00000111|11222223|33334444|45555566|66677777|...
+--------+--------+--------+--------+--------+---//
```

Arranging buckets with 4 bits per bucket is straightforward:

```
Byte 0   Byte 1
+--------+--------+---//
+00001111|22223333|...
+--------+--------+---//
```


## Using HyperLogLog in Vertica

From the Vertica's point of view a synopsis is VARBINARY. Its size depends on the number of precision bits and compactness (number of bits per bucket). In the table below one can consult for different values of parameters.

| | p=10 | p=11 | p=12 | p=13 | p=14 | p=15
--------|------|------|------|------|------|-----
8 bits per bucket | 1032 | 2056 | 4104 | 8200 | 16392 | 32776
6 bits per bucket | 776 | 1544 | 3080 | 6152 | 12296 | 24584
5 bits per bucket | 648 | 1288 | 2568 | 5128 | 10248 | 20488
4 bits per bucket | 520 | 1032 | 2056 | 4104 | 8200 | 16392

A general formula for the synopsis' size is (note a constant 8 comes from the header size):

S = B * 2^p + 8

Where:
- S is synopsis size
- B is number of bits per bucket
- p is number of precision bits

This representation has never been tested on a big-endian machine. Hence, it's safe to assume that it only works on little-endian machines.

To use our implementation of HyperLogLog we need to register the UDFs in Vertica, so that it can access them.
```SQL
SET ROLE pseudosuperuser;
DROP LIBRARY libhll CASCADE;
CREATE LIBRARY libhll AS '/path/to/libhll.so';
CREATE AGGREGATE FUNCTION HllCreateSynopsis AS LANGUAGE 'C++' NAME 'HllCreateSynopsisFactory' LIBRARY libhll;
CREATE AGGREGATE FUNCTION HllDistinctCount AS LANGUAGE 'C++' NAME 'HllDistinctCountFactory' LIBRARY libhll;
```

Then, to calculate DISTINCT COUNT we first have to aggregate some data:

```SQL
DROP TABLE test_schema.agg_clicks;
CREATE TABLE test_schema.agg_clicks
AS
SELECT
  HOUR(TO_TIMESTAMP(click_ts)) AS hour,
  banner_id,
  zone_id,
  client_id,
  network_id,
  HllCreateSynopsis(user_id_fast USING PARAMETERS hllLeadingBits=:precision, bitsPerBucket=:bitsperbucket) AS Synopsis
FROM
  test_schema.fact_clicks
WHERE
  client_id > :minrange AND client_id < :maxrange
GROUP BY
  HOUR(TO_TIMESTAMP(click_ts)),
  banner_id,
  zone_id,
  client_id,
  network_id;
```

Finally, we can calculate DISTINCT COUNT based on the synopsis:

```SQL
SELECT
  client_id,
  HllDistinctCount(synopsis USING PARAMETERS hllLeadingBits=:precision)
FROM
  test_schema.agg_clicks
GROUP BY
  client_id;
```

## Latency and accuracy benchmarks
To measure latency and accuracy we ran the queries from the listings above on some real data used at Criteo. They were run a cluster of three nodes on a table containing around 364M rows. In our query we used one third of the whole table.

##### Accuracy results
Version|Precision|Mean Error (%)|Error std. dev (%)|Max error (%)
---|---|---|---|---
Vertica's HLL|16?|**0.632771**|2.550527|18.825070
6 bits per bucket|10| 2.098286 |1.744262|7.385960
.|11|1.616222|1.317503|6.722578
.|12|1.107659|0.877167|4.169899
.|13|0.761509|0.577894|2.859550
.|14| **0.531887** |0.414764|2.041547
.|15| **0.362217** |0.301651|1.335048
5 bits per bucket|10|2.098286|1.744262|7.385960
.|11|1.616222|1.317503|6.722578
.|12|1.107659|0.877167|4.169899
.|13|0.761509|0.577894|2.859550
.|14| **0.531887** |0.414764|2.041547
.|15| **0.362217** |0.301651|1.335048
4 bits per bucket|10|2.098278|1.744529|7.385960
.|11|1.616220|1.317493|6.722137
.|12|1.107642|0.877198|4.169899
.|13|0.761514|0.577893|2.859550
.|14|**0.531884** |0.414764|2.041547
.|15|**0.362218** |0.301651|1.335048

##### Timing results
Version | Precision | Synopsis  creation (s) | Distinct Count (s) | Total (s)
---|---|---|---|---
Vertica's HLL | 16? | **676** | **505** | 1181
8 bits/bucket | 10 |126 |10 |136
. |11 |147 |12 |159
. |12 |163 |18 |181
. |13 |233 |36 |269
. |14 |324 |81 |405
. |15 |489 |129 |618
6 b/b|10 |124 |11 |135
. |11 |131 |14 |145
. |12 |152 |23 |175
. |13 |203 |39 |242
. |14 |293 |77 |370
. |15 |372 |144 |516
5 b/b|10 |123 |11 |134
. |11 |129 |16 |145
. |12 |155 |24 |179
. |13 |181 |46 |227
. |14 |**275** |**87** |362
. |15 |**364** |**167** |531
4 b/b|10 |124 |11 |135
. |11 |128 |14 |142
. |12 |148 |23 |171
. |13 |174 |40 |214
. |14 |**257** |**69** |326
. |15 |**314** |**136** |450

##### Conclusions
1. Vertica's implementation has huge inaccuracy. For some values the error is as high as 18%. From our observations the highest errors occur when the distinct count is above 100k.
2. With 4 bits per bucket and 14 bits of precision we achieve better accuracy than Vertica, while keeping the synopsis 6 times smaller and being 3.35x faster.
3. Bucket clipping for 5 and 4 bits per bucket is negligible.

## Building and testing
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

### How to run the unit tests?
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

### How to run the integration tests with Docker?
This repository defines a derived Vertica docker container able to run a set of SQL queries put in the /tests/integration folder.
As of today, the only outcome of those tests are the following: they whether run or not (return exit code 0 or not from vsql).
To run the tests in a Docker container and auto destroy them, run the following command:

```bash
cd <path_to_dir_containing_dockerfile>
docker build -t fjehl/docker-vertica-itests .
docker run -v <full_path_of_vertica_rpm>:/tmp/vertica.rpm -v docker-vertica-itests:/opt/vertica --cap-add SYS_NICE --cap-add SYS_RESOURCE --rm --name docker-vertica-itests -ti fjehl/docker-vertica-itests
```

If the container gets somehow corrupted, destroy the volume with persistent storage (named docker-vertica-itests) with the following command:

```
docker volume rm docker-vertica-itests
```

## Licensing
This work is distributed under the Apache License, Version 2.0.

## References
1. http://algo.inria.fr/flajole/Publications/FlFuGaMe07.pdf
2. http://druid.io/blog/2012/05/04/fast-cheap-and-98-right-cardinality-estimation-for-big-data.html
3. http://druid.io/blog/2014/02/18/hyperloglog-optimizations-for-real-world-systems.html
4. http://antirez.com/news/75
5. https://github.com/antirez/redis/blob/unstable/src/hyperloglog.c
6. https://stefanheule.com/papers/edbt13-hyperloglog.pdf
7. https://docs.google.com/document/d/1gyjfMHy43U9OWBXxfaeG-3MjGzejW1dlpyMwEYAAWEI/view?fullscreen
8. https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Functions/Aggregate/APPROXIMATE_COUNT_DISTINCT_SYNOPSIS.htm
8. https://my.vertica.com/docs/7.1.x/HTML/Content/Authoring/SQLReferenceManual/Functions/Aggregate/APPROXIMATE_COUNT_DISTINCT_OF_SYNOPSIS.htm
9. https://research.neustar.biz/2013/01/24/hyperloglog-googles-take-on-engineering-hll/
10. https://www.facebook.com/notes/facebook-engineering/presto-interacting-with-petabytes-of-data-at-facebook/10151786197628920
11. http://organ.kaist.ac.kr/Prof/pdf/Whang1990(linear).pdf

#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>


#include "gtest/gtest.h"
#include "Hll.hpp"

namespace {

class HllTest : public ::testing::Test {
 protected:
  const std::string ids_filepath = "../data/ids.dat";
  std::ifstream data_file;

  HllTest() {
  }

  virtual ~HllTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  virtual void SetUp() {
    data_file = std::ifstream(ids_filepath, std::ifstream::in);
    // ignore the first line of the data file; it contains a comment
    data_file.ignore(256, '\n');
  }

  virtual void TearDown() {
    data_file.close();
  }

};

class DummyHash : public Hash<uint64_t> {
public:
  uint64_t operator()(uint64_t) const override {
    return 0x1;
  }
};

class StdHash : public Hash<uint64_t> {
public:
  uint64_t operator()(uint64_t val) const override {
    return std::hash<uint64_t>()(val);
  }
};

TEST_F(HllTest, TestStdHash) {
  /**
   * std::hash is a C++11 standard hashing function. We don't use it in our
   * code as the actual hash function is not specified by the standard,
   * therefore is not guaranteed not to be the same between platforms or even
   * between different libstdc++ releases.
   * However, it should still be as good as MurMurHash, therefore we in this
   * test we make sure it really works
   */
  Hll<uint64_t, StdHash> hll(14);

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}
TEST_F(HllTest, TestErrorWithinRangeForDifferentBucketMasks) {
  /**
   * We create a bunch of different HLL synopsis with bucket masks spreading
   * from 8 to 16. We store them together with the precision parameter
   * in order to be able to estimate the relative error later on.
   */
  std::vector<std::pair<uint8_t, Hll<uint64_t>>> hlls = { {6, {6}},
      {8,  {8} },
      {10, {10}},
      {12, {12}},
      {14, {14}},
      {16, {16}}
    };

  for(uint64_t id; data_file >> id;) {
    for(auto& bits_hll: hlls)
      bits_hll.second.add(id);
  }


  const uint64_t real_cardinality = 632055;
  /**
   * TODO: For the time being this is just a magic value out of my hat,
   *       but once a better error correction method is implemented
   *       we can adjust it to better fit the estimate.
   */
  const double error_tolerance = 1.5;

  for(auto& bits_hll: hlls) {
    uint8_t basket_bits = bits_hll.first;
    Hll<uint64_t> hll = bits_hll.second;
    
    uint64_t approximated_cardinality = hll.approximateCountDistinct();

    uint32_t m = 2 << basket_bits;
    /**
     * The paper says that the relative error is typically lower than 
     * 1.04/sqrt(m)
     */
    double expected_error = 1.04/std::sqrt(m);
    double real_error = std::abs(real_cardinality-approximated_cardinality)/real_cardinality;
    
    EXPECT_LT(real_error, expected_error*error_tolerance);
  }
}

/**
 * This test uses a non-standard hash function which yields 0x1 for every input
 * value. For such a hash function Hll should always return the same estimate,
 * no matter how many elements will be added.
 */
TEST_F(HllTest, TestDummyHashFunction) {
  Hll<uint64_t, DummyHash> hll(14);
  hll.add(static_cast<uint64_t>(0));
  auto singleElementDummyCount = hll.approximateCountDistinct();
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  auto millionElementsDummyCount = hll.approximateCountDistinct();

  ASSERT_EQ(singleElementDummyCount, millionElementsDummyCount);
}


/**
 * This tests uses 15 bits for the bucket, which uses a diferent path in
 * Hll.approximateCountDistinct()
 */
TEST_F(HllTest, TestNonStandardBuckets) {
  Hll<uint64_t> hll(15); //XXX: NOTE 15 bits

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}


/**
 * This tests splits the input data set into two halfs. The first half is added
 * to one Hll, the second half to another Hll. At the same time all the elements
 * are added to a third Hll. Assumption is that if we add to halfs of the input
 * data, we should get the same estimate as for having all the elements stored in
 * one Hll.
 */
TEST_F(HllTest, TestSynopsisAdditionWorks) {
  Hll<uint64_t> hll1(14), hll2(14), hll1plus2(14);
  std::vector<uint32_t> ids;
  ids.reserve(1000000); 
  for(uint32_t id; data_file >> id;) {
    ids.push_back(id);
  }
  size_t i=0;
  for(; i<500000; ++i) {
    hll1.add(ids[i]);
    hll1plus2.add(ids[i]);
  }
  for(;i<1000000; ++i) {
    hll2.add(ids[i]);
    hll1plus2.add(ids[i]);
  }

  hll1.add(hll2);
  ASSERT_EQ(hll1.approximateCountDistinct(), hll1plus2.approximateCountDistinct());
}

TEST_F(HllTest, Test32BitHash) {
  Hll<uint32_t> hll(14);
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}

TEST_F(HllTest, Test32BitIds) {
  Hll<uint32_t> hll(14);
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}

/**
 * Since the estimate is proportional to a harmonic mean of bucketed values
 * growing monotonically, adding new elements should never make the estimate
 * lower.
 */
TEST_F(HllTest, TestEstimateGrowsMonotonically) {
  Hll<uint64_t> hll(14);

  uint32_t counter = 0;
  uint64_t prevEstimate = 0;
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
    ++counter;
    if(counter % 100000 == 0) { // check how the estimate behaves every 100k elements
      uint64_t curEstimate = hll.approximateCountDistinct();
      EXPECT_GE(curEstimate, prevEstimate);
      prevEstimate = curEstimate;
    }
  }

}


/**
 * Adding items already seen should never decrease the estimate. Since they
 * were already hashed and inserted into corresponding bucket, this bucket
 * either contains the same value or a larger one. If it's larger, adding
 * already seen value will not change it.
 */
TEST_F(HllTest, TestAlreadySeenItemsDontChangeEstimate) {
  Hll<uint64_t> hll(14);

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  auto firstEstimate = hll.approximateCountDistinct();

  data_file.seekg(0); // go back to the beginning
  data_file.ignore(256, '\n');

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  EXPECT_EQ(firstEstimate, hll.approximateCountDistinct());
}


/**
 * Test whether an estimate for 1M numbers with cardinality=630k will fall
 * within 1% from the real value.
 */
TEST_F(HllTest, TestBigInput) {
  Hll<uint64_t> hll(14);

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}


TEST_F(HllTest, TestSmallInput) {
  Hll<uint64_t> hll(14);

  size_t i=0;
  for(uint32_t id; data_file >> id && i < 100000; ++i) {
    hll.add(id);
  }

  const uint32_t realCardinality = 95245;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

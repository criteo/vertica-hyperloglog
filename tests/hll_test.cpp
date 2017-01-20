#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>

#include "gtest/gtest.h"
#include "Hll.h"

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


// TEST_F(HllTest, TestMoreBucketsYieldHigherEstimate) {
//   Hll hll8(8);
//   Hll hll10(10);
//   Hll hll12(12);
//   Hll hll14(14);

//   for(uint32_t id; data_file >> id;) {
//     hll8.add(id);
//     hll10.add(id);
//     hll12.add(id);
//     hll14.add(id);
//   }

//   EXPECT_GE(hll14.approximateCountDistinct(), hll12.approximateCountDistinct());
//   EXPECT_GE(hll12.approximateCountDistinct(), hll10.approximateCountDistinct());
//   EXPECT_GE(hll10.approximateCountDistinct(), hll8.approximateCountDistinct());
// }


/**
 * Since the estimate is proportional to a harmonic mean of bucketed values
 * growing monotonically, adding new elements should never make the estimate
 * lower.
 */
TEST_F(HllTest, TestEstimateGrowsMonotonically) {
  Hll hll(14);

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
  Hll hll(14);

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
TEST_F(HllTest, TestErrorBelowOnePercentFor14Bits) {
  Hll hll(14);

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}


}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

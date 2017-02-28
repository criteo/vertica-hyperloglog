#include <cstdlib>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"
#include "bias_corrected_estimate.hpp"
#include "hll.hpp"

using namespace std;

class BiasCorrectionTest : public HllBaseTest {
 protected:
  std::ifstream data_file;

  BiasCorrectionTest() : HllBaseTest("bias_correction.dat"), 
    data_file{getInputPath(), std::ifstream::in} {}

  virtual ~BiasCorrectionTest() {
  }

  virtual void SetUp() {
    data_file.seekg(0, data_file.beg);
    // ignore the first line of the data file; it contains a comment
    data_file.ignore(256, '\n');
  }

  virtual void TearDown() {
    data_file.close();
  }


};


/**
 * These two tests check that BiasCorrection really works. How?
 * Well, in theory when we apply bias correction, the new estimate is supposed
 * to be closer to the actual cardinality than the raw HLL estimate.
 */
TEST_F(BiasCorrectionTest, TestBiasCorrectionGivesBetterEstimateThanRawHLL) {
  const uint8_t MIN_SUPPORTED_PRECISION = 6;
  const uint8_t MAX_SUPPORTED_PRECISION = 18;


  std::set<uint64_t> ids;
  uint32_t maxCardinality = 5 * (1 << MAX_SUPPORTED_PRECISION);
  generateNumbers(ids, maxCardinality);
  for(uint8_t precision = MIN_SUPPORTED_PRECISION;
              precision <= MAX_SUPPORTED_PRECISION;
              ++precision) {
    HllRaw<uint64_t> hll(precision);
    uint32_t biasCorrectionThreshold = 5 * (1 << precision);

    uint8_t rawBetter = 0;
    uint8_t biasBetter = 0;
    /**
     * For every precision we look at the the bias correction threshold
     * and generate input numbers accordingly. We use fractions of that
     * threshold and for each of them we make sure the bias correction makes
     * the estimate more reliable.
     */
    for(double frac: {2./5, 2.5/5, 3./5, 3.5/5, 4./5}) {
      uint32_t realCardinality = biasCorrectionThreshold*frac;

      auto it=ids.begin();
      for(uint32_t idx=0; idx< realCardinality; ++idx, ++it) {
        hll.add(*it);
      }

      int32_t hllCardinality = static_cast<int32_t>(hll.estimate());
      int32_t biasCorrectedCardinality =
        static_cast<int32_t>(BiasCorrectedEstimate::estimate(hllCardinality, precision));

      uint32_t hllError = abs(hllCardinality - realCardinality);
      uint32_t biasCorrectedError = abs(biasCorrectedCardinality - static_cast<int32_t>(realCardinality));

      if(biasCorrectedError < hllError)
          biasBetter++;
      else if(hllError < biasCorrectedError)
          rawBetter++;
    }
    EXPECT_GT(biasBetter, rawBetter);
  }
}

/**
 * What we do here is to use some pre-generated numbers (60k in total). First,
 * we slice them into 10 subsets. Then we add each subset to an instance of HLL
 * and to std::set. We take an estimate from the former and the real cardinality
 * from the latter. Then, BiasCorrection is fed with the estimate from HLL. If
 * everything works correctly, the estimate from BiasCorrection is closer to the
 * real cardinality than the HLL estimate
 */
TEST_F(BiasCorrectionTest, TestFixedPrecisionEstimate) {
  const uint8_t PRECISION = 14;
  HllRaw<uint64_t> hll(PRECISION);
  std::vector<uint64_t> ids;
  std::set<uint64_t> idsSet;

  for(uint64_t id; data_file >> id;) {
    ids.push_back(id);
  }
  uint32_t slice = ids.size()/10;
  for(uint8_t i=0; i<10; ++i) {
    for(auto it=ids.begin()+i*slice; it != ids.begin()+(i+1)*slice; ++it) {
      hll.add(*it);
      idsSet.insert(*it);
    }

    // we need this weird casting because those values will be subtracted
    // later on
    int32_t realCardinality = static_cast<int32_t>(idsSet.size());
    int32_t hllEstimate = static_cast<int32_t>(hll.estimate());
    int32_t correctedEstimate = static_cast<int32_t>(BiasCorrectedEstimate::estimate(hllEstimate, PRECISION));

    auto hllError = abs(realCardinality - hllEstimate);
    auto correctedError = abs(realCardinality - correctedEstimate);

    // bias-corrected error is supposed to be lower than raw HLL error
    ASSERT_LT(correctedError, hllError);
  }

}

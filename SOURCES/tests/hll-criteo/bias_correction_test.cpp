#include <cstdlib>
#include <fstream>
#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "hll-criteo/bias_corrected_estimate.hpp"
#include "hll-criteo/hll.hpp"

using namespace std;

class BiasCorrectionTest : public HllBaseTest {
 protected:
  std::ifstream data_file;

  const uint8_t MIN_SUPPORTED_PRECISION = 6;
  const uint8_t MAX_SUPPORTED_PRECISION = 18;

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
 * This test is to check that the Bias Correction algorithm works well for the raw HLL estimates
 */

TEST_F(BiasCorrectionTest, TestBiasCorrectionNearZero) {
  std::vector<double> lowestEstimates = {11, 23, 46, 92, 184, 369, 738, 1477, 2954, 5908, 11817, 23635, 47271, 94542, 189084,};
  for(uint8_t prec = MIN_SUPPORTED_PRECISION; prec <= MAX_SUPPORTED_PRECISION; ++prec) {
    auto rawEstimate = lowestEstimates[prec-4];
    auto correctedEstimate = BiasCorrectedEstimate::estimate(rawEstimate, prec);
    EXPECT_LT(correctedEstimate, rawEstimate);
  }
}

/**
 * This test is to check that the Bias Correction algorithm works
 * correctly when the estimate is close to the linear coutning-bias correction threshold
 */

TEST_F(BiasCorrectionTest, TestBiasCorrectionNearThreshold) {
  // these are last estimates for precisions [4, 18] from the 6 neighbours bias correction algorithm
  // we will use these values and increase them slightly to make sure the bias correction algorithm operates next to its threshold
  std::vector<double> lastEstimates = {77.2394, 158.5852, 318.9858, 638.6102, 1274.5192, 2553.768, 5084.1828, 10229.9176, 20463.22, 40717.2424, 81876.3884, 163729.2842, 325847.1542, 654941.845, 1303455.691,};
  std::vector<double> lastCorrections = {-1.7606, -0.414800000000014, 0.985799999999983, 2.61019999999996, 1.51919999999996, -6.23199999999997, -9.81720000000041, -10.0823999999993, -15.7799999999988, -37.7575999999972, -42.6116000000038, -109.715800000005, -193.84580000001, -417.155000000028, -713.308999999892,};
  for(uint8_t precision = MIN_SUPPORTED_PRECISION;
            precision <= MAX_SUPPORTED_PRECISION;
            ++precision) {
    // lastEstimates starts for precision=4, so this must be subtracted
    // we add 1 to be right above the last estimate
    uint32_t cardinalityNearThreshold = lastEstimates[precision-4] + 1;

    auto estimate = BiasCorrectedEstimate::estimate(cardinalityNearThreshold, precision);

    // guesstimate: correction's absolute value shouldn't be bigger than a double of a correction for highest estimate
    auto absCorrectionUpperBound = 2*std::abs(lastCorrections[precision-4]);
    if(estimate > cardinalityNearThreshold)
      EXPECT_LT(estimate - cardinalityNearThreshold, absCorrectionUpperBound);
    else
      EXPECT_LT(cardinalityNearThreshold - estimate, absCorrectionUpperBound);
  }
}

/**
 * These two tests check that BiasCorrection really works. How?
 * Well, in theory when we apply bias correction, the new estimate is supposed
 * to be closer to the actual cardinality than the raw HLL estimate.
 */
TEST_F(BiasCorrectionTest, TestBiasCorrectionGivesBetterEstimateThanRawHLL) {
  std::set<uint64_t> ids;
  uint32_t maxCardinality = 5 * (1 << MAX_SUPPORTED_PRECISION);
  generateNumbers(ids, maxCardinality);
  for(uint8_t precision = MIN_SUPPORTED_PRECISION;
              precision <= MAX_SUPPORTED_PRECISION;
              ++precision) {
    SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(precision); // sizeof(HLLHdr) + synopsis
    HllRaw<uint64_t> hll(precision, buffer.first.get());
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

      uint32_t hllError = abs((long)(hllCardinality - realCardinality));
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
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint64_t> hll(PRECISION, buffer.first.get());
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

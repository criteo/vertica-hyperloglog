#include <cmath>

#include "gtest/gtest.h"
#include "hll_utils.hpp"

using namespace std;

class HllUtilsTest : public ::testing::Test {

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

/**
 * Test fast_inv_pow2 by looping over all the cases from 0 to FLOAT64_MAX_EXPONENT
 */
TEST_F(HllUtilsTest, TestFastInvPow2) {
  for(size_t n = 0; n < UINT8_MAX; n ++) {
    const double x = 1 / pow(2.0d, n);
    const double y = fast_inv_pow2(n);
    EXPECT_EQ(x, y);
  }
}


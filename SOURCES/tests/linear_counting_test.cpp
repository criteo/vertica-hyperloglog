#include <fstream>
#include <iostream>
#include <string>
#include "gtest/gtest.h"
#include "linear_counting.hpp"
#include "hll.hpp"

using namespace std;

class LinearCountingTest : public ::testing::Test {
 protected:
  const std::string ids_filepath = "../data/linear_counting.dat";
  std::ifstream data_file;

  LinearCountingTest() : data_file{ids_filepath, std::ifstream::in} {
  }

  virtual ~LinearCountingTest() {
  }

  virtual void SetUp() {
    data_file.seekg(0, data_file.beg);
    // ignore the first line of the data file; it contains a comment
    data_file.ignore(256, '\n');
  }

  virtual void TearDown() {
    data_file.close();
  }

  void generateNumbers(std::set<uint64_t>& s, uint32_t cardinality) {
      std::srand(0);
      while(s.size() < cardinality) {
          uint64_t missing = cardinality - s.size();
          for(uint32_t i=0; i < missing; ++i)
              s.insert(std::rand());
      }
  }
};


TEST_F(LinearCountingTest, TestLinearCounting) {
  std::set<uint64_t> ids;
  for(uint32_t precision=7; precision < 18; ++precision) {
    LinearCounting lc(precision);
    HllRaw<uint64_t> hll(precision);

    MurMurHash<uint64_t> hash;

    const uint64_t lcThreshold = lc.getLinearCountingThreshold(precision);
    const uint32_t realCardinality = lcThreshold / 10 ;

    generateNumbers(ids, realCardinality);
    assert(ids.size() == realCardinality);

    for(uint64_t id : ids) {
      hll.add(id);
      lc.add(hash(id));
    }

    uint64_t lcEstimate = lc.estimate();
    uint64_t hllEstimate = hll.estimate();
    uint64_t bcEstimate = BiasCorrectedEstimate::estimate(hllEstimate, precision);
    // /cout << precision << " " << realCardinality << " " << lcEstimate << " " << bcEstimate << endl;
    uint32_t lcError = abs(static_cast<int32_t>(lcEstimate)-static_cast<int32_t>(realCardinality));
    uint32_t bcError = abs(static_cast<int32_t>(bcEstimate)-static_cast<int32_t>(realCardinality));
    EXPECT_TRUE((lcError+bcError)/2 == bcError || lcError < bcError);
  }
}

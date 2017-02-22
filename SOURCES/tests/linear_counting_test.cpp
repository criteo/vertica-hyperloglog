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
    for(uint32_t i=s.size(); i < cardinality; ++i)
      s.insert(std::rand());
    while(s.size() < cardinality)
      s.insert(std::rand());
  }
};


TEST_F(LinearCountingTest, TestLinearCounting) {
  std::set<uint64_t> ids;
  for(uint32_t precision=11; precision < 18; ++precision) {
    // For LC we use 5 orders of magnitude lower precision than for HLL
    // e.g. if HLL uses 10 bits of precision (2**10 buckets), LC will
    // have 2**5 bits, i.e. 2**3 = 8 bytes 
    LinearCounting lc(precision-5);
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
    EXPECT_LT(lcError, bcError);
  }
}

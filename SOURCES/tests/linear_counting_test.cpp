/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

#include <fstream>
#include <iostream>
#include <string>

#include "base_test.hpp"
#include "gtest/gtest.h"
#include "linear_counting.hpp"
#include "hll.hpp"

using namespace std;

class LinearCountingTest : public HllBaseTest {
 protected:
  std::ifstream data_file;

  LinearCountingTest() : HllBaseTest("linear_counting.dat"),
    data_file{getInputPath(), std::ifstream::in} {}

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

};


TEST_F(LinearCountingTest, TestLinearCounting) {
  std::set<uint64_t> ids;
  for(uint32_t precision=7; precision < 18; ++precision) {
    SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(precision); // sizeof(HLLHdr) + synopsis
    LinearCounting lc(precision);
    HllRaw<uint64_t> hll(precision, buffer.first.get());

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

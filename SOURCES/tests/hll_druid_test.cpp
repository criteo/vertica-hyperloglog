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

#include "hll_druid.hpp"
#include "base_test.hpp"
#include "gtest/gtest.h"

#include <stdint.h>
#include <fstream>

using namespace std;
using namespace druid;

namespace
{

class HllDruidTest : public HllBaseTest {
 protected:
  std::ifstream data_file;

  HllDruidTest() : HllBaseTest("ids.dat"),
    data_file{getInputPath(), std::ifstream::in} {}

  virtual ~HllDruidTest() {
    // You can do clean-up work that doesn't throw exceptions here.
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

TEST_F(HllDruidTest, TestEmpty)
{

  unsigned char bufferDruid[1031] = {0};
  HllDruid hll = HllDruid::wrapRawBuffer(bufferDruid, sizeof(bufferDruid) / sizeof(char));
  hll.reset();

  EXPECT_EQ(hll.approximateCountDistinct(), 0);

  unsigned char synop[] = {0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  hll.fold(synop, sizeof(synop) / sizeof(char));

  EXPECT_EQ(hll.approximateCountDistinct(), 0);
}

/**
 * Test approximate count is working properly
 *
 **/
TEST_F(HllDruidTest, TestApproximateCounts) {
  const std::vector<size_t> testCardinalities = {100, 200, 1000, 100000, 500000};
  const double expectedMaxError = 0.05; // we expect error to stay within 5% of real cardinality
  std::set<uint64_t> ids;
  for(uint64_t id; data_file >> id;) {
    ids.insert(id);
  }

  size_t availableIds = ids.size();
  EXPECT_GT(availableIds, 500000); // we have enough unique IDs in the file ()

  unsigned char bufferDruid[1031] = {0};
  HllDruid hll = HllDruid::wrapRawBuffer(bufferDruid, sizeof(bufferDruid) / sizeof(char));
  hll.reset();

  for (size_t nCard = 0; nCard < testCardinalities.size(); ++nCard) {
    size_t itemsAdded = 0;
    uint64_t expectedMinimum = static_cast<uint64_t>( (1.0 - expectedMaxError) * testCardinalities[nCard]);
    uint64_t expectedMaximum = static_cast<uint64_t>( (1.0 + expectedMaxError) * testCardinalities[nCard]);
    for(uint64_t id: ids) {
      if (itemsAdded++ < testCardinalities[nCard]) {
        hll.add<uint64_t>(id);
      } else {
        break;
      }
    }

    EXPECT_GE(hll.approximateCountDistinct(), expectedMinimum);
    EXPECT_LE(hll.approximateCountDistinct(), expectedMaximum);
  }
}

} // namespace

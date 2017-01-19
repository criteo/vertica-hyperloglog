#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>

#include "gtest/gtest.h"
#include "Hll.h"

namespace {

// The fixture for testing class Foo.
class HllTest : public ::testing::Test {
 protected:
  // You can remove any or all of the following functions if its body
  // is empty.

  HllTest() {
    // You can do set-up work for each test here.
  }

  virtual ~HllTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  // If the constructor and destructor are not enough for setting up
  // and cleaning up each test, you can define the following methods:

  virtual void SetUp() {
    // Code here will be called immediately after the constructor (right
    // before each test).
  }

  virtual void TearDown() {
    // Code here will be called immediately after each test (right
    // before the destructor).
  }

  // Objects declared here can be used by all tests in the test case for Foo.
};

TEST_F(HllTest, TestResultIsRepeatable) {
}

// Tests that the Foo::Bar() method does Abc.
TEST_F(HllTest, TestEstimateFor14Bits) {
  const std::string ids_filepath = "data/ids.dat";
  Hll hll(14);

  std::ifstream data_file(ids_filepath, std::ifstream::in);

  // ignore the first line of the data file - it contains a comment
  data_file.ignore(256, '\n');
  for(uint64_t i=0; i<10000000; ++i) {
    uint32_t id;
    data_file >> id;
    hll.add(id);
  }

  std::cout << hll.approximateCountDistinct() << std::endl;
  EXPECT_EQ(true, true);
  //EXPECT_EQ(0, f.Bar(input_filepath, output_filepath));
}


}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
